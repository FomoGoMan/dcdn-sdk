// DownloadManager.cpp
#include "DownloadManager.h"

#include "util/HttpDownloader.h"
#include <fstream>
#include <iostream>
#include <thread>
#include <condition_variable>
#include <unordered_set>
#include <chrono>
#include <deque>
#include <functional>
#include <future>
#include <sstream>

using namespace dcdn;

namespace {

// 把每个 manager 对应的运行上下文放在一个结构里
struct ActiveSubTask {
    std::string parentTaskId; // 所属主任务 id
    size_t offset = 0;
    size_t length = 0; // 0 表示不确定（直到 IsEnd 或 ContentLength 可知）
    std::shared_ptr<dcdn::util::DownloaderTask> downloader;
    std::shared_ptr<std::ofstream> file; // 可能共享同一个文件（按 offset 写需 seek）
    size_t lastReportSize = 0;
    int index = 0;
};

struct Context {
    std::mutex mtx;
    std::condition_variable cv;

    // downloader raw ptr -> subtask info
    std::unordered_map<dcdn::util::DownloaderTask*, ActiveSubTask> tasksByPtr;

    // taskId -> list of downloader shared_ptrs (子任务)
    std::unordered_map<std::string, std::vector<std::shared_ptr<dcdn::util::DownloaderTask>>> downloaderByTaskId;

    // 被 notify 的任务集合（待处理的 downloader raw ptr）
    std::unordered_set<dcdn::util::DownloaderTask*> events;

    // 命令队列（由外部 public API post 到这里以串行化）
    std::deque<std::function<void()>> cmdQueue;

    // worker 线程：负责串行执行 cmdQueue，然后处理 events（read / write / 状态更新）
    std::thread worker;
    bool running = false;
};

// 全局 map：DownloadManager* -> HttpContext（每个 manager 一个 HttpContext）
static std::unordered_map<DownloadManager*, std::unique_ptr<Context>> g_httpContexts;

// 从 manager 获取 Context 指针（可能为 nullptr）
static Context* getHttpContext(DownloadManager* mgr) {
    auto it = g_httpContexts.find(mgr);
    if (it == g_httpContexts.end()) return nullptr;
    return it->second.get();
}

// HttpDownloader 的 notify 回调（传入 shared_ptr<DownloaderTask>, receiver 为 DownloadManager*）
static void HttpNotifyCallback(std::shared_ptr<dcdn::util::DownloaderTask> task, void* receiver) {
    if (!task || !receiver) return;
    DownloadManager* mgr = static_cast<DownloadManager*>(receiver);
    Context* ctx = getHttpContext(mgr);
    if (!ctx) return;
    {
        std::lock_guard<std::mutex> l(ctx->mtx);
        // 如果 tasksByPtr 中没有这个指针，先插入一个占位（后续 addDownloadTask 会填充）
        auto it = ctx->tasksByPtr.find(task.get());
        if (it == ctx->tasksByPtr.end()) {
            ActiveSubTask a;
            a.downloader = task;
            ctx->tasksByPtr[task.get()] = std::move(a);
        } else {
            it->second.downloader = task;
        }
        ctx->events.insert(task.get());
    }
    ctx->cv.notify_one();
}

static std::string genTaskId() {
    static std::atomic<uint64_t> cnt{0};
    std::ostringstream ss;
    ss << std::chrono::steady_clock::now().time_since_epoch().count() << "_" << cnt++;
    return ss.str();
}

} // namespace

////////////////////////
// DownloadTask 序列化 (简单实现)
////////////////////////
std::string DownloadTask::serialize() const {
    std::ostringstream ss;
    ss << id << "|" << url << "|" << contentHash << "|" << totalSize << "|"
       << downloaded << "|" << static_cast<int>(status);
    return ss.str();
}

DownloadTask DownloadTask::deserialize(const std::string& data) {
    DownloadTask task;
    std::istringstream ss(data);
    std::string token;
    std::getline(ss, task.id, '|');
    std::getline(ss, task.url, '|');
    std::getline(ss, task.contentHash, '|');
    if (!std::getline(ss, token, '|')) return task;
    task.totalSize = static_cast<size_t>(std::stoull(token));
    if (!std::getline(ss, token, '|')) return task;
    task.downloaded = static_cast<size_t>(std::stoull(token));
    if (!std::getline(ss, token, '|')) return task;
    task.status = static_cast<TaskStatus>(std::stoi(token));
    return task;
}

////////////////////////
// PersistenceHelper (占位 TODO)
////////////////////////
DownloadManager::PersistenceHelper::PersistenceHelper(const std::string& dbPath) {
    db_ = nullptr;
    (void)dbPath;
    // TODO: 打开 sqlite3 及建表
}

DownloadManager::PersistenceHelper::~PersistenceHelper() {
    // TODO: 关闭 sqlite3
}

bool DownloadManager::PersistenceHelper::saveTask(const DownloadTask& task) {
    (void)task;
    // TODO
    return true;
}

bool DownloadManager::PersistenceHelper::loadTasks(std::vector<DownloadTask>& tasks) {
    (void)tasks;
    // TODO
    return true;
}

bool DownloadManager::PersistenceHelper::deleteTask(const std::string& taskId) {
    (void)taskId;
    // TODO
    return true;
}

bool DownloadManager::PersistenceHelper::saveSubTasks(const std::string& taskId, const std::vector<SubTask>& subtasks) {
    (void)taskId; (void)subtasks;
    // TODO
    return true;
}

bool DownloadManager::PersistenceHelper::loadSubTasks(const std::string& taskId, std::vector<SubTask>& subtasks) {
    (void)taskId; (void)subtasks;
    // TODO
    return true;
}

////////////////////////
// DownloadManager 构造 / 析构
////////////////////////
DownloadManager::DownloadManager() {
    // 初始化 httpDownloader_
    httpDownloader_ = std::make_unique<util::HttpDownloader>();
    int ret = httpDownloader_->Init(nullptr);
    if (ret != 1) {
        std::cerr << "Warning: HttpDownloader Init returned " << ret << std::endl;
    }
    httpDownloader_->Start();

    // 建一个 Context 并启动 worker
    auto ctx = std::make_unique<Context>();
    ctx->running = true;

    // worker: 串行执行 cmdQueue，然后处理 events（读数据 -> 写文件 -> 更新状态）
    ctx->worker = std::thread([this]() {
        Context* ctx = getHttpContext(this);
        if (!ctx) return;
        while (true) {
            std::function<void()> cmd;
            dcdn::util::DownloaderTask* evPtr = nullptr;

            {
                std::unique_lock<std::mutex> l(ctx->mtx);
                // 优先执行命令队列（控制指令）
                if (!ctx->cmdQueue.empty()) {
                    cmd = std::move(ctx->cmdQueue.front());
                    ctx->cmdQueue.pop_front();
                } else if (!ctx->events.empty()) {
                    auto it = ctx->events.begin();
                    evPtr = *it;
                    ctx->events.erase(it);
                } else if (!ctx->running) {
                    // 退出条件
                    break;
                } else {
                    // nothing happened, wait
                    ctx->cv.wait(l);
                    continue;
                }
            }

            // 执行控制命令（在 manager 线程）
            if (cmd) {
                try {
                    cmd();
                } catch (const std::exception& e) {
                    std::cerr << "Exception in manager command: " << e.what() << std::endl;
                } catch (...) {
                    std::cerr << "Unknown exception in manager command" << std::endl;
                }
                continue;
            }

            // 处理下载事件（由 HttpDownloader 通知）
            if (evPtr) {
                ActiveSubTask active;
                {
                    std::lock_guard<std::mutex> l(ctx->mtx);
                    auto it = ctx->tasksByPtr.find(evPtr);
                    if (it == ctx->tasksByPtr.end()) {
                        continue;
                    }
                    active = it->second; // 复制，释放锁后处理
                }

                auto downloaderSP = active.downloader;
                if (!downloaderSP) {
                    continue;
                }
                auto raw = downloaderSP.get();

                // 读取数据
                bool isEnd = raw->IsEnd();

                auto buffer = raw->Read();
                size_t totalReadForThisNotify = 0;
                while (buffer) {
                    size_t len = buffer->Length();
                    size_t offset = buffer->Offset(); // 注意：如果使用 range 模式，offset 表示块位置

                    // 获取 options (主任务的 options)
                    FileDownloadOptions opts;
                    {
                        std::lock_guard<std::mutex> l(tasksMutex_);
                        auto itOpt = taskOptions_.find(active.parentTaskId);
                        if (itOpt != taskOptions_.end()) opts = itOpt->second;
                    }

                    // 写入：如果文件流存在且需要随机写（subtask offset != 0），需要 seekp
                    if (active.file && active.file->good()) {
                        // 如果这是并发分片写入，我们需要确保以 offset 写入
                        try {
                            active.file->seekp(static_cast<std::streamoff>(offset), std::ios::beg);
                        } catch (...) { /* ignore */ }
                        active.file->write(reinterpret_cast<const char*>(buffer->Data()), len);
                        active.file->flush();
                    } else if (opts.outputStream) {
                        opts.outputStream->write(reinterpret_cast<const char*>(buffer->Data()), len);
                        opts.outputStream->flush();
                    } else if (!opts.outputPath.empty()) {
                        // 按需打开并写（以随机写为主，按 offset 写）
                        std::fstream ofs(opts.outputPath, std::ios::in | std::ios::out | std::ios::binary);
                        if (!ofs) {
                            // 文件可能不存在: 创建
                            std::ofstream create(opts.outputPath, std::ios::binary);
                            create.close();
                            ofs.open(opts.outputPath, std::ios::in | std::ios::out | std::ios::binary);
                        }
                        if (ofs) {
                            ofs.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
                            ofs.write(reinterpret_cast<const char*>(buffer->Data()), len);
                            ofs.close();
                        }
                    }

                    // 流回调（媒体播放场景）
                    if (opts.streamCallback) {
                        opts.streamCallback(reinterpret_cast<const char*>(buffer->Data()), len, offset);
                    }

                    totalReadForThisNotify += len;
                    buffer = buffer->Next();
                } // while buffer

                // 更新父任务的进度（以 parent task 为粒度）
                if (totalReadForThisNotify > 0) {
                    updateTaskProgress(active.parentTaskId, totalReadForThisNotify);
                    // optionally notify buffer ready range; offset info lost if multiple buffers, we just notify a range
                    notifyBufferReady(active.parentTaskId, active.offset, active.offset + totalReadForThisNotify);
                }

                // 如果该子任务结束（IsEnd），检查整个 parent 是否完成
                if (isEnd) {
                    // 标记该子任务完成（从 ctx->tasksByPtr / downloaderByTaskId 清理）
                    {
                        std::lock_guard<std::mutex> l(ctx->mtx);
                        // erase from tasksByPtr
                        ctx->tasksByPtr.erase(raw);
                        // remove from downloaderByTaskId vector
                        auto itVec = ctx->downloaderByTaskId.find(active.parentTaskId);
                        if (itVec != ctx->downloaderByTaskId.end()) {
                            auto &vec = itVec->second;
                            vec.erase(std::remove_if(vec.begin(), vec.end(),
                                                     [raw](const std::shared_ptr<dcdn::util::DownloaderTask>& p) {
                                                         return p.get() == raw;
                                                     }),
                                      vec.end());
                        }
                    }

                    // 如果父任务没有剩余子任务，则父任务完成或失败
                    bool parentHasMore = false;
                    {
                        std::lock_guard<std::mutex> l(ctx->mtx);
                        auto itVec = ctx->downloaderByTaskId.find(active.parentTaskId);
                        if (itVec != ctx->downloaderByTaskId.end() && !itVec->second.empty()) parentHasMore = true;
                    }

                    if (!parentHasMore) {
                        // 最后一个子任务结束，更新父任务状态（completed / failed）
                        auto st = raw->Status();
                        {
                            std::lock_guard<std::mutex> l(tasksMutex_);
                            auto itTask = tasks_.find(active.parentTaskId);
                            if (itTask != tasks_.end()) {
                                if (st == dcdn::util::DownloaderTask::Completed && !itTask->second.cancelled) {
                                    itTask->second.status = TaskStatus::Completed;
                                } else if (itTask->second.cancelled) {
                                    itTask->second.status = TaskStatus::Cancelled;
                                } else {
                                    itTask->second.status = TaskStatus::Failed;
                                }
                            }
                        }
                    } // if !parentHasMore

                    // close child file handle if exists
                    if (active.file) {
                        try { active.file->close(); } catch (...) {}
                    }
                } // if isEnd
            } // if evPtr
        } // while
    });

    g_httpContexts[this] = std::move(ctx);
}

DownloadManager::~DownloadManager() {
    // 停止 Context worker
    auto it = g_httpContexts.find(this);
    if (it != g_httpContexts.end()) {
        Context* ctx = it->second.get();
        {
            std::lock_guard<std::mutex> l(ctx->mtx);
            ctx->running = false;
            ctx->cv.notify_all();
        }
        if (ctx->worker.joinable()) ctx->worker.join();
        g_httpContexts.erase(it);
    }

    // 停止 http/p2p downloader (注意：若需要优雅取消所有子任务，应先遍历并Cancel)
    if (httpDownloader_) {
        httpDownloader_.reset();
    }
    if (p2pDownloader_){
        p2pDownloader_.reset();
    }
}

////////////////////////
// Helper: 在 manager 的线程上同步执行函数并返回值
////////////////////////
template<typename R>
R runSyncOnContext(Context* ctx, std::function<R()> fn) {
    auto prom = std::make_shared<std::promise<R>>();
    auto fut = prom->get_future();
    {
        std::lock_guard<std::mutex> l(ctx->mtx);
        ctx->cmdQueue.emplace_back([prom, fn]() {
            try {
                R r = fn();
                prom->set_value(r);
            } catch (...) {
                try { prom->set_exception(std::current_exception()); } catch (...) {}
            }
        });
    }
    ctx->cv.notify_one();
    return fut.get();
}

void runAsyncOnContext(Context* ctx, std::function<void()> fn) {
    {
        std::lock_guard<std::mutex> l(ctx->mtx);
        ctx->cmdQueue.emplace_back(std::move(fn));
    }
    ctx->cv.notify_one();
}

////////////////////////
// 配置接口
////////////////////////
void DownloadManager::setStrategy(DownloadStrategy strategy) {
    strategy_ = strategy;
}

void DownloadManager::setMaxConcurrentDownloads(size_t max) {
    maxConcurrent_ = max;
}

void DownloadManager::setPersistPath(const std::string& path) {
    persistPath_ = path;
    dbHelper_ = std::make_unique<PersistenceHelper>(path);
}

////////////////////////
// 任务管理（HTTP_ONLY 实现） 
////////////////////////
std::string DownloadManager::addDownloadTask(
    const std::string& url,
    const std::string& contentHash,
    const FileDownloadOptions& options
) {
    Context* ctx = getHttpContext(this);
    if (!ctx) {
        // shouldn't happen
        return {};
    }

    // run in manager thread to serialize control cmd operations
    auto newId = runSyncOnContext<std::string>(ctx, [this, ctx, url, contentHash, options]() -> std::string {
        // create parent task
        DownloadTask task;
        task.id = genTaskId();
        task.url = url;
        task.contentHash = contentHash;
        task.totalSize = 0; // will update when subtask info arrives
        task.downloaded = 0;
        task.startTime = std::chrono::system_clock::now();
        task.lastUpdate = task.startTime;
        task.status = TaskStatus::Pending;

        tasks_[task.id] = task;
        taskOptions_[task.id] = options;

        // If strategy is HTTP_ONLY we create either:
        // - a single downloader task (if cannot determine totalSize now)
        // - OR split into subtasks if totalSize known and chunking desired.
        if (strategy_ == DownloadStrategy::HTTP_ONLY) {
            // Create a probe task to get Content-Length if needed.
            // Simpler approach: create a single task first, wait for first notify (content length).
            dcdn::util::HttpDownloaderTaskOption probeOpt;
            probeOpt.Url = url;
            probeOpt.Start = 0;
            probeOpt.Notify = HttpNotifyCallback;
            probeOpt.Receiver = this;

            auto probe = httpDownloader_->AddTask(&probeOpt);
            if (!probe) {
                tasks_.at(task.id).status = TaskStatus::Failed;
                return task.id;
            }

            // register probe as a child downloader
            {
                std::lock_guard<std::mutex> l(ctx->mtx);
                ActiveSubTask a;
                a.parentTaskId = task.id;
                a.downloader = probe;
                a.offset = 0;
                a.length = 0;
                a.index = 0;
                // open file if needed (we'll write by offset later)
                if (options.outputStream) {
                    // no file
                } else if (!options.outputPath.empty()) {
                    auto f = std::make_shared<std::ofstream>(options.outputPath, std::ios::binary | std::ios::app);
                    if (!f->good()) f.reset();
                    a.file = f;
                }
                ctx->tasksByPtr[probe.get()] = a;
                ctx->downloaderByTaskId[task.id].push_back(probe);
            }

            // mark running
            tasks_.at(task.id).status = TaskStatus::Running;
            return task.id;
        }

        // unsupported strategies: return pending id
        tasks_.at(task.id).status = TaskStatus::Pending;
        return task.id;
    });

    return newId;
}

bool DownloadManager::cancelDownloadTask(const std::string& taskId) {
    Context* ctx = getHttpContext(this);
    if (!ctx) return false;
    return runSyncOnContext<bool>(ctx, [this, ctx, taskId]() -> bool {
        // set cancelled flag
        auto it = tasks_.find(taskId);
        if (it == tasks_.end()) return false;
        it->second.cancelled = true;
        it->second.status = TaskStatus::Cancelled;

        // cancel all child downloader tasks
        auto itVec = ctx->downloaderByTaskId.find(taskId);
        if (itVec != ctx->downloaderByTaskId.end()) {
            for (auto &sp : itVec->second) {
                if (sp) httpDownloader_->CancelTask(sp);
                // also remove mapping in tasksByPtr
                std::lock_guard<std::mutex> l(ctx->mtx);
                ctx->tasksByPtr.erase(sp.get());
            }
            itVec->second.clear();
        }
        return true;
    });
}

bool DownloadManager::pauseDownloadTask(const std::string& taskId) {
    Context* ctx = getHttpContext(this);
    if (!ctx) return false;
    return runSyncOnContext<bool>(ctx, [this, ctx, taskId]() -> bool {
        auto it = tasks_.find(taskId);
        if (it == tasks_.end()) return false;
        it->second.paused = true;
        it->second.status = TaskStatus::Paused;

        // pause all child downloader tasks
        auto itVec = ctx->downloaderByTaskId.find(taskId);
        if (itVec != ctx->downloaderByTaskId.end()) {
            for (auto &sp : itVec->second) {
                if (sp) httpDownloader_->PauseTask(sp);
            }
        }
        return true;
    });
}

bool DownloadManager::resumeDownloadTask(const std::string& taskId) {
    Context* ctx = getHttpContext(this);
    if (!ctx) return false;
    return runSyncOnContext<bool>(ctx, [this, ctx, taskId]() -> bool {
        auto it = tasks_.find(taskId);
        if (it == tasks_.end()) return false;
        it->second.paused = false;
        it->second.status = TaskStatus::Running;

        // resume all child downloader tasks, or create subtasks if probe previously ran
        auto itVec = ctx->downloaderByTaskId.find(taskId);
        if (itVec != ctx->downloaderByTaskId.end() && !itVec->second.empty()) {
            for (auto &sp : itVec->second) {
                if (sp) httpDownloader_->ResumeTask(sp);
            }
            return true;
        }

        // Edge case: no child tasks found (maybe we've only created a probe earlier which ended)
        // TODO: if no child tasks, possibly re-split and create child tasks
        return true;
    });
}

////////////////////////
// 状态查询与回调注册
////////////////////////
DownloadTask DownloadManager::getTaskStatus(const std::string& taskId) const {
    std::lock_guard<std::mutex> l(tasksMutex_);
    auto it = tasks_.find(taskId);
    if (it != tasks_.end()) return it->second;
    return {};
}

std::vector<DownloadTask> DownloadManager::getAllTasks() const {
    std::lock_guard<std::mutex> l(tasksMutex_);
    std::vector<DownloadTask> out;
    out.reserve(tasks_.size());
    for (auto &kv : tasks_) out.push_back(kv.second);
    return out;
}

double DownloadManager::getOverallSpeed() const {
    std::lock_guard<std::mutex> l(tasksMutex_);
    double sum = 0.0;
    for (auto &kv : tasks_) sum += kv.second.speed;
    return sum;
}

void DownloadManager::setBufferReadyCallback(const std::string& taskId, BufferReadyCallback callback) {
    std::lock_guard<std::mutex> l(tasksMutex_);
    bufferCallbacks_[taskId] = std::move(callback);
}

void DownloadManager::removeBufferReadyCallback(const std::string& taskId) {
    std::lock_guard<std::mutex> l(tasksMutex_);
    bufferCallbacks_.erase(taskId);
}

std::vector<std::pair<size_t, size_t>> DownloadManager::getAvailableRanges(const std::string& taskId) const {
    std::lock_guard<std::mutex> l(tasksMutex_);
    auto it = tasks_.find(taskId);
    if (it != tasks_.end()) return it->second.completedRanges;
    return {};
}

void DownloadManager::setHttpBandwidthRatio(float ratio) {
    httpBandwidthRatio_ = ratio;
}

void DownloadManager::setP2pBandwidthRatio(float ratio) {
    p2pBandwidthRatio_ = ratio;
}

////////////////////////
// 内部：进度计算与通知（以父任务为单位）
////////////////////////
void DownloadManager::updateTaskProgress(const std::string& taskId, size_t downloaded) {
    std::lock_guard<std::mutex> l(tasksMutex_);
    auto it = tasks_.find(taskId);
    if (it == tasks_.end()) return;
    it->second.downloaded += downloaded;
    it->second.lastUpdate = std::chrono::system_clock::now();
    calculateSpeed(taskId);
}

void DownloadManager::calculateSpeed(const std::string& taskId) {
    std::lock_guard<std::mutex> l(tasksMutex_);
    auto it = tasks_.find(taskId);
    if (it == tasks_.end()) return;
    auto &t = it->second;
    // 计算近似瞬时速度：用短时间窗口（这里用 startTime->lastUpdate 的秒为示例）
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(t.lastUpdate - t.startTime).count();
    if (seconds > 0) {
        t.speed = static_cast<double>(t.downloaded) / static_cast<double>(seconds);
    } else {
        t.speed = 0;
    }
}

void DownloadManager::notifyBufferReady(const std::string& taskId, size_t start, size_t end) {
    std::lock_guard<std::mutex> l(tasksMutex_);
    auto it = bufferCallbacks_.find(taskId);
    if (it != bufferCallbacks_.end()) {
        auto cb = it->second;
        if (cb) cb(taskId, start, end);
    }
}

////////////////////////
// SubTask / P2P / 混合策略（占位、部分功能在 worker 已处理）
////////////////////////
void DownloadManager::startHttpDownload(const std::string& taskId) {
    // 此函数保留为兼容 header；真正逻辑在 addDownloadTask / worker 中完成
    (void)taskId;
}

void DownloadManager::startP2pDownload(const std::string& taskId) {
    (void)taskId;
}

void DownloadManager::startHybridDownload(const std::string& taskId) {
    (void)taskId;
}

// void DownloadManager::splitTask(const std::string& taskId, size_t totalSize)
// {
//     auto it = taskOptions_.find(taskId);
//     if (it == taskOptions_.end()) {
//         throw std::runtime_error("Task options not found for taskId: " + taskId);
//     }

//     const auto& opts = it->second;
//     if (opts.chunkSize == 0) {
//         throw std::runtime_error("chunkSize must be > 0");
//     }
//     if (opts.outputPath.empty()) {
//         throw std::runtime_error("outputPath must be set");
//     }

//     // 计算总 chunk 数量
//     size_t numChunks = static_cast<size_t>(
//         std::ceil(static_cast<double>(totalSize) / static_cast<double>(opts.chunkSize))
//     );

//     // 限制同时进行的 chunk 数量（实际下载时使用 maxConcurrent_ 控制）
//     size_t concurrentChunks = std::min(numChunks, maxConcurrent_);

//     // 预分配或 truncate 文件到 totalSize
//     {
//         std::ofstream ofs(opts.outputPath, std::ios::binary | std::ios::out);
//         if (!ofs) {
//             throw std::runtime_error("Failed to open output file: " + opts.outputPath);
//         }
//         ofs.seekp(totalSize ? totalSize - 1 : 0, std::ios::beg);
//         ofs.write("", 1);
//         ofs.close();
//     }

//     // 创建 Range-based 子任务
//     struct Range {
//         size_t start;
//         size_t end; // inclusive
//     };
//     std::vector<Range> ranges;
//     ranges.reserve(numChunks);

//     size_t start = 0;
//     for (size_t i = 0; i < numChunks; ++i) {
//         size_t end = std::min(start + opts.chunkSize - 1, totalSize - 1);
//         ranges.push_back({start, end});
//         start = end + 1;
//     }

//     // 将 ranges 转成内部的下载任务
//     for (size_t i = 0; i < ranges.size(); ++i) {
//         // 这里可以生成一个子任务 ID，比如 taskId-<chunkIndex>
//         std::string subTaskId = taskId + "-" + std::to_string(i);

//         // 假设内部有个 addHttpRangeTask() 专门加 Range 请求
//         addHttpRangeTask(subTaskId, opts.outputPath, ranges[i].start, ranges[i].end);

//         if (i + 1 >= concurrentChunks) {
//             // 只添加 maxConcurrent_ 个，剩下的等调度
//             break;
//         }
//     }

//     // 如果有调度系统，可以把剩下的 ranges 放入等待队列
//     if (ranges.size() > concurrentChunks) {
//         pendingRanges_[taskId] = std::vector<Range>(
//             ranges.begin() + concurrentChunks,
//             ranges.end()
//         );
//     }
// }

// void DownloadManager::addHttpRangeTask(
//     const std::string& subTaskId,
//     const std::string& filePath,
//     size_t start,
//     size_t end)
// {
//     // 记录子任务信息
//     subTaskFilePath_[subTaskId] = filePath;
//     subTaskParent_[subTaskId] = extractParentTaskId(subTaskId); // 需要写一个辅助函数解析父ID

//     // 这里假设我们有个 HttpDownloader 封装类，可以设置 Range 请求
//     HttpDownloaderTaskOption opt;
//     opt.Url = taskOptions_[subTaskParent_[subTaskId]].url; // 父任务 URL
//     opt.RangeStart = start;
//     opt.RangeEnd = end;
//     opt.Notify = &DownloadManager::onHttpData; // 静态函数回调
//     opt.Receiver = this;

//     auto t = httpDownloader_.AddTask(&opt);
//     if (!t) {
//         throw std::runtime_error("Failed to add HTTP range task: " + subTaskId);
//     }

//     // 存储任务状态
//     subTasks_[subTaskId] = t;
// }

// void DownloadManager::onSubTaskCompleted(const std::string& taskId, size_t subtaskIndex) {
//     (void)taskId; (void)subtaskIndex;
// }

// void DownloadManager::checkTaskCompletion(const std::string& taskId) {
//     (void)taskId;
// }

// void DownloadManager::onHttpData(std::shared_ptr<DownloaderTask> task, void* receiver)
// {
//     auto* mgr = static_cast<DownloadManager*>(receiver);
//     mgr->handleHttpData(task.get());
// }

// void DownloadManager::handleHttpData(DownloaderTask* task)
// {
//     // 找到子任务 ID
//     std::string subTaskId;
//     for (auto& [id, t] : subTasks_) {
//         if (t.get() == task) {
//             subTaskId = id;
//             break;
//         }
//     }
//     if (subTaskId.empty()) return;

//     auto fileIt = subTaskFilePath_.find(subTaskId);
//     if (fileIt == subTaskFilePath_.end()) return;

//     const std::string& filePath = fileIt->second;

//     // 获取数据并写入指定位置
//     auto data = task->Read();
//     size_t writePos = task->RangeStart(); // 需要 HttpDownloaderTask 支持返回 RangeStart
//     std::ofstream ofs(filePath, std::ios::binary | std::ios::in | std::ios::out);
//     if (!ofs) {
//         throw std::runtime_error("Failed to open file for writing: " + filePath);
//     }

//     while (data) {
//         ofs.seekp(writePos, std::ios::beg);
//         ofs.write(reinterpret_cast<const char*>(data->Data()), data->Length());
//         writePos += data->Length();
//         data = data->Next();
//     }

//     if (task->IsEnd()) {
//         finalizeSubTask(subTaskId);
//     }
// }


// void DownloadManager::finalizeSubTask(const std::string& subTaskId)
// {
//     std::string parentId = subTaskParent_[subTaskId];
//     subTasks_.erase(subTaskId);
//     subTaskFilePath_.erase(subTaskId);
//     subTaskParent_.erase(subTaskId);

//     auto pendIt = pendingRanges_.find(parentId);
//     if (pendIt != pendingRanges_.end() && !pendIt->second.empty()) {
//         auto range = pendIt->second.front();
//         pendIt->second.erase(pendIt->second.begin());

//         std::string newSubId = parentId + "-" + std::to_string(parentRangeIndex_[parentId]++);
//         addHttpRangeTask(newSubId, taskOptions_[parentId].outputFile, range.start, range.end);
//     }
// }
