#include "DownloadManager.h"

#include <fstream>
#include <iostream>
#include <memory>
#include <thread>
#include <condition_variable>
#include <unordered_set>
#include <chrono>
#include <deque>
#include <future>
#include <algorithm>
#include <sstream>

using namespace dcdn;

namespace {

// 传输类型（为未来 P2P/HYBRID 预留）
enum class Transport { HTTP, P2P };

// 统一的最小子任务描述（HTTP/P2P 复用）
struct ActiveSubTask {
    std::string parentTaskId; // 所属主任务 id
    Transport transport = Transport::HTTP;
    size_t offset = 0;          // Range start（含）
    size_t length = 0;          // Range 长度（end-start+1）, 0 表示不确定（直到 IsEnd 或 ContentLength 可知）
    int index = 0;              // 该子任务在父任务的分片序号
    std::shared_ptr<dcdn::util::DownloaderTask> downloader;
    std::shared_ptr<std::fstream> file;  // 父任务共享随机写文件,可能共享同一个文件（按 offset 写需 seek）
    size_t lastReportSize = 0;
};

// 引擎上下文（协议无关）：统一串行执行 public API + 处理 downloader 事件
struct CoreContext {
    std::mutex mtx;
    std::condition_variable cv;

    // downloader raw 指针 -> 子任务
    std::unordered_map<dcdn::util::DownloaderTask*, ActiveSubTask> tasksByPtr;

    // 父任务 -> 当前运行中的 downloader 列表
    std::unordered_map<std::string, std::vector<std::shared_ptr<dcdn::util::DownloaderTask>>> downloaderByTaskId;

    // downloader 通知事件队列
    std::unordered_set<dcdn::util::DownloaderTask*> events;

    // 串行化命令队列
    std::deque<std::function<void()>> cmdQueue;

    // 父任务 -> 等待调度的 Range 队列
    struct Range { size_t start; size_t end; };
    std::unordered_map<std::string, std::deque<Range>> pendingRanges;
    std::unordered_map<std::string, size_t> nextRangeIndex;

    // 父任务 -> 共享随机写文件句柄
    std::unordered_map<std::string, std::shared_ptr<std::fstream>> parentFiles;

    std::thread worker;
    bool running = false;
};

// 全局：manager -> CoreContext
static std::unordered_map<DownloadManager*, std::unique_ptr<CoreContext>> g_core;

static CoreContext* getCore(DownloadManager* mgr) {
    auto it = g_core.find(mgr);
    if (it == g_core.end()) return nullptr;
    return it->second.get();
}

// 统一的 downloader notify 回调
static void CoreNotifyCallback(std::shared_ptr<dcdn::util::DownloaderTask> task, void* receiver) {
    if (!task || !receiver) return;
    auto* mgr = static_cast<DownloadManager*>(receiver);
    auto* core = getCore(mgr);
    if (!core) return;
    {
        std::lock_guard<std::mutex> l(core->mtx);
        // 如果 tasksByPtr 中没有这个指针，先插入一个占位（后续 addDownloadTask 会填充）
        auto it = core->tasksByPtr.find(task.get());
        if (it == core->tasksByPtr.end()) {
            ActiveSubTask a;
            a.downloader = task;
            core->tasksByPtr[task.get()] = std::move(a);
        } else {
            it->second.downloader = task;
        }
        core->events.insert(task.get());
    }
    core->cv.notify_one();
}

static std::string genTaskId() {
    static std::atomic<uint64_t> cnt{0};
    std::ostringstream ss;
    ss << std::chrono::steady_clock::now().time_since_epoch().count() << "_" << cnt++;
    return ss.str();
}

// 在 manager 线程同步执行（串行化 public API）
template<typename R>
static R runSyncOnCore(CoreContext* core, std::function<R()> fn) {
    auto prom = std::make_shared<std::promise<R>>();
    auto fut = prom->get_future();
    {
        std::lock_guard<std::mutex> l(core->mtx);
        core->cmdQueue.emplace_back([prom, fn]() {
            try { prom->set_value(fn()); }
            catch (...) { try { prom->set_exception(std::current_exception()); } catch (...) {} }
        });
    }
    core->cv.notify_one();
    return fut.get();
}

static void runAsyncOnCore(CoreContext* core, std::function<void()> fn) {
    {
        std::lock_guard<std::mutex> l(core->mtx);
        core->cmdQueue.emplace_back(std::move(fn));
    }
    core->cv.notify_one();
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
}

DownloadManager::PersistenceHelper::~PersistenceHelper() {}

bool DownloadManager::PersistenceHelper::saveTask(const DownloadTask& task) { (void)task; return true; }
bool DownloadManager::PersistenceHelper::loadTasks(std::vector<DownloadTask>& tasks) { (void)tasks; return true; }
bool DownloadManager::PersistenceHelper::deleteTask(const std::string& taskId) { (void)taskId; return true; }
bool DownloadManager::PersistenceHelper::saveSubTasks(const std::string& taskId, const std::vector<SubTask>& subtasks) {
    (void)taskId; (void)subtasks; return true;
}
bool DownloadManager::PersistenceHelper::loadSubTasks(const std::string& taskId, std::vector<SubTask>& subtasks) {
    (void)taskId; (void)subtasks; return true;
}

////////////////////////
// DownloadManager 构造 / 析构
////////////////////////
DownloadManager::DownloadManager() {
    // 初始化 HTTP 引擎
    httpDownloader_ = std::make_unique<util::HttpDownloader>();
    int ret = httpDownloader_->Init(nullptr);
    if (ret != 1) {
        std::cerr << "Warning: HttpDownloader Init returned " << ret << std::endl;
    }
    httpDownloader_->Start();

    // 构建通用上下文, 并启动worker
    auto core = std::make_unique<CoreContext>();
    core->running = true;

    // worker: 串行执行 cmdQueue，然后处理 events（读数据 -> 写文件 -> 更新状态）
    core->worker = std::thread([this]() {
        std::cout << "worker started" << std::endl;
        CoreContext* core = getCore(this);
        if (!core) return;
        while (true) {
            std::function<void()> cmd;
            dcdn::util::DownloaderTask* evPtr = nullptr;

            {
                std::unique_lock<std::mutex> l(core->mtx);
                // // 优先执行命令队列（控制指令）
                if (!core->cmdQueue.empty()) {
                    cmd = std::move(core->cmdQueue.front());
                    core->cmdQueue.pop_front();
                } else if (!core->events.empty()) {
                    auto it = core->events.begin();
                    evPtr = *it;
                    core->events.erase(it);
                } else if (!core->running) {
                    break;
                } else {
                    // nothing happened, wait
                    core->cv.wait(l);
                    continue;
                }
            }

            if (cmd) { // 执行控制命令
                try { cmd(); } catch (...) {}
                continue;
            }

            if (evPtr) { // 处理 downloader 事件
                ActiveSubTask active;
                {
                    std::lock_guard<std::mutex> l(core->mtx);
                    auto it = core->tasksByPtr.find(evPtr);
                    if (it == core->tasksByPtr.end()) continue;
                    active = it->second;
                }
                // 从 core->tasksByPtr 取到 active 之后
                if (active.parentTaskId.empty()) {
                    std::cout << "warning: active.parentTaskId is empty" << std::endl;
                    bool belongsToAnyParent = false;
                    {
                        std::lock_guard<std::mutex> l(core->mtx);
                        // 判断这个 evPtr 是否仍然在任何父任务的 downloader 列表里
                        for (auto &kv : core->downloaderByTaskId) {
                            auto &vec = kv.second;
                            if (std::any_of(vec.begin(), vec.end(),
                                            [evPtr](const std::shared_ptr<dcdn::util::DownloaderTask>& p){
                                                return p && p.get() == evPtr;
                                            })) {
                                belongsToAnyParent = true;
                                break;
                            }
                        }
                        if (!belongsToAnyParent) {
                            // 孤儿通知：可能是 cancel 后的迟到回调，直接删除占位并丢弃
                            core->tasksByPtr.erase(evPtr);
                        }
                    }

                    if (belongsToAnyParent) {
                        // 仍处于“正在注册”的 race，允许下一轮再处理
                        std::this_thread::sleep_for(std::chrono::milliseconds(50));
                        std::lock_guard<std::mutex> l2(core->mtx);
                        core->events.insert(evPtr);
                    }
                    // 不再打印 warning，也不自旋
                    continue;
                }
                auto sp = active.downloader;
                if (!sp) continue;
                auto raw = sp.get();

                // 若是 HTTP probe：尝试拿到 Content-Length 触发 split
                if (strategy_ == DownloadStrategy::HTTP_ONLY && tasks_.count(active.parentTaskId)) {
                    DownloadTask parent;
                    {
                        std::lock_guard<std::mutex> lk(tasksMutex_);
                        parent = tasks_.at(active.parentTaskId);
                    }
                    if (parent.totalSize == 0) {
                        if (auto ht = dynamic_cast<dcdn::util::HttpDownloaderTask*>(raw)) {
                            size_t clen = ht->ContentLength();
                            if (clen > 0) {
                                {
                                    std::lock_guard<std::mutex> lk(tasksMutex_);
                                    tasks_[active.parentTaskId].totalSize = clen;
                                }
                                // 在当前 manager 线程直接切分
                                splitTask(active.parentTaskId, clen);
                            }
                        }
                    }
                }

                // 读取并写入
                bool isEnd = raw->IsEnd();
                auto buffer = raw->Read();
                size_t readSum = 0;
                while (buffer) {
                    size_t len = buffer->Length();
                    size_t off = buffer->Offset();
                    // 获取 options (主任务的 options)
                    FileDownloadOptions opts;
                    std::shared_ptr<std::fstream> file;
                    {
                        std::lock_guard<std::mutex> lk(tasksMutex_);
                        auto itOpt = taskOptions_.find(active.parentTaskId);
                        if (itOpt != taskOptions_.end()) opts = itOpt->second;
                    }
                    {
                        std::lock_guard<std::mutex> l(core->mtx);
                        auto itF = core->parentFiles.find(active.parentTaskId);
                        if (itF != core->parentFiles.end()) file = itF->second;
                    }

                    if (file && file->good()) {
                        try { file->seekp(static_cast<std::streamoff>(off), std::ios::beg); } catch (...) {}
                        file->write(reinterpret_cast<const char*>(buffer->Data()), len);
                        file->flush();
                    } else if (opts.outputStream) {
                        opts.outputStream->write(reinterpret_cast<const char*>(buffer->Data()), len);
                        opts.outputStream->flush();
                    } else if (!opts.outputPath.empty()) {
                        // 兜底：随机写
                        std::fstream ofs(opts.outputPath, std::ios::in | std::ios::out | std::ios::binary);
                        if (!ofs) { std::ofstream create(opts.outputPath, std::ios::binary); create.close();
                                    ofs.open(opts.outputPath, std::ios::in | std::ios::out | std::ios::binary); }
                        if (ofs) { ofs.seekp(static_cast<std::streamoff>(off), std::ios::beg);
                                   ofs.write(reinterpret_cast<const char*>(buffer->Data()), len); }
                    }

                    if (opts.streamCallback) {
                        opts.streamCallback(reinterpret_cast<const char*>(buffer->Data()), len, off);
                    }

                    readSum += len;
                    buffer = buffer->Next();
                }

                if (readSum > 0) {
                    updateTaskProgress(active.parentTaskId, readSum);
                    notifyBufferReady(active.parentTaskId, active.offset, active.offset + readSum);
                }

                // 子任务结束：清理并尝试续排 pendingRanges
                if (isEnd) {
                    // 标记该子任务完成（从 ctx->tasksByPtr / downloaderByTaskId 清理）
                    {
                        std::lock_guard<std::mutex> l(core->mtx);
                        core->tasksByPtr.erase(raw);
                        auto &vec = core->downloaderByTaskId[active.parentTaskId];
                        vec.erase(std::remove_if(vec.begin(), vec.end(),
                            [raw](const std::shared_ptr<dcdn::util::DownloaderTask>& p){ return p.get() == raw; }),
                            vec.end());
                    }

                    bool launched = false;
                    {
                        std::lock_guard<std::mutex> l(core->mtx);
                        auto &q = core->pendingRanges[active.parentTaskId];
                        if (!q.empty()) {
                            auto r = q.front(); q.pop_front();
                            size_t idx = core->nextRangeIndex[active.parentTaskId]++;

                            dcdn::util::HttpDownloaderTaskOption opt;
                            {
                                std::lock_guard<std::mutex> lk(tasksMutex_);
                                opt.Url = tasks_.at(active.parentTaskId).url;
                            }
                            opt.Start = r.start;
                            opt.End   = r.end;
                            opt.Notify = CoreNotifyCallback;
                            opt.Receiver = this;

                            auto sub = httpDownloader_->AddTask(&opt);
                            if (sub) {
                                std::shared_ptr<std::fstream> f;
                                auto itF = core->parentFiles.find(active.parentTaskId);
                                if (itF != core->parentFiles.end()) f = itF->second;

                                ActiveSubTask st;
                                st.parentTaskId = active.parentTaskId;
                                st.transport = Transport::HTTP;
                                st.offset = r.start;
                                st.length = r.end - r.start + 1;
                                st.index = static_cast<int>(idx);
                                st.downloader = sub;
                                st.file = f;

                                core->tasksByPtr[sub.get()] = std::move(st);
                                core->downloaderByTaskId[active.parentTaskId].push_back(sub);
                                launched = true;
                            }
                        }
                    }

                    // 无运行子任务且无 pending，则父任务完成/失败
                    bool stillRunning = false;
                    {
                        std::lock_guard<std::mutex> l(core->mtx);
                        if (!core->downloaderByTaskId[active.parentTaskId].empty()) stillRunning = true;
                        if (!stillRunning && !core->pendingRanges[active.parentTaskId].empty()) stillRunning = true;
                    }
                    if (!stillRunning) {
                        auto st = raw->Status();
                        std::lock_guard<std::mutex> lk(tasksMutex_);
                        auto &parent = tasks_.at(active.parentTaskId);
                        if (st == dcdn::util::DownloaderTask::Completed && !parent.cancelled)
                            parent.status = TaskStatus::Completed;
                        else if (parent.cancelled)
                            parent.status = TaskStatus::Cancelled;
                        else
                            parent.status = TaskStatus::Failed;
                    }
                }
            } // evPtr
        } // while
    });

    g_core[this] = std::move(core);
}

DownloadManager::~DownloadManager() {
    // 停止 CoreContext worker
    auto it = g_core.find(this);
    if (it != g_core.end()) {
        CoreContext* core = it->second.get();
        {
            std::lock_guard<std::mutex> l(core->mtx);
            core->running = false;
            core->cv.notify_all();
        }
        if (core->worker.joinable()) core->worker.join();
        g_core.erase(it);
    }

    if (httpDownloader_) {
        httpDownloader_.reset();
    }
}

////////////////////////
// 配置接口
////////////////////////
void DownloadManager::setStrategy(DownloadStrategy strategy) { strategy_ = strategy; }
void DownloadManager::setMaxConcurrentDownloads(size_t max) { maxConcurrent_ = max; }
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
    CoreContext* core = getCore(this);
    if (!core) return {};

    return runSyncOnCore<std::string>(core, [this, core, url, contentHash, options]() -> std::string {
        DownloadTask task;
        task.id = genTaskId();
        task.url = url;
        task.contentHash = contentHash;
        task.totalSize = 0;
        task.downloaded = 0;
        task.startTime = std::chrono::system_clock::now();
        task.lastUpdate = task.startTime;
        task.status = TaskStatus::Pending;

        tasks_[task.id] = task;
        taskOptions_[task.id] = options;

        // 打开父任务共享文件（随机写）
        if (!options.outputPath.empty()) {
            auto fs = std::make_shared<std::fstream>(options.outputPath,
                        std::ios::in | std::ios::out | std::ios::binary);
            if (!fs->is_open()) {
                std::ofstream create(options.outputPath, std::ios::binary);
                create.close();
                fs->open(options.outputPath, std::ios::in | std::ios::out | std::ios::binary);
            }
            if (fs->is_open()) {
                std::lock_guard<std::mutex> l(core->mtx);
                core->parentFiles[task.id] = fs;
            }
        }

        if (strategy_ == DownloadStrategy::HTTP_ONLY) {
            // 先创建一个 probe 以获取 Content-Length
            dcdn::util::HttpDownloaderTaskOption opt;
            opt.Url = url;
            opt.Start = 0;
            opt.Notify = CoreNotifyCallback;
            opt.Receiver = this;

            auto probe = httpDownloader_->AddTask(&opt);
            if (!probe) {
                tasks_.at(task.id).status = TaskStatus::Failed;
                return task.id;
            }

            ActiveSubTask a;
            a.parentTaskId = task.id;
            a.transport = Transport::HTTP;
            a.downloader = probe;
            a.offset = 0;
            a.length = 0;
            a.index = 0;

            {
                std::lock_guard<std::mutex> l(core->mtx);

                auto itF = core->parentFiles.find(task.id);
                std::shared_ptr<std::fstream> sharedFile = (itF != core->parentFiles.end()) ? itF->second : nullptr;

                auto itExisting = core->tasksByPtr.find(probe.get());
                if (itExisting == core->tasksByPtr.end()) {
                    ActiveSubTask a;
                    a.parentTaskId = task.id;
                    a.transport = Transport::HTTP;
                    a.downloader = probe;
                    a.offset = 0;
                    a.length = 0;
                    a.index = 0;
                    a.file = sharedFile;
                    core->tasksByPtr[probe.get()] = std::move(a);
                } else {
                    // 补齐占位对象的关键信息
                    itExisting->second.parentTaskId = task.id;
                    itExisting->second.transport = Transport::HTTP;
                    itExisting->second.downloader = probe;
                    itExisting->second.offset = 0;
                    itExisting->second.length = 0;
                    itExisting->second.index = 0;
                    itExisting->second.file = sharedFile;
                }
                std::cout << "pushed probe, parent task id = " << core->tasksByPtr[probe.get()].parentTaskId << std::endl;
                core->downloaderByTaskId[task.id].push_back(probe);
            } 

            tasks_.at(task.id).status = TaskStatus::Running;
            return task.id;
        }

        // 其他策略暂未实现
        tasks_.at(task.id).status = TaskStatus::Pending;
        return task.id;
    });
}

bool DownloadManager::cancelDownloadTask(const std::string& taskId) {
    CoreContext* core = getCore(this);
    if (!core) return false;
    return runSyncOnCore<bool>(core, [this, core, taskId]() -> bool {
        auto it = tasks_.find(taskId);
        if (it == tasks_.end()) return false;
        it->second.cancelled = true;
        it->second.status = TaskStatus::Cancelled;

        auto itVec = core->downloaderByTaskId.find(taskId);
        if (itVec != core->downloaderByTaskId.end()) {
            for (auto &sp : itVec->second) {
                if (!sp) continue;
                // 1) 先让底层取消
                httpDownloader_->CancelTask(sp);
                std::lock_guard<std::mutex> l(core->mtx);
                // 2) 把可能残留的事件移除
                core->events.erase(sp.get());
                // 3) 把占位/记录移除
                core->tasksByPtr.erase(sp.get());
            }
            itVec->second.clear();
        }
        // 清空 pending
        core->pendingRanges[taskId].clear();
        return true;
    });
}

bool DownloadManager::pauseDownloadTask(const std::string& taskId) {
    CoreContext* core = getCore(this);
    if (!core) return false;
    return runSyncOnCore<bool>(core, [this, core, taskId]() -> bool {
        auto it = tasks_.find(taskId);
        if (it == tasks_.end()) return false;
        it->second.paused = true;
        it->second.status = TaskStatus::Paused;

        auto itVec = core->downloaderByTaskId.find(taskId);
        if (itVec != core->downloaderByTaskId.end()) {
            for (auto &sp : itVec->second) {
                if (sp) httpDownloader_->PauseTask(sp);
            }
        }
        return true;
    });
}

bool DownloadManager::resumeDownloadTask(const std::string& taskId) {
    CoreContext* core = getCore(this);
    if (!core) return false;
    return runSyncOnCore<bool>(core, [this, core, taskId]() -> bool {
        auto it = tasks_.find(taskId);
        if (it == tasks_.end()) return false;
        it->second.paused = false;
        it->second.status = TaskStatus::Running;

        auto itVec = core->downloaderByTaskId.find(taskId);
        if (itVec != core->downloaderByTaskId.end()) {
            for (auto &sp : itVec->second) {
                if (sp) httpDownloader_->ResumeTask(sp);
            }
        }
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

void DownloadManager::setHttpBandwidthRatio(float ratio) { httpBandwidthRatio_ = ratio; }
void DownloadManager::setP2pBandwidthRatio(float ratio) { p2pBandwidthRatio_ = ratio; }

////////////////////////
// 内部：进度计算与通知（以父任务为单位）
////////////////////////
void DownloadManager::updateTaskProgress(const std::string& taskId, size_t downloaded) {
    auto now = std::chrono::system_clock::now();
    std::lock_guard<std::mutex> l(tasksMutex_);
    auto it = tasks_.find(taskId);
    if (it == tasks_.end()) return;
    it->second.downloaded += downloaded;
    it->second.lastUpdate = now;
    calculateSpeedLocked(it->second, now);  // 不再二次加锁
}

// 新增：仅在已持有 tasksMutex_ 时调用
void DownloadManager::calculateSpeedLocked(DownloadTask& t,
                                 std::chrono::system_clock::time_point now) {
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(now - t.startTime).count();
    t.speed = (seconds > 0) ? (static_cast<double>(t.downloaded) / static_cast<double>(seconds)) : 0.0;
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
// SubTask / P2P / 混合策略（占位）
////////////////////////
void DownloadManager::startHttpDownload(const std::string& taskId) { (void)taskId; }
void DownloadManager::startP2pDownload(const std::string& taskId) { (void)taskId; }
void DownloadManager::startHybridDownload(const std::string& taskId) { (void)taskId; }

// 在探测到 Content-Length 后切分 & 调度（HTTP_ONLY）
void DownloadManager::splitTask(const std::string& taskId, size_t totalSize) {
    CoreContext* core = getCore(this);
    if (!core) return;

    FileDownloadOptions opts;
    std::string url;
    {
        std::lock_guard<std::mutex> lk(tasksMutex_);
        auto it = tasks_.find(taskId);
        if (it == tasks_.end()) return;
        url = it->second.url;
        auto itOpt = taskOptions_.find(taskId);
        if (itOpt != taskOptions_.end()) opts = itOpt->second;
    }

    // 预分配/截断文件到 totalSize
    if (!opts.outputPath.empty()) {
        auto fs = std::make_shared<std::fstream>(opts.outputPath,
                    std::ios::in | std::ios::out | std::ios::binary);
        if (!fs->is_open()) {
            std::ofstream create(opts.outputPath, std::ios::binary);
            create.close();
            fs->open(opts.outputPath, std::ios::in | std::ios::out | std::ios::binary);
        }
        if (fs->is_open() && totalSize > 0) {
            try {
                fs->seekp(static_cast<std::streamoff>(totalSize - 1), std::ios::beg);
                char z = 0; fs->write(&z, 1); fs->flush();
            } catch (...) {}
            std::lock_guard<std::mutex> l(core->mtx);
            core->parentFiles[taskId] = fs;
        }
    }

    // 取消 probe（若在跑）
    std::shared_ptr<dcdn::util::DownloaderTask> probeToCancel;
    size_t already = 0;
    {
        std::lock_guard<std::mutex> l(core->mtx);
        auto &vec = core->downloaderByTaskId[taskId];
        if (!vec.empty()) probeToCancel = vec.front();
    }
    if (probeToCancel) {
        already = probeToCancel->Size();
        auto  taskCancle = std::dynamic_pointer_cast<dcdn::util::HttpDownloaderTask>(probeToCancel);
        httpDownloader_->CancelTask(taskCancle);
        std::cout << "httpDownloader: probe cancel" << std::endl;
        std::lock_guard<std::mutex> l(core->mtx);
        core->tasksByPtr.erase(probeToCancel.get());
        auto &vec = core->downloaderByTaskId[taskId];
        vec.erase(std::remove(vec.begin(), vec.end(), probeToCancel), vec.end());
    }

    // 生成 ranges
    size_t chunk = (opts.chunkSize ? opts.chunkSize : (1u << 20)); // default 1MB
    std::vector<CoreContext::Range> ranges;
    ranges.reserve((totalSize + chunk - 1) / chunk);

    // 第一块考虑 probe 已下载 [0, already)
    if (already < totalSize) {
        size_t firstEnd = std::min(chunk - 1, totalSize - 1);
        ranges.push_back({ already, firstEnd });
    }
    // 其余块
    for (size_t pos = chunk; pos < totalSize; pos += chunk) {
        size_t end = std::min(pos + chunk - 1, totalSize - 1);
        ranges.push_back({ pos, end });
    }

    // 并发启动
    size_t canLaunch = std::min(ranges.size(), maxConcurrent_);

    std::shared_ptr<std::fstream> parentFile;
    {
        std::lock_guard<std::mutex> l(core->mtx);
        auto itF = core->parentFiles.find(taskId);
        if (itF != core->parentFiles.end()) parentFile = itF->second;
        core->nextRangeIndex[taskId] = 0;
    }

    for (size_t i = 0; i < canLaunch; ++i) {
        auto r = ranges[i];
        dcdn::util::HttpDownloaderTaskOption opt;
        opt.Url = url;
        opt.Start = r.start;
        opt.End   = r.end;
        opt.Notify = CoreNotifyCallback;
        opt.Receiver = this;

        auto sub = httpDownloader_->AddTask(&opt);
        if (!sub) continue;

        ActiveSubTask st;
        st.parentTaskId = taskId;
        st.transport = Transport::HTTP;
        st.offset = r.start;
        st.length = r.end - r.start + 1;
        st.index = static_cast<int>(i);
        st.downloader = sub;
        st.file = parentFile;

        std::lock_guard<std::mutex> l(core->mtx);
        core->tasksByPtr[sub.get()] = std::move(st);
        core->downloaderByTaskId[taskId].push_back(sub);
        core->nextRangeIndex[taskId] = i + 1;
    }

    // 余下入队
    if (ranges.size() > canLaunch) {
        std::lock_guard<std::mutex> l(core->mtx);
        auto &q = core->pendingRanges[taskId];
        for (size_t i = canLaunch; i < ranges.size(); ++i) q.push_back(ranges[i]);
    }
}

void DownloadManager::onSubTaskCompleted(const std::string& taskId, size_t subtaskIndex) {
    (void)taskId; (void)subtaskIndex;
    // 逻辑已在 worker 的 IsEnd 分支里做了统一处理
}
