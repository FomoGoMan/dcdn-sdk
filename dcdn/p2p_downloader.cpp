// DownloadManager.cpp
#include "DownloadManager.h"

#include "util/HttpDownloader.h"
#include <fstream>
#include <iostream>
#include <thread>
#include <condition_variable>
#include <unordered_set>
#include <chrono>

using namespace dcdn;

namespace {

// 内部为每个 DownloadManager 实例维护一个 HTTP 运行上下文
struct HttpActiveTask {
    std::string taskId;
    std::shared_ptr<dcdn::util::DownloaderTask> downloader; // shared_ptr 保持生命周期
    std::shared_ptr<std::ofstream> file; // 如果要写文件
    std::string url;
    size_t lastReportSize = 0;
};

struct HttpContext {
    std::mutex mtx;
    std::condition_variable cv;
    // downloader ptr -> active task
    std::unordered_map<dcdn::util::DownloaderTask*, HttpActiveTask> tasksByPtr;
    // taskId -> downloader shared_ptr
    std::unordered_map<std::string, std::shared_ptr<dcdn::util::DownloaderTask>> downloaderByTaskId;
    std::unordered_set<dcdn::util::DownloaderTask*> events; // 被 notify 的任务集合（待处理）
    std::thread worker;
    bool running = true;
};

// 全局 map：DownloadManager* -> HttpContext
static std::unordered_map<DownloadManager*, std::unique_ptr<HttpContext>> g_httpContexts;

// 从 manager 获取 HttpContext 指针（可能为 nullptr）
static HttpContext* getHttpContext(DownloadManager* mgr) {
    auto it = g_httpContexts.find(mgr);
    if (it == g_httpContexts.end()) return nullptr;
    return it->second.get();
}

// HttpDownloader 的 notify 回调（传入 shared_ptr<DownloaderTask>, receiver 为 DownloadManager*）
static void HttpNotifyCallback(std::shared_ptr<dcdn::util::DownloaderTask> task, void* receiver) {
    if (!task || !receiver) return;
    DownloadManager* mgr = static_cast<DownloadManager*>(receiver);
    HttpContext* ctx = getHttpContext(mgr);
    if (!ctx) return;
    {
        std::lock_guard<std::mutex> l(ctx->mtx);
        // 如果 tasksByPtr 中没有这个指针，可能是第一次通知 —— 先插入一个占位（downloader will be set at AddTask）
        auto it = ctx->tasksByPtr.find(task.get());
        if (it == ctx->tasksByPtr.end()) {
            HttpActiveTask a;
            a.downloader = task;
            ctx->tasksByPtr[task.get()] = std::move(a);
        } else {
            // 更新 shared_ptr（以防调用方并未在 addDownloadTask 中填充）
            it->second.downloader = task;
        }
        ctx->events.insert(task.get());
    }
    ctx->cv.notify_one();
}

} // namespace (anon)

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
// PersistenceHelper (TODOs)
////////////////////////
DownloadManager::PersistenceHelper::PersistenceHelper(const std::string& dbPath) {
    db_ = nullptr;
    // TODO: 打开 sqlite3 及建表
}

DownloadManager::PersistenceHelper::~PersistenceHelper() {
    // TODO: 关闭 sqlite3
}

bool DownloadManager::PersistenceHelper::saveTask(const DownloadTask& task) {
    // TODO: persist
    (void)task;
    return true;
}

bool DownloadManager::PersistenceHelper::loadTasks(std::vector<DownloadTask>& tasks) {
    (void)tasks;
    // TODO: load
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
    // 创建 HttpDownloader 并初始化、启动
    httpDownloader_ = std::make_unique<util::HttpDownloader>();
    int ret = httpDownloader_->Init(nullptr);
    if (ret != 1) {
        // 仅输出提示，仍继续以便上层决定
        std::cerr << "Warning: HttpDownloader Init returned " << ret << std::endl;
    }
    // 启动 http 下载器的内部线程
    httpDownloader_->Start();

    // 创建并注册 HttpContext
    auto ctx = std::make_unique<HttpContext>();
    ctx->running = true;

    // 创建 worker 线程：处理 notify 到来的事件（读取 DownloaderTask 的缓冲区并写入）
    ctx->worker = std::thread([this]() {
        HttpContext* ctx = getHttpContext(this);
        if (!ctx) return;
        while (true) {
            dcdn::util::DownloaderTask* tptr = nullptr;
            {
                std::unique_lock<std::mutex> l(ctx->mtx);
                ctx->cv.wait(l, [&] { return !ctx->events.empty() || !ctx->running; });
                if (!ctx->running && ctx->events.empty()) break;
                if (!ctx->events.empty()) {
                    auto it = ctx->events.begin();
                    tptr = *it;
                    ctx->events.erase(it);
                }
            }
            if (!tptr) continue;

            HttpActiveTask active;
            {
                std::lock_guard<std::mutex> l(ctx->mtx);
                auto it = ctx->tasksByPtr.find(tptr);
                if (it == ctx->tasksByPtr.end()) {
                    continue;
                }
                active = it->second; // 复制一份，便于释放 ctx 锁后处理
            }

            // 确保 shared_ptr 有效
            auto downloaderSP = active.downloader;
            if (!downloaderSP) {
                // 可能是尚未在 addDownloadTask 中注册 shared_ptr（极少数情况），跳过
                continue;
            }
            dcdn::util::DownloaderTask* raw = downloaderSP.get();

            // 读取可用数据并写入
            bool isEnd = raw->IsEnd();
            auto buffer = raw->Read();
            while (buffer) {
                size_t len = buffer->Length();
                size_t offset = buffer->Offset();

                // 获取 options
                FileDownloadOptions opts;
                {
                    std::lock_guard<std::mutex> l(tasksMutex_);
                    auto itOpt = taskOptions_.find(active.taskId);
                    if (itOpt != taskOptions_.end()) opts = itOpt->second;
                }

                // 写入流或文件（优先 outputStream，再 file，再 outputPath）
                if (opts.outputStream) {
                    opts.outputStream->write(reinterpret_cast<const char*>(buffer->Data()), len);
                    opts.outputStream->flush();
                } else if (active.file && active.file->good()) {
                    // 尝试定位（若序列化的实现之后改变，可按 offset 写）
                    // 这里采用直接写（大多数 HTTP 场景数据按顺序到达）
                    active.file->write(reinterpret_cast<const char*>(buffer->Data()), len);
                    active.file->flush();
                } else if (!opts.outputPath.empty()) {
                    // 按需打开并写（append）
                    std::ofstream ofs(opts.outputPath, std::ios::binary | std::ios::app);
                    if (ofs) {
                        ofs.write(reinterpret_cast<const char*>(buffer->Data()), len);
                        ofs.close();
                    }
                }

                // 流回调（媒体播放场景）
                if (opts.streamCallback) {
                    opts.streamCallback(reinterpret_cast<const char*>(buffer->Data()), len, offset);
                }

                // 更新进度并通知缓冲可用
                updateTaskProgress(active.taskId, len);
                notifyBufferReady(active.taskId, offset, offset + len);

                buffer = buffer->Next();
            }

            // 如果该 DownloaderTask 已结束，做清理与任务状态更新
            if (raw->IsEnd()) {
                auto st = raw->Status(); // DownloaderTask::Completed or Fail
                {
                    std::lock_guard<std::mutex> l(tasksMutex_);
                    auto itTask = tasks_.find(active.taskId);
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
                // 清理 HttpContext 中对应映射
                {
                    std::lock_guard<std::mutex> l(ctx->mtx);
                    ctx->downloaderByTaskId.erase(active.taskId);
                    ctx->tasksByPtr.erase(raw);
                }
                // 关闭文件流（if any)
                if (active.file) {
                    try { active.file->close(); } catch (...) {}
                }
            }
        } // while
    });

    g_httpContexts[this] = std::move(ctx);
}

DownloadManager::~DownloadManager() {
    // 停止 HttpContext worker
    auto it = g_httpContexts.find(this);
    if (it != g_httpContexts.end()) {
        HttpContext* ctx = it->second.get();
        {
            std::lock_guard<std::mutex> l(ctx->mtx);
            ctx->running = false;
        }
        ctx->cv.notify_all();
        if (ctx->worker.joinable()) ctx->worker.join();
        g_httpContexts.erase(it);
    }

    // 停止 http downloader
    if (httpDownloader_) {
        // TODO: 如需安全停止运行中的 tasks，可先 CancelTask 所有 active downloader
        httpDownloader_.reset();
    }
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
    // 创建任务元数据
    DownloadTask task;
    task.id = std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
    task.url = url;
    task.contentHash = contentHash;
    task.totalSize = 0; // 若能通过 HEAD 获取可以更新（TODO）
    task.downloaded = 0;
    task.startTime = std::chrono::system_clock::now();
    task.lastUpdate = task.startTime;
    task.status = TaskStatus::Pending;

    {
        std::lock_guard<std::mutex> l(tasksMutex_);
        tasks_[task.id] = task;
        taskOptions_[task.id] = options;
    }

    // HTTP_ONLY 模式时，走 HttpDownloader
    if (strategy_ == DownloadStrategy::HTTP_ONLY) {
        // 准备 HttpDownloaderTaskOption
        dcdn::util::HttpDownloaderTaskOption opt;
        opt.Url = url;
        opt.Start = 0; // TODO：若实现断点续传，从已保存偏移开始
        opt.Notify = HttpNotifyCallback;
        opt.Receiver = this;

        auto downloaderTask = httpDownloader_->AddTask(&opt);
        if (!downloaderTask) {
            // 添加失败
            std::lock_guard<std::mutex> l(tasksMutex_);
            tasks_.at(task.id).status = TaskStatus::Failed;
            return task.id;
        }

        // 在 HttpContext 中注册并保存文件句柄/stream
        HttpContext* ctx = getHttpContext(this);
        if (!ctx) {
            // should not happen, 但如果没有 context，则直接失败
            std::lock_guard<std::mutex> l(tasksMutex_);
            tasks_.at(task.id).status = TaskStatus::Failed;
            return task.id;
        }

        HttpActiveTask active;
        active.taskId = task.id;
        active.downloader = downloaderTask;
        active.url = url;
        active.lastReportSize = 0;

        // 打开文件（如果指定了 outputPath 且没有提供 outputStream）
        if (options.outputStream) {
            // nothing to open
        } else if (!options.outputPath.empty()) {
            try {
                // 以 append 模式打开（简化实现：假设 HTTP 数据按顺序到达）
                active.file = std::make_shared<std::ofstream>(options.outputPath, std::ios::binary | std::ios::app);
                if (!active.file->good()) {
                    active.file.reset();
                }
            } catch (...) {
                active.file.reset();
            }
        }

        {
            std::lock_guard<std::mutex> l(ctx->mtx);
            ctx->tasksByPtr[downloaderTask.get()] = active;
            ctx->downloaderByTaskId[task.id] = downloaderTask;
        }

        // 标记任务为 running
        {
            std::lock_guard<std::mutex> l(tasksMutex_);
            tasks_.at(task.id).status = TaskStatus::Running;
        }

        // 返回任务 ID
        return task.id;
    } else {
        // 非 HTTP_ONLY 的策略暂不实现（留空）
        std::lock_guard<std::mutex> l(tasksMutex_);
        tasks_.at(task.id).status = TaskStatus::Pending;
        return task.id;
    }
}

bool DownloadManager::cancelDownloadTask(const std::string& taskId) {
    // 先设置逻辑取消标记
    {
        std::lock_guard<std::mutex> l(tasksMutex_);
        auto it = tasks_.find(taskId);
        if (it != tasks_.end()) {
            it->second.cancelled = true;
            it->second.status = TaskStatus::Cancelled;
        } else {
            return false;
        }
    }

    // 如果是 HTTP task，调用 HttpDownloader::CancelTask
    HttpContext* ctx = getHttpContext(this);
    if (!ctx) return true;

    std::shared_ptr<dcdn::util::DownloaderTask> dt;
    {
        std::lock_guard<std::mutex> l(ctx->mtx);
        auto it = ctx->downloaderByTaskId.find(taskId);
        if (it != ctx->downloaderByTaskId.end()) dt = it->second;
    }
    if (dt) {
        httpDownloader_->CancelTask(dt);
    }
    return true;
}

bool DownloadManager::pauseDownloadTask(const std::string& taskId) {
    {
        std::lock_guard<std::mutex> l(tasksMutex_);
        auto it = tasks_.find(taskId);
        if (it != tasks_.end()) {
            it->second.paused = true;
            it->second.status = TaskStatus::Paused;
        } else {
            return false;
        }
    }
    HttpContext* ctx = getHttpContext(this);
    if (!ctx) return true;

    std::shared_ptr<dcdn::util::DownloaderTask> dt;
    {
        std::lock_guard<std::mutex> l(ctx->mtx);
        auto it = ctx->downloaderByTaskId.find(taskId);
        if (it != ctx->downloaderByTaskId.end()) dt = it->second;
    }
    if (dt) {
        httpDownloader_->PauseTask(dt);
    }
    return true;
}

bool DownloadManager::resumeDownloadTask(const std::string& taskId) {
    {
        std::lock_guard<std::mutex> l(tasksMutex_);
        auto it = tasks_.find(taskId);
        if (it != tasks_.end()) {
            it->second.paused = false;
            it->second.status = TaskStatus::Running;
        } else {
            return false;
        }
    }
    HttpContext* ctx = getHttpContext(this);
    if (!ctx) return true;
    std::shared_ptr<dcdn::util::DownloaderTask> dt;
    {
        std::lock_guard<std::mutex> l(ctx->mtx);
        auto it = ctx->downloaderByTaskId.find(taskId);
        if (it != ctx->downloaderByTaskId.end()) dt = it->second;
    }
    if (dt) {
        httpDownloader_->ResumeTask(dt);
    } else {
        // 若找不到对应 downloader（可能 restart 后），可以重新 Start 一个 task（TODO: 更完善的断点续传）
    }
    return true;
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
// 内部：进度计算与通知
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
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(t.lastUpdate - t.startTime).count();
    if (duration > 0) {
        t.speed = static_cast<double>(t.downloaded) / static_cast<double>(duration);
    } else {
        t.speed = 0;
    }
}

void DownloadManager::notifyBufferReady(const std::string& taskId, size_t start, size_t end) {
    std::lock_guard<std::mutex> l(tasksMutex_);
    auto it = bufferCallbacks_.find(taskId);
    if (it != bufferCallbacks_.end()) {
        // 调用回调（注意：回调执行可能会较慢，若需要更高性能可把回调放入线程池）
        auto cb = it->second;
        if (cb) cb(taskId, start, end);
    }
}

////////////////////////
// SubTask / P2P / 混合策略占位（留空）
////////////////////////
void DownloadManager::startHttpDownload(const std::string& taskId) {
    // 已不再使用单独的 startHttpDownload 函数 —— addDownloadTask 直接通过 HttpDownloader::AddTask 注册。
    // 保留此函数以兼容 header（但实现为空）。
    (void)taskId;
}

void DownloadManager::startP2pDownload(const std::string& taskId) {
    // TODO: 实现 P2P_ONLY
    (void)taskId;
}

void DownloadManager::startHybridDownload(const std::string& taskId) {
    // TODO: 实现 HYBRID 策略
    (void)taskId;
}

void DownloadManager::splitTask(const std::string& taskId) {
    // TODO: 分片逻辑（用于 P2P / HYBRID）
    (void)taskId;
}

void DownloadManager::onSubTaskCompleted(const std::string& taskId, size_t subtaskIndex) {
    // TODO
    (void)taskId; (void)subtaskIndex;
}

void DownloadManager::checkTaskCompletion(const std::string& taskId) {
    // TODO
    (void)taskId;
}

