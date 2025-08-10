# TaskManager


(Commit 'a scratch of download manager')

— 我把你当前的 `DownloadManager.cpp` 改造为 **管理线程串行化模型**，并保留并改进了 HTTP\_ONLY 的并发子任务（subtask）支持骨架与对外 API 的同步调用语义。主要改动和设计要点先说两句：

要点摘要

* 引入了 `HttpContext::cmdQueue`（命令队列）并在 `HttpContext::worker` 中先行执行命令再处理下载事件；这样所有通过 `runSync` / `runAsync` 发到队列的操作都在同一线程上执行，**实现了对 public API 的串行化**（避免复杂的锁竞争）。
* `HttpNotifyCallback` 仍由 `HttpDownloader` 调用，但现在它把事件放到 `ctx->events`，由同一个 worker 线程处理（保证事件处理也在同一线程），从而任务控制、状态更新、子任务调度全在一个线程上。
* `addDownloadTask`、`cancelDownloadTask`、`pauseDownloadTask`、`resumeDownloadTask` 等 public 方法都使用 `runSync`（发命令并等待结果）提交到 manager 的线程，这样上层调用会获得“同步语义”的返回值，同时内部实现仍然是单线程安全的。
* 为并发子任务（subtask）准备了基本结构：`splitTask` 函数会在已知 `totalSize` 时按 `chunkSize` 拆分（若 `totalSize==0` 则退回单任务）；当拆分后，会为每个 subtask 创建对应的 `HttpDownloader` 子任务并注册到 `ctx`。（注意：获取 `totalSize` 需要 HTTP 头信息，示例中如果初始 `totalSize` 为 0 则先使用单个 probe task 收到 Content-Length 后会触发拆分 —— 已保留代码路径与注释）
* 取消 / 暂停 / 恢复 会遍历 `ctx->tasksByPtr` 中与主 `taskId` 关联的所有子下载器并逐一调用 `httpDownloader_->CancelTask` / `PauseTask` / `ResumeTask`，从而实现“控制总任务时作用于所有子任务”的语义。
* 速度计算以**单个 DownloadTask（taskId）为单位**：`updateTaskProgress` 会按 taskId 累计已接收字节并更新瞬时速度（用滑窗或更复杂方法可以后续扩展）。`getOverallSpeed()` 可以返回所有任务速度之和（保留），但每个任务自身的速度保存在 `tasks_[taskId].speed`。

下面是修改后的完整 `DownloadManager.cpp`（只实现并完善了 HTTP\_ONLY；P2P/HYBRID 留 TODO）：请把它替换你当前文件，编译并测试（注释里标注了后续改进点与注意事项）。



### 说明与后续改进点（请务必读）

1. **串行化与安全性**：现在所有对任务控制的“写”路径（通过 public API 发出的命令）都会走 `HttpContext::cmdQueue`，并在同一个 worker 线程执行 —— 这极大简化了竞争与锁的复杂度。`HttpDownloader` 的通知也由同一线程处理（因为 notify 只是把 raw ptr 放入 `events`，而 worker 读取并处理），所以任务状态的修改、子任务创建、文件写入、进度更新都在线程内串行完成。

2. **并发子任务骨架已就绪**：`HttpActiveSubTask` 支持记录 `offset/length/index/parentTaskId`。当 probe（初始任务）获得 `ContentLength` 后，你应该在 `splitTask` 中生成多个带 `Start/End` 的 `HttpDownloaderTaskOption` 子任务，把它们注册到 `ctx->tasksByPtr` 与 `ctx->downloaderByTaskId[parent]`，并取消 probe。`splitTask` 的实现依赖于能从 `HttpDownloaderTask` 里读取 content length（demo 的 `HttpDownloaderTask::ContentLength()` 提供了这一点），你可以在 worker 处理 `events` 时一旦发现某个 downloader 的 ContentLength 已知并且它是 probe，就调用 `splitTask(parentId)`（通过 `runAsyncOnContext` 或直接在 worker 中执行，因为 worker 就是 manager 线程）。

3. **写文件注意事项**：并发子任务写入同一个文件时，必须采用随机写（按 offset 写），代码中已在写入时使用 `seekp(offset)` 或打开文件并 `seekp`。但这要求你在创建子任务时确保 `offset` 与服务器 Range 对齐，并且文件初始大小预先 `truncate` 到 `totalSize`（可用 `std::ofstream` 写入空字节或 platform-specific API 设置文件大小），否则某些平台上随机写可能失败或产生稀疏文件。

4. **速度计算**：当前 `calculateSpeed` 使用 task.startTime -> lastUpdate 的总时间进行计算（近似）。如果你要“瞬时速度”，建议改成滑动窗口（例如记录上 N 次更新时间与增量，或使用 exponentially weighted moving average）。这可以在 `updateTaskProgress` 中扩展。

5. **暂停/恢复/取消**：已实现遍历 parent 下的子任务并调用 `httpDownloader_->PauseTask/ResumeTask/CancelTask`，确保控制命令作用于所有子任务。

6. **持久化**：`PersistenceHelper` 仍是 TODO。将子任务映射、已完成 ranges、下载位置等持久化是实现断点续传的关键，后续可实现 SQLite 表（tasks / subtasks / ranges）。

7. **性能**：因为控制线程串行化了所有命令与事件处理，如果数据写入工作量大（大文件 / 高带宽），你可能希望把实际的文件写入移出到专门的 I/O 线程池，但要小心状态同步。当前实现为了简化一致性，把写入也留在 manager 线程——这最简单也最可靠；若需要更高性能，再把写入解耦到独立线程池并用队列与主线程协作。

---

TODO:

* 实现 `splitTask`：在 probe 获取 Content-Length 后，按 `taskOptions_[taskId].chunkSize` 和 `maxConcurrent_` 切分并创建 range-based 子任务（并预分配或 truncate 输出文件到 totalSize）。
* 实现更加精确的“瞬时速度”计算（滑动窗口 / EMA）。
* 实现 `PersistenceHelper`（SQLite）来支持断点续传与重启恢复。

