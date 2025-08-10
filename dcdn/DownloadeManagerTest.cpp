#include "DownloadManager.h"
#include <iostream>
#include <thread>
#include <chrono>

int main() {
    dcdn::DownloadManager mgr;

    // 配置为 HTTP_ONLY 策略，最大并发 4
    mgr.setStrategy(dcdn::DownloadStrategy::HTTP_ONLY);
    mgr.setMaxConcurrentDownloads(4);

    // 输出到本地 test.bin，分片大小 1MB
    dcdn::FileDownloadOptions opts;
    opts.outputPath = "test.bin";
    opts.chunkSize = 100 *1024 * 1024;


    // 这里换成你想下载的 HTTP 文件 URL
    std::string url = "https://hil-speed.hetzner.com/1GB.bin";

    // 添加下载任务
    std::string taskId = mgr.addDownloadTask(url, "", opts);

    std::cout << "任务已创建，taskId = " << taskId << std::endl;

    // 每秒打印一次任务状态，直到完成/失败
    while (true) {
        auto task = mgr.getTaskStatus(taskId);

        double percent = 0.0;
        if (task.totalSize > 0) {
            percent = (100.0 * task.downloaded) / task.totalSize;
        }

        std::cout << "进度: " << percent << "% "
                  << "已下载: " << task.downloaded << "/" << task.totalSize << " bytes "
                  << "速度: " << task.speed / 1024.0 << " KB/s "
                  << "状态: " << static_cast<int>(task.status) << std::endl;

        if (task.status == dcdn::TaskStatus::Completed ||
            task.status == dcdn::TaskStatus::Failed ||
            task.status == dcdn::TaskStatus::Cancelled) {
            std::cout << "main: 任务结束" << std::endl;
            break;
        }

        if (percent > 0.1){
           auto res = mgr.cancelDownloadTask(taskId);
           if (res) std::cout << "main: 取消任务成功" << std::endl;
           else std::cout << "main: 取消任务失败" << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return 0;
}
