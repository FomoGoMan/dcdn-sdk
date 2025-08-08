#include "DownloadManager.h"
#include <plog/Appenders/ConsoleAppender.h>
#include <plog/Initializers/RollingFileInitializer.h>
#include <plog/Log.h>

#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

int main(int argc, char* argv[])
{
    static plog::ConsoleAppender<plog::TxtFormatter> consoleAppender;
    plog::Severity logLevel = plog::info;

    std::string persistPath = "./downloads";
    std::vector<std::string> urls;

    // 解析命令行参数
    for (int i = 1; i < argc; ++i)
    {
        if (strcmp(argv[i], "-v") == 0)
        {
            logLevel = plog::verbose;
        }
        else if (strcmp(argv[i], "--path") == 0)
        {
            if (++i < argc) persistPath = argv[i];
        }
        else
        {
            urls.push_back(argv[i]);
        }
    }

    if (urls.empty())
    {
        std::cout << "Usage: " << argv[0] << " [-v] [--path <dir>] <url>..." << std::endl;
        return 1;
    }

    plog::init<DCDN_LOGGER_ID>(logLevel, &consoleAppender);

    // 创建下载管理器
    dcdn::DownloadManager manager;
    manager.setStrategy(dcdn::DownloadStrategy::HTTP_ONLY);
    manager.setMaxConcurrentDownloads(2);
    manager.setPersistPath(persistPath);

    // 添加任务
    std::vector<std::string> taskIds;
    for (const auto& url : urls)
    {
        std::string taskId = manager.addDownloadTask(url);
        taskIds.push_back(taskId);
        logInfo << "Added download task: " << taskId << " for URL: " << url;
    }

    // 轮询任务状态并打印进度
    bool allCompleted = false;
    auto lastReport = std::chrono::steady_clock::now();

    while (!allCompleted)
    {
        allCompleted = true;
        auto now = std::chrono::steady_clock::now();
        if (now - lastReport >= std::chrono::seconds(1))
        {
            system("clear"); // 或者用 std::cout << "\033[2J\033[H"; 来清屏
            double overallSpeed = manager.getOverallSpeed();
            std::cout << "Overall Speed: " << overallSpeed << " Bytes/s\n";
            std::cout << "----------------------------------------\n";

            for (const auto& id : taskIds)
            {
                dcdn::DownloadTask task = manager.getTaskStatus(id);
                double progress = 0.0;
                if (task.totalSize> 0)
                    progress = (task.downloaded* 100.0) / task.totalSize;

                std::cout << "Task: " << id
                          << " | " << progress << "% "
                          << "(" << task.downloaded<< "/" << task.totalSize<< " bytes)"
                          << " | Speed: " << task.speed << " B/s"
                          << " | Status: " << static_cast<int>(task.status)
                          << "\n";

                if (task.status != dcdn::TaskStatus::Completed&&
                    task.status != dcdn::TaskStatus::Failed&&
                    task.status != dcdn::TaskStatus::Cancelled)
                {
                    allCompleted = false;
                }
            }

            lastReport = now;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    logInfo << "All downloads finished.";
    return 0;
}
