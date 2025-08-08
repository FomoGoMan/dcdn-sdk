#ifndef _DCDN_SDK_DOWNLOAD_MANAGER_H_
#define _DCDN_SDK_DOWNLOAD_MANAGER_H_

#include "BaseManager.h"
#include "EventLoop.h"

namespace dcdn {

class DownloadManager : public BaseManager, public EventLoop<DownloadManager> {

public:
  explicit DownloadManager(MainManager *man);

private:
  void run();
  void handleDeployMsgEvent(std::shared_ptr<Event> evt);
};

} // namespace dcdn

#endif