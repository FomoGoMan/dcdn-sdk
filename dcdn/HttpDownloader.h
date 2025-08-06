#ifndef _DCDN_CDN_HTTP_DOWNLODER_H_
#define _DCDN_CDN_HTTP_DOWNLODER_H_

#include "BaseManager.h"
#include "EventLoop.h"

namespace dcdn{

class HttpDownloader: public BaseManager, public EventLoop<HttpDownloader>
{
public:
    HttpDownloader(MainManager* man);
private:
    void run();
};

} 

#endif
