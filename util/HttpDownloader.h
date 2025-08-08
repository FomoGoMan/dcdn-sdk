#ifndef _DCDN_UTIL_HTTP_DOWNLOADER_H_
#define _DCDN_UTIL_HTTP_DOWNLOADER_H_

#include "HttpClient.h"
#include <any>
#include <curl/curl.h>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

NS_BEGIN(dcdn)
NS_BEGIN(util)

class HttpDownloaderTask;
class HttpDownloader;
struct HttpDownloaderTaskOption {
  std::string Url;
  HttpHeaders Headers;
  size_t Start = 0;
  size_t End = 0;
  size_t MaxBuf = 0;

  void (*Notify)(std::shared_ptr<HttpDownloaderTask> task,
                 void *recevier) = nullptr;
  void *Receiver = nullptr;
};

class HttpDownloaderTask {
public:
  enum StatusType {
    Cancelled = -2,
    Fail = -1,
    Idle = 0,
    Running,
    Paused,
    Completed,
  };

public:
  HttpDownloaderTask(CURL *curl, const HttpDownloaderTaskOption &opt)
      : mCurl(curl), mOpt(opt) {}
  ~HttpDownloaderTask() {
    if (mCurl) {
      curl_easy_cleanup(mCurl);
    }
  }
  StatusType Status() const {
    std::unique_lock<std::mutex> lck(mMtx);
    return mStatus;
  }
  const HttpDownloaderTaskOption &Option() const { return mOpt; }
  bool IsEnd() const {
    auto st = Status();
    return st < Idle || st >= Completed;
  }
  const std::string &ContentType() const {
    std::unique_lock<std::mutex> lck(mMtx);
    return mContentType;
  }
  size_t ContentLength() const {
    std::unique_lock<std::mutex> lck(mMtx);
    return mContentLength;
  }
  size_t Size() const {
    std::unique_lock<std::mutex> lck(mMtx);
    return mSize;
  }
  size_t Read(std::vector<unsigned char> &data) {
    std::unique_lock<std::mutex> lck(mMtx);
    size_t offset = mOffset;
    mOffset += mData.size();
    data.swap(mData);
    mData.resize(0);
    return offset;
  }

private:
  void notify(std::shared_ptr<HttpDownloaderTask> t) {
    if (mOpt.Notify) {
      mOpt.Notify(t, mOpt.Receiver);
    }
  }
  void setStatus(StatusType st) {
    std::unique_lock<std::mutex> lck(mMtx);
    mStatus = st;
  }
  bool cancel() {
    std::unique_lock<std::mutex> lck(mMtx);
    if (mStatus == Idle || mStatus == Running) {
      mStatus = Cancelled;
      return true;
    }
    return false;
  }
  bool pause() {
    std::unique_lock<std::mutex> lck(mMtx);
    if (mStatus == Idle || mStatus == Running) {
      mStatus = Paused;
      return true;
    }
    return false;
  }
  bool resume() {
    std::unique_lock<std::mutex> lck(mMtx);
    if (mStatus == Paused) {
      mStatus = Running;
      return true;
    }
    return false;
  }
  void setContent(const std::string &tp, size_t length) {
    std::unique_lock<std::mutex> lck(mMtx);
    mContentType = tp;
    mContentLength = length;
  }
  bool outOfBuf() const {
    std::unique_lock<std::mutex> lck(mMtx);
    return mOpt.MaxBuf > 0 && mData.size() > mOpt.MaxBuf;
  }
  void write(const unsigned char *dat, size_t len) {
    std::unique_lock<std::mutex> lck(mMtx);
    mData.insert(mData.end(), dat, dat + len);
    mSize += len;
  }

private:
  friend class HttpDownloader;
  StatusType mStatus = Idle;
  mutable std::mutex mMtx;
  CURL *mCurl = nullptr;
  bool mCurlAdded = false;
  HttpDownloaderTaskOption mOpt;
  HttpHeaders::CurlHeaders mCurlHeaders;
  size_t mOffset = 0;
  std::vector<unsigned char> mData;
  size_t mSize = 0;
  size_t mContentLength = 0;
  std::string mContentType;
};

class HttpDownloader : public HttpClientOption {
public:
  HttpDownloader() {}
  ~HttpDownloader() {
    if (mCM) {
      curl_multi_cleanup(mCM);
      mCM = nullptr;
    }
  }
  HttpDownloader(const HttpDownloader &oth) = delete;
  HttpDownloader(HttpDownloader &&oth) = delete;
  HttpDownloader &operator=(const HttpDownloader &oth) = delete;
  HttpDownloader &operator=(HttpDownloader &&oth) = delete;
  int Init() {
    mCM = curl_multi_init();
    if (!mCM) {
      return ErrorCodeErr;
    }
    return ErrorCodeOk;
  }
  void Start(bool detach = true) {
    mThread = std::make_shared<std::thread>([&]() { run(); });
    if (detach) {
      mThread->detach();
    }
  }
  std::shared_ptr<std::thread> Thread() { return mThread; }
  std::shared_ptr<HttpDownloaderTask>
  AddTask(const HttpDownloaderTaskOption &opt) {
    logDebug << "AddTask url:" << opt.Url;
    auto c = curl_easy_init();
    if (!c) {
      return nullptr;
    }
    auto t = std::make_shared<HttpDownloaderTask>(c, opt);
    curl_easy_setopt(c, CURLOPT_URL, opt.Url.c_str());
    fill(c);
    if (!t->Option().Headers.Empty()) {
      t->mCurlHeaders = t->Option().Headers.Curl();
      curl_easy_setopt(c, CURLOPT_HTTPHEADER, (curl_slist *)(t->mCurlHeaders));
    }
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, t.get());
    if (opt.Start > 0) {
      std::string range = std::to_string(opt.Start) + "-";
      if (opt.End >= opt.Start) {
        range += std::to_string(opt.End);
        curl_easy_setopt(c, CURLOPT_RANGE, range.c_str());
      }
    }

    postEvent(EventType::AddTask, t);
    return t;
  }
  void CancelTask(std::shared_ptr<HttpDownloaderTask> task) {
    postEvent(EventType::CancelTask, task);
  }
  void PauseTask(std::shared_ptr<HttpDownloaderTask> task) {
    postEvent(EventType::PauseTask, task);
  }
  void ResumeTask(std::shared_ptr<HttpDownloaderTask> task) {
    postEvent(EventType::ResumeTask, task);
  }

private:
  enum class EventType {
    AddTask,
    CancelTask,
    PauseTask,
    ResumeTask,
  };
  typedef std::pair<EventType, std::any> Event;
  template <class T> void postEvent(EventType tp, const T &arg) {
    std::unique_lock<std::mutex> lck(mMtx);
    auto e = std::make_shared<Event>(tp, arg);
    mEvts.push_back(e);
    curl_multi_wakeup(mCM);
  }

  void run() {
    std::vector<HttpDownloaderTask *> rmTasks;
    std::list<std::shared_ptr<Event>> evts;
    while (true) {
      int num = 0;
      int mc = curl_multi_perform(mCM, &num);
      logVerb << "curl_multi_perform:" << mc << " num:" << num;
      if (mc != CURLM_OK) {
        logWarn << "unexpected curl_multi_perform error";
        abortAll();
        continue;
      }

      CURLMsg *msg;
      int msgs_left = 0;
      while ((msg = curl_multi_info_read(mCM, &msgs_left))) {
        if (msg->msg == CURLMSG_DONE) {
          CURL *c = msg->easy_handle;
          CURLcode res = msg->data.result;
          auto t = mCurlTasks[c];
          if (res == CURLE_OK) {
            t->setStatus(HttpDownloaderTask::Completed);
            logDebug << "task url:" << t->mOpt.Url << " completed";
          } else {
            t->setStatus(HttpDownloaderTask::Fail);
          }
        }
      }

      for (auto it : mTasks) {
        auto t = it.first;
        bool notify = false;
        if (!t->mData.empty() || t->IsEnd()) {
          notify = true;
        }
        if (t->outOfBuf()) {
          if (t->mCurlAdded) {
            curl_multi_remove_handle(mCM, t->mCurl);
            t->mCurlAdded = false;
          }
        } else if (!t->mCurlAdded) {
          int ret = curl_multi_add_handle(mCM, t->mCurl);
          if (ret != CURLM_OK) {
            t->mCurlAdded = false;
            t->setStatus(HttpDownloaderTask::Fail);
            notify = true;
          }
        }
        if (notify) {
          t->notify(it.second);
        }
        if (t->IsEnd()) {
          rmTasks.push_back(t);
        }
      }
      if (!rmTasks.empty()) {
        for (auto t : rmTasks) {
          removeTask(t);
        }
        rmTasks.clear();
      }

      mc = curl_multi_poll(mCM, NULL, 0, 10000, &num);
      if (mc != CURLM_OK) {
        logWarn << "unexpected curl_multi_poll error";
        abortAll();
        continue;
      }

      do {
        std::unique_lock<std::mutex> lck(mMtx);
        evts.swap(mEvts);
      } while (0);
      while (!evts.empty()) {
        auto evt = evts.front();
        evts.pop_front();
        handleEvent(evt);
      }
    }
  }
  void abortAll() {
    while (!mTasks.empty()) {
      auto it = mTasks.begin();
      auto t = it->first;
      t->setStatus(HttpDownloaderTask::Fail);
      t->notify(it->second);
      removeTask(t);
    }
  }
  void removeTask(HttpDownloaderTask *t) {
    if (t->mCurlAdded) {
      curl_multi_remove_handle(mCM, t->mCurl);
      t->mCurlAdded = false;
    }
    mCurlTasks.erase(t->mCurl);
    mTasks.erase(t);
  }
  void handleEvent(std::shared_ptr<Event> evt) {
    switch (evt->first) {
    case EventType::AddTask:
      handleAddTaskEvent(evt);
      break;
    case EventType::CancelTask:
      handleCancelTaskEvent(evt);
      break;
    case EventType::PauseTask:
      handlePauseTaskEvent(evt);
      break;
    default:
      break;
    }
  }
  void handleAddTaskEvent(std::shared_ptr<Event> evt) {
    auto t = std::any_cast<std::shared_ptr<HttpDownloaderTask>>(evt->second);
    logDebug << "AddTaskEvent url:" << t->mOpt.Url;
    mTasks[t.get()] = t;
    mCurlTasks[t->mCurl] = t;
    int ret = curl_multi_add_handle(mCM, t->mCurl);
    if (ret != CURLM_OK) {
      t->mCurlAdded = false;
      t->setStatus(HttpDownloaderTask::Fail);
      t->notify(t);
      return;
    }
    t->mCurlAdded = true;
    t->setStatus(HttpDownloaderTask::Running);
  }
  void handleCancelTaskEvent(std::shared_ptr<Event> evt) {
    auto t = std::any_cast<std::shared_ptr<HttpDownloaderTask>>(evt->second);
    auto it = mTasks.find(t.get());
    if (it != mTasks.end()) {
      if (t->cancel()) {
        t->notify(it->second);
      }
      mTasks.erase(it);
    }
  }
  void handlePauseTaskEvent(std::shared_ptr<Event> evt) {
    auto t = std::any_cast<std::shared_ptr<HttpDownloaderTask>>(evt->second);
    auto it = mTasks.find(t.get());
    if (it != mTasks.end()) {
      if (t->pause()) {
        if (t->mCurlAdded) {
          curl_multi_remove_handle(mCM, t->mCurl);
          t->mCurlAdded = false;
        }
        t->notify(it->second);
      }
    }
  }
  void handleResumeTaskEvent(std::shared_ptr<Event> evt) {
    auto t = std::any_cast<std::shared_ptr<HttpDownloaderTask>>(evt->second);
    auto it = mTasks.find(t.get());
    if (it != mTasks.end()) {
      if (t->resume()) {
        int ret = curl_multi_add_handle(mCM, t->mCurl);
        if (ret != CURLM_OK) {
          t->mCurlAdded = false;
          t->setStatus(HttpDownloaderTask::Fail);
          t->notify(t);
          return;
        }
        t->mCurlAdded = true;
      }
    }
  }

  static size_t writeCallback(void *contents, size_t size, size_t nmemb,
                              void *data) {
    auto t = static_cast<HttpDownloaderTask *>(data);
    if (t->mSize == 0) {
      char *ctype = nullptr;
      if (curl_easy_getinfo(t->mCurl, CURLINFO_CONTENT_TYPE, &ctype) !=
          CURLE_OK) {
        ctype = nullptr;
      }
      curl_off_t clen = 0;
      if (curl_easy_getinfo(t->mCurl, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T,
                            &clen) != CURLE_OK) {
        clen = 0;
      }
      if (ctype || clen > 0) {
        t->setContent((ctype ? ctype : ""), (clen > 0 ? clen : 0));
      }
    }
    const unsigned char *p = (const unsigned char *)contents;
    size *= nmemb;
    t->write(p, size);
    logVerb << "task url:" << t->mOpt.Url << " write data:" << size;
    return size;
  }

private:
  std::shared_ptr<std::thread> mThread;
  std::mutex mMtx;
  std::list<std::shared_ptr<Event>> mEvts;

  CURLM *mCM;
  std::unordered_map<HttpDownloaderTask *, std::shared_ptr<HttpDownloaderTask>>
      mTasks;
  std::unordered_map<CURL *, std::shared_ptr<HttpDownloaderTask>> mCurlTasks;
};

NS_END
NS_END

#endif
