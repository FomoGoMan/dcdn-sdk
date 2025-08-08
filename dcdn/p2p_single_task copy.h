/*
 *  Copyright (c) 2025 The XXX project authors. All Rights Reserved.
 *
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the LICENSE file in the root of the source
 *  tree. An additional intellectual property rights grant can be found
 *  in the file PATENTS.  All contributing project authors may
 *  be found in the AUTHORS file in the root of the source tree.
 */

#ifndef _DCDN_SDK_P2P_SINGLE_TASK_H_
#define _DCDN_SDK_P2P_SINGLE_TASK_H_

#include "p2p_downloader_interface.h"
#include "rtc/datachannel.hpp"
#include <atomic>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace dcdn {

typedef std::string PeerId;

namespace download {

// struct DownloadRequest {
//   std::string Url;
//   std::string FileHash;
//   uint64_t Start;
//   uint64_t End;
// };

// TaskId static GenerateTaskId();

// // Represents a single P2P downloading task between a client (downloader)
// // and a peer (uploader). Each download request is associated with a unique task
// // ID. and its a minimum unit of downloading
// class P2PSingleTask : public P2PDownloadTaskInterface {
// public:
//   struct TaskPram {
//     std::string ContentHash;
//     uint64_t Start;
//     uint64_t End;
//   };
//   P2PSingleTask() = delete;
//   explicit P2PSingleTask(TaskId taskId, TaskPram param,
//                          std::shared_ptr<rtc::DataChannel> dc);
//   ~P2PSingleTask() override;

//   // return non-zero TaskId on start success
//   void Start() override;
//   void Pause() override;
//   void Resume() override;
//   void Cancel() override;

//   Progress *GetProgress() const override;
//   size_t GetDownloadSpeed() const override;
//   State GetState() const override;
//   int GetError() const override;

//   // return data offset and data itself
//   size_t ReadData(std::vector<std::byte> &data) override;

// private:
//   void
//   HandleIncomingData(std::variant<std::vector<std::byte>, std::string> &&data);
  
//   void UpdateDownloadSpeed();

// private:
//   size_t nextReadOffset_ = 0; // 当前读取位置

//   std::atomic<TaskId> taskId_;
//   std::shared_ptr<rtc::DataChannel> dc_;

//   size_t totalSize_ = 0;
//   size_t start = 0;
//   size_t end = 0;
//   const std::string contentHash_;

//   std::atomic<State> state_{State::IDLE};

//   enum PauseReason {NONE, PAUSE_BY_USER, PAUSE_BY_BUFFER_FULL };
//   std::atomic<PauseReason> lastPauseReason_{NONE};

//   std::mutex dataMutex_;
//   std::condition_variable dataAvailableCv_; // 数据到了，消费者来读
//   std::condition_variable bufferFreedCv_;   // 数据被读走，生产者继续写

//   // download speed estimator
//   std::atomic<size_t> downloaded_{0};

//   int lastError_ = 0;

//   std::vector<std::byte> buffer_;
//   size_t maxBufferSize_ = 10 * 1000 * 1000; // 举例，10M
// };

} // namespace download
} // namespace dcdn

#endif
