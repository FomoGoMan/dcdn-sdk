#include "p2p_single_task.h"
#include "common/Logger.h"
#include "plog/Log.h"
#include "rtc/common.hpp"
#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
namespace dcdn {
namespace download {

// const std::map<int, std::string> P2PSingleTask::ERROR_CODES =
//     std::map<int, std::string>{{0, "Success"},
//                                {1, "Invalid argument"},
//                                {2, "File not found"},
//                                {3, "Permission denied"},
//                                {4, "Connection timeout"},
//                                {5, "Out of memory"},
//                                {6, "can not start stw"},
//                                {100, "Unknown error"}};

// P2PSingleTask::P2PSingleTask(TaskId taskId, TaskPram param,
//                              std::shared_ptr<rtc::DataChannel> dc)
//     : taskId_(taskId), dc_(dc), totalSize_(param.End - param.Start),
//       start(param.Start), end(param.End), contentHash_(param.ContentHash) {

//   state_ = State::IDLE;
//   buffer_.reserve(maxBufferSize_);
// }

// void P2PSingleTask::HandleIncomingData(
//     std::variant<std::vector<std::byte>, std::string> &&data) {
//   if (std::holds_alternative<std::vector<std::byte>>(data)) {
//     const auto &bytes = std::get<std::vector<std::byte>>(data);

//     buffer_.insert(buffer_.end(), bytes.begin(), bytes.end());
//     downloaded_ += bytes.size();

//     dataAvailableCv_.notify_one(); // notify ReadData can be called

//     // buffer full, tell peer to stop
//     double progress = (double)buffer_.size() / maxBufferSize_;
//     logDebug << "Received binary chunk. Progress: " << (progress * 100) << "%";
//     if (progress >= 0.95) {
//       std::string stop_msg = "STOP";
//       dc_->send(stop_msg);
//       logDebug << "Reached 95%, sent STOP receiving  signal, task : " << taskId_
//                << "content hash: " << contentHash_;
//       // state_ = State::PAUSEING_BY_BUFFER_FULL;
//     }
//   } else if (std::holds_alternative<std::string>(data)) {
//     const std::string &msg = std::get<std::string>(data);
//     if (msg == "PAUSE_ACK") {
//       state_ = State::PAUSED;
//       logDebug << "ACK Paused task: " << taskId_
//                << "content hash: " << contentHash_;
//     } else if (msg == "CANCEL_ACK") {
//       state_ = State::CANCELLED;
//       logDebug << "ACK Cancelled task: " << taskId_
//                << "content hash: " << contentHash_;
//     } else {
//       logWarn << "Received unhandled control message: " << msg;
//     }
//   }
// }

// P2PSingleTask::~P2PSingleTask() { Cancel(); }

// void P2PSingleTask::Start() {
//   if (state_ != State::IDLE) {
//     return;
//   }
//   dc_->onMessage(
//       [this](std::variant<std::vector<std::byte>, std::string> data) {
//         HandleIncomingData(std::move(data));
//       });
//   logDebug << "state form " << state_ << " to " << State::STARTED;
//   state_ = State::STARTED;
// }

// void P2PSingleTask::Pause() {
//   if (state_ == State::STARTED || state_ == State::DOWNLOADING) {
//     std::string stop_msg = "PAUSE";
//     dc_->send(stop_msg);
//     state_ = State::PAUSEING;
//   }
// }

// void P2PSingleTask::Resume() {
//   if (state_ == State::PAUSED) {
//     state_ = State::DOWNLOADING;
//   }
// }

// void P2PSingleTask::Cancel() {
//   state_ = State::CANCELLED;
//   // close all peer connections
//   {
//     std::lock_guard<std::mutex> lock(dataMutex_);
//     if (state_ != State::CANCELLED && dc_.get() != nullptr && dc_->isOpen()) {
//       dc_->close();
//     }
//   }
// }

// P2PDownloadTaskInterface::Progress *P2PSingleTask::GetProgress() const {
//   auto progress = new Progress();
//   progress->downloaded = downloaded_;
//   progress->total = totalSize_;
//   progress->percentage =
//       totalSize_ > 0 ? (downloaded_ * 100.0 / totalSize_) : 0;
//   return progress;
// }

// P2PDownloadTaskInterface::State P2PSingleTask::GetState() const {
//   return state_;
// }

// int P2PSingleTask::GetError() const { return lastError_; }

// size_t P2PSingleTask::ReadData(std::vector<std::byte> &data) {
//   {
//   std::unique_lock<std::mutex> lock(dataMutex_);
//     data.swap(buffer_);
//   }
//     assert(buffer_.empty());
//     auto curOffset = nextReadOffset_ ;
//     nextReadOffset_ += data.size();
//     return curOffset;
// }

} // namespace download
} // namespace dcdn
