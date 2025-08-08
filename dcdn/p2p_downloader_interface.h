
/*
 *  Copyright (c) 2025 The XXX project authors. All Rights Reserved.
 *
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the LICENSE file in the root of the source
 *  tree. An additional intellectual property rights grant can be found
 *  in the file PATENTS.  All contributing project authors may
 *  be found in the AUTHORS file in the root of the source tree.
 */

#ifndef _DCDN_SDK_DOWNLOAD_TASK_INTERFACE_H_
#define _DCDN_SDK_DOWNLOAD_TASK_INTERFACE_H_

// #include "rtc/datachannel.hpp"
// #include <cstddef>
// #include <string>

namespace dcdn {
namespace download {

// typedef unsigned long long TaskId;

// using rtc::DataChannel;
// // This interface defines the functions that a peer to peer  
// // downloader needs to implement. 
// class P2PDownloadTaskInterface {
// public:
//   struct DownloadParam {
//     std::string ContentHash;
//     uint64_t Start;
//     uint64_t End;
//   };

//   // TODO: in-appropriate method here
//  virtual void SetTaskTransport(std::shared_ptr<DataChannel> dc);

//   virtual void  Start() = 0;

//   // Pause the download
//   virtual void Pause() = 0;

//   // Resume the download
//   virtual void Resume() = 0;

//   // Cancel the download
//   virtual void Cancel() = 0;

//   struct Progress {
//     double percentage; // percentage progress
//     size_t downloaded; // bytes downloaded so far
//     size_t total;      // total bytes to be downloaded (may be 0 if unknown)
//   };
//   // the receiver is responsible for deleting the returned object
//   virtual Progress *GetProgress() const = 0;

//   // Get the download speed (bytes/sec)
//   virtual size_t GetDownloadSpeed() const = 0;

//   // Get the task state
//   enum class State {IDLE, STARTED, DOWNLOADING, COMPLETED, PAUSEING,PAUSED, CANCELLING, CANCELLED};
//   virtual State GetState() const = 0;

//   // Get the last error code (if any)
//   // of course The GetState() should return ERROR if 
//   // GetError() returns non-zero value
//   // TODO: (FomoGoMan) return error code, non-zero means error
//   virtual int GetError() const = 0;

//   // seek control
//   // jump to specified position
//   virtual size_t ReadData(std::vector<std::byte> &data);

//   virtual ~P2PDownloadTaskInterface() = default;
// };


} // namespace download
} // namespace dcdn
#endif