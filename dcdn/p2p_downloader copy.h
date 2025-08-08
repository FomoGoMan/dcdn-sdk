/*
 *  Copyright (c) 2025 The XXX project authors. All Rights Reserved.
 *
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the LICENSE file in the root of the source
 *  tree. An additional intellectual property rights grant can be found
 *  in the file PATENTS.  All contributing project authors may
 *  be found in the AUTHORS file in the root of the source tree.
 */

#ifndef _DCDN_SDK_P2P_DOWNLOAD_MANAGER_H_
#define _DCDN_SDK_P2P_DOWNLOAD_MANAGER_H_

// #include <atomic>
// #include <cstddef>
// #include <functional>
// #include <memory>
// #include <mutex>
// #include <string>
// #include <unordered_map>

// #include "Cert.h"
// #include "dcdn/p2p_single_task.h"
// #include "p2p_downloader_interface.h"
// #include "util/Downloader.h"
// #include "rtc/peerconnection.hpp"
namespace dcdn {
namespace download {

// class P2PSingleTask;

// class P2PDownloader {
// public:
//   class Option : util::DownloaderOption{
//   public:
//     // time out that connect to peer when start download
//     std::string connection_timeout;
//     // max time that the peer connection is idle before closing
//     std::string max_peer_connection_idle_time;

//     CertificatePair certificate;
//   };

//   struct DownloadRequest {
//     std::string ContentHash;
//     uint64_t Start;
//     uint64_t End;
//   };

//   explicit P2PDownloader(const Option &option);
//   ~P2PDownloader();

//   // Start a download task and return a unique task ID
//   // non-zero TaskId on success
//   TaskId StartDownload(const std::string &peerId, const std::string &peerSdp,
//                        const DownloadRequest &request);

//   // Task Control
//   bool PauseTask(TaskId taskId);
//   bool ResumeTask(TaskId taskId);
//   bool CancelTask(TaskId taskId);

//   /// Task State Query
//   std::optional<P2PDownloadTaskInterface::State>
//   GetTaskState(TaskId taskId) const;

//   std::optional<P2PDownloadTaskInterface::Progress>
//   GetTaskProgress(TaskId taskId) const;

//   std::optional<size_t> GetDownloadSpeed(TaskId taskId) const;
//   std::optional<int> GetError(TaskId taskId) const;

//   // Set a callback to be triggered when the underlying peer connection for a
//   // task is successfully established
//   void
//   OnPeerConnectionEstablished(std::function<void(TaskId taskId)> &&callback);

//   void OnPeerData(std::function<void(TaskId taskId, void *data, size_t size,
//                                      size_t offset)> &&callback);

// private:
//   void OnDataChannelReceived(std::shared_ptr<rtc::DataChannel> dc,
//                              DownloadRequest request, TaskId taskId);
//   void InitPeerConnection(const std::string &peerId, const std::string &peerSdp,
//                           const DownloadRequest &request, TaskId taskId);

// private:
//   const Option opt_;
//   mutable std::mutex mutex_;

//   // peerId --> peerConnection
//   std::unordered_map<std::string, std::shared_ptr<rtc::PeerConnection>>
//       peerConnections_;
//   std::unordered_map<TaskId, std::shared_ptr<P2PSingleTask>> tasks_;

//   std::atomic<TaskId> next_task_id_ = 1;

//   std::function<void(TaskId)> connection_established_callback_;
//   std::function<void(TaskId, void *, size_t)> data_callback_;
// };

} // namespace download
} // namespace dcdn
#endif