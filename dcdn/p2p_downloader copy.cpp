// #include "dcdn/p2p_downloader.h"
// #include "common/Logger.h"
// #include "dcdn/Cert.h"
// #include "dcdn/p2p_downloader_interface.h"
// #include "dcdn/p2p_single_task.h"
// #include "rtc/datachannel.hpp"
// #include "rtc/peerconnection.hpp"
// #include "rtc/rtc.hpp"
// #include <iostream>

// namespace dcdn {
// namespace download {

// P2PDownloader::P2PDownloader(const Option &option) : opt_(option) {
//   // TODO: implement
// }

// TaskId P2PDownloader::StartDownload(const std::string &peerId,
//                                          const std::string &peerSdp,
//                                          const DownloadRequest &request) {

//   auto taskId = next_task_id_++;
//   // TODO: make this  async
//   std::shared_ptr<rtc::PeerConnection> peerConn;
//   {
//     std::lock_guard<std::mutex> lock(mutex_);
//     // Get or create peer session
//     auto connectionIt = peerConnections_.find(peerId);

//     // create peer session
//     if (connectionIt == peerConnections_.end()) {
//       peerConn = std::make_shared<rtc::PeerConnection>(peerId);
//       peerConnections_[peerId] = peerConn;
//       InitPeerConnection(peerId, peerSdp, request, taskId);
//     } else {
//       peerConn = connectionIt->second;
//     }
//   }

//   return taskId;
// }

// void P2PDownloader::InitPeerConnection(const std::string &peerID,
//                                             const std::string &peerSdp,
//                                             const DownloadRequest &request,
//                                             TaskId taskId) {
//   std::lock_guard<std::mutex> lock(mutex_);
//   auto it = peerConnections_.find(peerID);
//   if (it == peerConnections_.end())
//     return;

//   // create peer connection
//   // TODO: parse from sdp
//   rtc::Configuration config;
//   config.iceUfrag = "p0hI";
//   config.icePwd = "aKESPyeQ51OID4NQFypOIIuJ";

//   auto cp = opt_;
//   std::cout << "cert:\n" << cp.certificate.certPem << std::endl;
//   std::cout << "key:\n" << cp.certificate.keyPem << std::endl;

//   // rtc::IceServer serv("39.106.141.70", 8347);
//   rtc::IceServer serv("47.236.146.120", 3478);
//   serv.type = rtc::IceServer::Type::Stun;
//   config.iceServers.push_back(serv);
//   config.enableIceUdpMux = true;
//   config.certificatePemFile = cp.certificate.certPem;
//   config.keyPemFile = cp.certificate.keyPem;

//   auto pc_ = std::make_unique<rtc::PeerConnection>(config);
//   pc_->onDataChannel(
//       [this, request = request, taskId](std::shared_ptr<rtc::DataChannel> dc) {
//         OnDataChannelReceived(dc, request, taskId);
//       });
// }

// void P2PDownloader::OnDataChannelReceived(
//     std::shared_ptr<rtc::DataChannel> dc, DownloadRequest request,
//     TaskId taskId) {
//   auto taskParms =
//       P2PSingleTask::TaskPram{request.ContentHash, request.Start, request.End};
//   auto task = std::make_shared<P2PSingleTask>(taskId, taskParms, dc);
//   task->Start();

//   {
//     std::lock_guard<std::mutex> lock(mutex_);
//     tasks_[taskId] = task;
//   }
//   logDebug << "manager registered task" << taskId;
//   // call user registered callback
//   connection_established_callback_(taskId);
// };

// bool P2PDownloader::PauseTask(TaskId taskId) {
//   std::lock_guard<std::mutex> lock(mutex_);
//   auto it = tasks_.find(taskId);

//   if (it == tasks_.end())
//     return false;
//   it->second->Pause();
//   return true;
// }

// bool P2PDownloader::ResumeTask(TaskId taskId) {
//   std::lock_guard<std::mutex> lock(mutex_);
//   auto it = tasks_.find(taskId);
//   if (it == tasks_.end())
//     return false;
//   it->second->Resume();
//   return true;
// }

// bool P2PDownloader::CancelTask(TaskId taskId) {
//   std::lock_guard<std::mutex> lock(mutex_);
//   auto it = tasks_.find(taskId);
//   if (it == tasks_.end())
//     return false;
//   it->second->Cancel();
//   return true;
// }

// std::optional<P2PDownloadTaskInterface::State>
// P2PDownloader::GetTaskState(TaskId taskId) const {
//   std::lock_guard<std::mutex> lock(mutex_);
//   auto it = tasks_.find(taskId);
//   if (it == tasks_.end())
//     return std::nullopt;
//   return it->second->GetState();
// }

// std::optional<P2PDownloadTaskInterface::Progress>
// P2PDownloader::GetTaskProgress(TaskId taskId) const {
//   std::lock_guard<std::mutex> lock(mutex_);
//   auto it = tasks_.find(taskId);
//   if (it == tasks_.end())
//     return std::nullopt;
//   return *it->second->GetProgress();
// }

// std::optional<size_t>
// P2PDownloader::GetDownloadSpeed(TaskId taskId) const {
//   std::lock_guard<std::mutex> lock(mutex_);
//   auto it = tasks_.find(taskId);
//   if (it == tasks_.end())
//     return std::nullopt;
//   return it->second->GetDownloadSpeed();
// }

// std::optional<int> P2PDownloader::GetError(TaskId taskId) const {
//   std::lock_guard<std::mutex> lock(mutex_);
//   auto it = tasks_.find(taskId);
//   if (it == tasks_.end())
//     return std::nullopt;
//   return it->second->GetError();
// }

// void P2PDownloader::OnPeerConnectionEstablished(
//     std::function<void(TaskId)> &&callback) {
//   std::lock_guard<std::mutex> lock(mutex_);
//   connection_established_callback_ = std::move(callback);
// }

// } // namespace download
// } // namespace dcdn
