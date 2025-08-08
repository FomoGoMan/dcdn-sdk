#include "common/Logger.h"
#include "dcdn/Cert.h"
#include "dcdn/p2p_downloader.h"
#include "dcdn/p2p_downloader_interface.h"
#include "dcdn/p2p_single_task.h"
#include "plog/Formatters/TxtFormatter.h"
#include "plog/Init.h"
#include "plog/Severity.h"
#include "util/Downloader.h"
#include <condition_variable>
#include <fstream>
#include <memory>
#include <mutex>
#include <plog/Appenders/ConsoleAppender.h>

using namespace dcdn::download;

int main(int argc, char *argv[]) {
  static plog::ConsoleAppender<plog::TxtFormatter> consoleAppender;
  plog::Severity lvl = plog::debug;
  plog::init<DCDN_LOGGER_ID>(lvl, &consoleAppender);
  struct signal {
    std::mutex mutex;
    std::unique_lock<std::mutex> mtx;
    std::condition_variable cv;

    signal() : mtx(mutex, std::defer_lock) {}
  } sig;

  // create downloader
  // auto cert = generate_ecdsa_certificate();
  CertificatePair cert;
  cert.certPem =
      "-----BEGIN CERTIFICATE-----\n"
      "MIIBHTCBxAIBATAKBggqhkjOPQQDAjAbMRkwFwYDVQQDDBB3ZWJydGMuZ2VuZXJh\n"
      "dGVkMB4XDTI1MDgwODAzMzAwNFoXDTI2MDgwODAzMzAwNFowGzEZMBcGA1UEAwwQ\n"
      "d2VicnRjLmdlbmVyYXRlZDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABCkMxEBU\n"
      "HmuBweNBXu0bIDz1MpIJNWyb9isQYfEerxom1SOpbHVj1jOtbH6vFOv7g+DV5EuD\n"
      "oZGeVt2YBsLw78wwCgYIKoZIzj0EAwIDSAAwRQIgLtaqVF9ulJFm1yANR4zmpsOL\n"
      "gMtwOSIgl8NirmGr77sCIQDf6I/3MUUDGJ8rTQUAjF/s3eZ2gRR7HvezQujpD0sE\n"
      "Hg==\n"
      "-----END CERTIFICATE-----\n";

  cert.keyPem =
      "-----BEGIN PRIVATE KEY-----\n"
      "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgZRSNN31PSJlzxmWT\n"
      "S2svbsSepZB9R5Ukfj8Z3GXs30ahRANCAAQpDMRAVB5rgcHjQV7tGyA89TKSCTVs\n"
      "m/YrEGHxHq8aJtUjqWx1Y9YzrWx+rxTr+4Pg1eRLg6GRnlbdmAbC8O/M\n"
      "-----END PRIVATE KEY-----\n";

  auto downloaderOpt = dcdn::download::P2PDownloader::Option();
  downloaderOpt.certificate = cert;
  auto downloader =
      std::make_shared<dcdn::download::P2PDownloader>(downloaderOpt);

  // configure task
  dcdn::download::P2PDownloaderTaskOption taskOpt{};
  taskOpt.Start = 0;
  taskOpt.End = 100;
  taskOpt.MaxBuf = 10240000; // 10M
  taskOpt.PeerId = "peerID1";
  taskOpt.PeerSdp = "sdp";
  taskOpt.Receiver = &sig;
  taskOpt.Notify = [](std::shared_ptr<dcdn::util::DownloaderTask> task,
                      void *p) {
    auto sig = static_cast<signal *>(p);
    sig->mtx.lock();
    sig->cv.notify_one();
    sig->mtx.unlock();
  };
  logInfo << "123";

  auto task = downloader->AddTask(&taskOpt);

  // save task downloaded content
  std::fstream file("tmp", std::ios::out | std::ios::binary);
  while (!task->IsEnd()) {
    sig.mtx.lock();
    sig.cv.wait(sig.mtx, [&] { return task->HasData(); });
    auto data = task->Read();
    file.seekg(data->Offset());
    file.write((char *)data->Data(), data->Length());
  }

  return 0;
}
