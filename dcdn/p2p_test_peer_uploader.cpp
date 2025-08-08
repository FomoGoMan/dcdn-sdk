#include "common/Logger.h"
#include "dcdn/Cert.h"
#include "dcdn/p2p_downloader.h"
#include "dcdn/p2p_downloader_interface.h"
#include "dcdn/p2p_single_task.h"
#include "plog/Formatters/TxtFormatter.h"
#include "plog/Init.h"
#include "plog/Severity.h"
#include "rtc/description.hpp"
#include "util/Downloader.h"
#include <condition_variable>
#include <fstream>
#include <memory>
#include <mutex>
#include <plog/Appenders/ConsoleAppender.h>
#include <string>

using namespace dcdn::download;

int main(int argc, char *argv[]) {
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

  rtc::Configuration config;
  config.iceUfrag = "a0hI";
  config.icePwd = "bKESPyeQ51OID4NQFypOIIuJ";

  rtc::IceServer serv("47.236.146.120", 3478);
  serv.type = rtc::IceServer::Type::Stun;
  config.iceServers.push_back(serv);
  config.enableIceUdpMux = true;
  config.certificatePemFile = cert.certPem;
  config.keyPemFile = cert.keyPem;

  std::cout << "hello" << std::endl;
  auto pc = std::make_shared<rtc::PeerConnection>(config);
  pc->onLocalCandidate([&](rtc::Candidate candidate) {
    std::cout << candidate.candidate() << std::endl;
  });
  pc->onGatheringStateChange([&](rtc::PeerConnection::GatheringState state) {
    if (state == rtc::PeerConnection::GatheringState::Complete) {
      std::cout << "gather done" << std::endl;
    }
    auto desc = pc->localDescription();
    if (desc) {
      std::cout << desc.value() << std::endl;
    }
  });

  std::string line;
  while (std::getline(std::cin, line)) {
    pc->onDataChannel(
        [=](std::shared_ptr<rtc::DataChannel> dc) { dc->send(line); });
  }
  return 0;
}
