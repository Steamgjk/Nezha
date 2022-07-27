#include <fstream>
#include <iostream>
#include "lib/utils.h"
#include "lib/zipfian.h"
#include "proto/nezha_proto.pb.h"

DEFINE_string(folder, "/home/steam1994/micro-stats/2-10000-0-50",
              "The folder of the csv");

DEFINE_int32(replica_num, 2, "The number of replicas");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1;
  //   std::vector<uint32_t> zipfianKeys;
  //   uint32_t keyNum = 1000000;
  //   zipfianKeys.resize(1000000, 0);
  //   uint32_t skewFactor = 0.5;
  //   if (keyNum > 1) {
  //     std::default_random_engine generator(1);  // clientId as the seed
  //     zipfian_int_distribution<uint32_t> zipfianDistribution(0, keyNum - 1,
  //                                                            skewFactor);
  //     for (uint32_t i = 0; i < zipfianKeys.size(); i++) {
  //       zipfianKeys[i] = zipfianDistribution(generator);
  //     }
  //   }

  std::string r0Fname = FLAGS_folder + "/" + "Replica-Stats-0.csv";
  std::ifstream ifs1(r0Fname);
  LOG(INFO) << "fname=" << r0Fname;
  uint32_t clientId, reqId;
  uint32_t id = 0;
  std::map<uint64_t, uint32_t> mapIdx;
  std::map<uint64_t, uint64_t> mapKey;
  while (ifs1 >> clientId >> reqId) {
    uint64_t reqKey = CONCAT_UINT32(clientId, reqId);
    mapIdx[reqKey] = id;
    id++;
  }

  for (int i = 1; i < FLAGS_replica_num; i++) {
    std::string r1Fname =
        FLAGS_folder + "/" + "Replica-Stats-" + std::to_string(i) + ".csv";
    std::ifstream ifs2(r1Fname);
    std::vector<uint64_t> reqKeys;
    reqKeys.reserve(100000);
    std::vector<uint32_t> mappedIds;
    mappedIds.reserve(100000);
    while (ifs2 >> clientId >> reqId) {
      uint64_t reqKey = CONCAT_UINT32(clientId, reqId);
      reqKeys.push_back(reqKey);
      mappedIds.push_back(mapIdx[reqKey]);
    }
    uint32_t reorderedCase = 0;
    for (uint32_t i = 1; i < reqKeys.size(); i++) {
      if (mappedIds[i] == 0 || mappedIds[i] < mappedIds[i - 1]) {
        reorderedCase++;
      }
    }
    LOG(INFO) << "reorderedCase=" << reorderedCase << "\t"
              << "total=" << id << "\t rate=" << reorderedCase * 1.0 / id;
  }
}