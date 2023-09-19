#include <glog/logging.h>
#include <stdint.h>
#include <yaml-cpp/yaml.h>
#include <string>
#include <vector>

struct ReplicaConfig {
  uint32_t endpointType;
  std::vector<std::string> replicaIps;
  int replicaId;
  int receiverShards;
  int recordShards;
  int replyShards;
  int trackShards;
  int receiverPort;
  int indexSyncPort;
  int requestAskPort;
  int indexAskPort;
  int masterPort;
  int monitorPeriodMs;
  int heartbeatThresholdMs;
  int indexAskPeriodMs;
  int viewChangePeriodMs;
  int stateTransferPeriodMs;
  int stateTransferTimeoutMs;
  int indexTransferBatch;
  int requestKeyTransferBatch;
  int requestTransferBatch;
  int requestAskPeriodMs;
  int crashVectorRequestPeriodMs;
  int recoveryRequestPeriodMs;
  int syncReportPeriodMs;
  int indexSyncPeriodUs;
  double movingPercentile;
  int keyNum;
  uint32_t owdEstimationWindow;
  uint32_t reclaimTimeoutMs;
  int indexSyncShards;

  // The number of threads to process requests. For now process-shards
  // is fixed to 1, because the early-buffer enque/deque is hard to
  // parallelize. Maybe later we can find a high-performant **concurrent
  // priority queue** for early-buffer, then process-shards may be
  // parallelized for higher performance
  int processShards = 1;

  // Parses yaml file configFilename and fills in fields of ReplicaConfig
  // accordingly. Returns an error message or "" if there are no errors.
  std::string parseConfig(std::string configFilename) {
    YAML::Node config;
    try {
      config = YAML::LoadFile(configFilename);
    } catch (const YAML::BadFile& e) {
      return "Error loading config file:" + e.msg + ".";
    }
    LOG(INFO) << "Using config:\n " << config;

    std::string key;  // Keep track of current key for better error messages
    try {
      key = "endpoint-type";
      endpointType = config[key].as<uint32_t>();
      key = "replica-ips";
      for (uint32_t i = 0; i < config[key].size(); i++) {
        replicaIps.push_back(config[key][i].as<std::string>());
      }
      key = "replica-id";
      replicaId = config[key].as<int>();
      key = "receiver-shards";
      receiverShards = config[key].as<int>();
      key = "record-shards";
      recordShards = config[key].as<int>();
      key = "reply-shards";
      replyShards = config[key].as<int>();
      key = "index-sync-shards";
      indexSyncShards = config[key].as<int>();
      key = "track-shards";
      trackShards = config[key].as<int>();
      key = "receiver-port";
      receiverPort = config[key].as<int>();
      key = "index-sync-port";
      indexSyncPort = config[key].as<int>();
      key = "request-ask-port";
      requestAskPort = config[key].as<int>();
      key = "index-ask-port";
      indexAskPort = config[key].as<int>();
      key = "master-port";
      masterPort = config[key].as<int>();
      key = "monitor-period-ms";
      monitorPeriodMs = config[key].as<int>();
      key = "heartbeat-threshold-ms";
      heartbeatThresholdMs = config[key].as<int>();
      key = "index-ask-period-ms";
      indexAskPeriodMs = config[key].as<int>();
      key = "view-change-period-ms";
      viewChangePeriodMs = config[key].as<int>();
      key = "request-ask-period-ms";
      requestAskPeriodMs = config[key].as<int>();
      key = "state-transfer-period-ms";
      stateTransferPeriodMs = config[key].as<int>();
      key = "state-transfer-timeout-ms";
      stateTransferTimeoutMs = config[key].as<int>();
      key = "index-transfer-batch";
      indexTransferBatch = config[key].as<int>();
      key = "request-key-transfer-batch";
      requestKeyTransferBatch = config[key].as<int>();
      key = "request-transfer-batch";
      requestTransferBatch = config[key].as<int>();
      key = "crash-vector-request-period-ms";
      crashVectorRequestPeriodMs = config[key].as<int>();
      key = "recovery-request-period-ms";
      recoveryRequestPeriodMs = config[key].as<int>();
      key = "sync-report-period-ms";
      syncReportPeriodMs = config[key].as<int>();
      key = "key-num";
      keyNum = config[key].as<int>();
      key = "owd-estimation-window";
      owdEstimationWindow = config[key].as<uint32_t>();
      key = "index-sync-period-us";
      indexSyncPeriodUs = config[key].as<uint32_t>();
      key = "moving-percentile";
      movingPercentile = config[key].as<double>();
      key = "reclaim-timeout-ms";
      reclaimTimeoutMs = config[key].as<uint32_t>();
      return "";
    } catch (const YAML::BadConversion& e) {
      if (config[key]) {
        return "Error parsing config field " + key + ": " + e.msg + ".";
      } else {
        return "Error parsing config field " + key + ": " + key + " not found.";
      }
    } catch (const std::exception& e) {
      return "Error parsing config field " + key + ": " + e.what() + ".";
    }
  }
};