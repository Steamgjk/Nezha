#include <glog/logging.h>
#include <stdint.h>
#include <yaml-cpp/yaml.h>
#include <string>
#include <vector>

struct ProxyConfig {
  int proxyId;
  std::string proxyIp;
  int proxyShardNum;
  uint32_t proxyMaxOwd;
  int proxyRequestPortBase;
  int proxyReplyPortBase;

  std::vector<std::string> replicaIps;
  uint32_t replicaInitialOwd;
  int replicaReceiverPort;
  int replicaReceiverShards;

  // Parses yaml file configFilename and fills in fields of ProxyConfig
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
      key = "replica-ips";
      for (uint32_t i = 0; i < config[key].size(); i++) {
        replicaIps.push_back(config[key][i].as<std::string>());
      }
      key = "replica-receiver-shards";
      replicaReceiverShards = config[key].as<int>();
      key = "replica-initial-owd";
      replicaInitialOwd = config[key].as<uint32_t>();
      key = "replica-receiver-port";
      replicaReceiverPort = config[key].as<int>();

      key = "proxy-id";
      proxyId = config[key].as<int>();
      key = "proxy-ip";
      proxyIp = config[key].as<std::string>();
      key = "proxy-shard-num";
      proxyShardNum = config[key].as<int>();
      key = "proxy-max-owd";
      proxyMaxOwd = config[key].as<uint32_t>();
      key = "proxy-request-port-base";
      proxyRequestPortBase = config[key].as<int>();
      key = "proxy-reply-port-base";
      proxyReplyPortBase = config[key].as<int>();

      return "";
    } catch (const YAML::BadConversion& e) {
      if (config[key]) {
        return "Error parsing config field " + key + ": " + e.msg + ".";
      } else {
        return "Error parsing config field " + key + ": key not found.";
      }
    } catch (const std::exception& e) {
      return "Error parsing config field " + key + ": " + e.what() + ".";
    }
  }
};