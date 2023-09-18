#include <glog/logging.h>
#include <stdint.h>
#include <yaml-cpp/yaml.h>
#include <string>
#include <vector>

struct ClientConfig {
  int clientId;
  std::string clientIp;
  int endpointType;
  int requestPort;
  uint32_t proxyMaxOwd;
  int proxyReplyPortBase;
  bool isOpenLoop;
  int poissonRate;
  uint32_t durationSec;
  int keyNum;
  double skewFactor;
  double writeRatio;
  int requestRetryTimeUs;

  int proxyRequestPortBase;
  std::vector<std::string> proxyIps;
  int proxyShardNum;

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
      key = "client-id";
      clientId = config[key].as<int>();
      key = "client-ip";
      clientIp = config[key].as<std::string>();
      key = "endpoint-type";
      endpointType = config[key].as<int>();
      key = "request-port";
      requestPort = config[key].as<int>();
      key = "is-openloop";
      isOpenLoop = config[key].as<bool>();
      key = "poisson-rate";
      poissonRate = config[key].as<int>();
      key = "duration-sec";
      durationSec = config[key].as<uint32_t>();
      key = "key-num";
      keyNum = config[key].as<int>();
      key = "skew-factor";
      skewFactor = config[key].as<double>();
      key = "write-ratio";
      writeRatio = config[key].as<double>();
      key = "request-retry-time-us";
      requestRetryTimeUs = config[key].as<int>();

      key = "proxy-ips";
      for (uint32_t i = 0; i < config[key].size(); i++) {
        proxyIps.push_back(config[key][i].as<std::string>());
      }
      key = "proxy-shards";
      proxyShardNum = config[key].as<int>();
      key = "proxy-request-port-base";
      proxyRequestPortBase = config[key].as<int>();

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