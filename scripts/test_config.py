

import os

STATE_CONFIG = '/root/CloudExchange/configs/exchange_state.json'
MARKET_MAKER_CONFIG = "/root/CloudExchange/configs/market.json"

# Common config variables
NUM_CLIENTS = 48
ADD_MARKET_MAKER = 0
ENABLE_DYNAMIC_UPSTREAM = False
UPSTREAM_WINDOW_SIZE = 1000
UPSTREAM_STEP_SIZE_US = 5
UPSTREAM_FAIRNESSS_UPPER_BOUND = 0.01
UPSTREA_FAIRNESS_LOWER_BOUND = 0.01

DOWNSTREAM_WINDOW_SIZE = 500
DOWNSTREAM_STEP_SIZE_US = 5
DOWNSTREAM_FAIRNESSS_UPPER_BOUUND = 0.01
DOWNSTREA_FAIRNESS_LOWER_BOUND = 0.01


ENABLE_DYNAMIC_DOWNSTREAM = True
ENABLE_ARTIFICIAL_DELAY = True
GATEWAY_ESTIMATION_PERCENTILE = 0.9

TRADE_ESTIMATION_THRESHOLD = 500
TRADE_RESET_THRESHOLD = 5000
ORDER_ESTIMATION_THRESHOLD = 500
ORDER_RESET_THRESHOLD = 5000

ARTIFICIAL_DELAY_FILE = "/root/CloudExchange/configs/artificial_delay.json"
GATEWAY_REPLICATION = 1
LOG_VERBOSITY = 0

# Experiment components to run
CREATE_CLUSTER = True
CREATE_ME = True  # IF CREATE_CLUSTER==True, create ME VMs or not
CREATE_GWS = True  # IF CREATE_CLUSTER==True, create GW VMs or not
CREATE_CLIENTS = False  # IF CREATE_CLUSTER==True, create Client VMs or not
CREATE_WEBSERVER = False

COPY_SSH_KEYS = False
COPY_BINARIES = False

RUN_WEBSERVER = False

RUN_ME = True

RUN_GW = True
RUN_CLIENT = True
# IF RUN_CLIENT==True, # of Clients to run trader_algo at
NUM_CLIENTS_TO_RUN = NUM_CLIENTS  # for notebook: - 1

ENABLE_TTCS = False  # if CREATE_CLUSTER is False, this is generally also False
NEW_TTCS_CLUSTER = False  # i.e new reference node

ENABLE_SERVER_PUSHER = False
##############################################
# Cloud Platform configs
##############################################

# Instance name settings.
CLIENT_TAG = 'aa-client'
GW_TAG = 'aa-client'
ME_TAG = 'aa-me'
PROXY_TAG = 'aa-proxy'
WEBSERVER_TAG = 'aa-webserver'
REPLICA_TAG = "aa-replica"
TRADER_TAG = 'aa-trader'



# Google Compute Engine settings.
PROJECT_ID = 'jinkun-pro1'
BIGTABLE_INSTANCE_ID = 'test-bt'
ZONE = 'us-central1-a'
SOURCE_IMAGE = 'nezha'
STUDNET_IMAGE = 'nezha'

DEFAULT_CLIENT_MACHINE_TYPE = 'n1-standard-4'
DEFAULT_REPLICA_MACHINE_TYPE = 'n1-standard-32'
DEFAULT_WEB_SERVER_TYPE = 'n1-standard-4'
DEFAULT_GW_MACHINE_TYPE = 'n1-standard-8'
DEFAULT_ME_MACHINE_TYPE = 'n1-standard-8'
DEFAULT_MARKET_MAKER_MACHINE_TYPE = 'n1-standard-32'
CLOUD_BUCKET = 'tests-new'

# GCP credentials
if 'CLOUDEX_USERNAME' in os.environ:
    USERNAME = os.environ['CLOUDEX_USERNAME']
else:
    print(
        'error: Please specify the CLOUDEX_USERNAME in your environment if needed'
    )

SSH_KEY = os.environ.get('CLOUDEX_SSH_KEY', '')

##############################################
# Executables configs
##############################################
USER_PATH = '/root/'
CLIENT_TOKEN = '/root/CloudExchange/configs/client_token.json'
WEBDIS_CONFIG = '/root/CloudExchange/configs/webdis.json'
REPO_PATH = '/root/nezha/'

LOCAL_STATS_PATH = './new_perf_test'
LOCAL_CONFIG_PATH = '/root/CloudExchange/scripts/test_config.py'
LOCAL_STITCH_PATH = "general_stitch.py"
REMOTE_STITCH_PATH = "/root/CloudExchange/scripts/general_stitch.py"

LOCAL_BINARY_PATH = '/root/CloudExchange/bazel-bin/'
LOCAL_CLOUDEX_SO = LOCAL_BINARY_PATH + 'python/cloud_ex.so'
LOCAL_ANALYZE_PY = REPO_PATH + 'scripts/redis_analyze.py'
LOCAL_PERF_PY = REPO_PATH + 'scripts/get_cpu.py'
LOCAL_EXCHANGE_BIN_PATH = LOCAL_BINARY_PATH + 'exchange/exchange_runner'
LOCAL_GATEWAY_BIN_PATH = LOCAL_BINARY_PATH + 'exchange/gateway'
LOCAL_TRADERALGO_BIN_PATH = LOCAL_BINARY_PATH + 'trader/trader_algo'
LOCAL_TRADERSUB_BIN_PATH = LOCAL_BINARY_PATH + 'trader/trader_sub'
LOCAL_MARKET_MAKER_BIN_PATH = LOCAL_BINARY_PATH + 'trader/market_maker'
LOCAL_G_RANDOM_TRADER_BIN_PATH = LOCAL_BINARY_PATH + 'trader/g_random_trader'
LOCAL_G_MEAN_REVERSION_TRADER_BIN_PATH = LOCAL_BINARY_PATH + 'trader/mean_reversion_trader'
LOCAL_G_MOMENTUM_TRADER_BIN_PATH = LOCAL_BINARY_PATH + 'trader/momentum_trader'
LOCAL_REMOTE_SERVER_BIN_PATH = LOCAL_BINARY_PATH + 'database/remote_server'
SSH_KEY_FOLDER = "/root/CloudExchange/configs/ssh_keys"

SNAPSHOT_PERIOD = 0
TIMECHECKER_PERIOD = 0
TIMECHECKER_CONFIG = 'timechecker_config.json'

PERSIST_TO_DISK = 0

LOG_TB = 'log-tb'
RECORD_TB = 'record-tb'
ME_LOG_WORKER_NUM = 15
ME_RECORD_WORKER_NUM = 15
GW_LOG_WORKER_NUM = 4
CLIENT_LOG_WORKER_NUM = 1

##############################################
# Experiment configs
##############################################
JOB_NAME = 'nezha'
EXP_NAME = '01'
LOG_FILE_NAME = '/root/cloud_ex_exp.log'
MAXIMUM_OWD_US = 350
HR_OWD_US = 650
USE_INTERNAL_IP = True
ENABLE_EXTERNAL_IP_FOR_CLIENTS = False
ENABLE_EXTERNAL_IP_FOR_GWS = True
SECONDS_TO_WAIT_AFTER_STARTING_TTCS = 60

# Matching Engine specific configs
NUM_SHARDS = 1
NUM_SYMBOLS = 100
NUM_TARGET_SYMBOLS_PER_CLIENT = 5
NUM_SHARES = 100
BASE_SHARES = 200
SMART_NUM_SHARES = 1000
SMART_BASE_SHARES = 500
SMART_WAIT_INTERVAL = 0.1
NUM_PRICES = 150
NUM_TRANSACTION_RANGE = 30

REMOTE_SERVER_PORT = 55555
TRADE_PUBLISHER_PORT = 55556
ORDER_PUBLISHER_PORT = 55557

# Gateway specific configs
NUM_CLIENTS_PER_GATEWAY = 3

OVERWRITE_GATEWAY_TIMESTAMP = 0
HIGH_LATENCY_GATEWAY = 20000
LOW_LATENCY_GATEWAY = 0
NUM_HIGH_LATENCY_GATEWAYS = 1

# Trader specific configs
NUM_ORDERS = 1000000
ORDER_WINDOW_SIZE = 1
NUM_SMART_CLIENTS_PER_GATEWAY = 0
SECONDS_TO_WAIT_AFTER_STARTING_TRADERS = 300  # for notebook: 700000000 #60
SHARE_INITIAL = 10000
CASH_INITIAL = 10000000

MARKET_WARM_TIME = 20

##Random Trader
TRADER_SUBMIT_INTERVAL = 10000 #1000  #100000 #0
RANDOM_TRADER_PRICE_OFFSET = 5
RANDOM_TRADER_TARGET_SYMBOL_NUM = 5
RANDOM_TRADER_SYMBOL_RANGE_JUMP = 5

#Mean Reversion Trader
MEAN_REVERSION_TRADER_NUM = 0
MEAN_REVERSION_TRADER_TARGET_SYMBOL_NUM = 20
MEAN_REVERSION_TRADER_SYMBOL_RANGE_JUMP = 5
MEAN_REVERSION_TRADER_BASE_SHARES = 3000
MEAN_REVERSION_TRADER_TICK_LENGTH = 1
MEAN_REVERSION_TRADERS = {}

# Momentum Reversion Trader
MOMENTUM_TRADER_NUM = 0
MOMENTUM_TRADER_TARGET_SYMBOL_NUM = 20
MOMENTUM_TRADER_SYMBOL_RANGE_JUMP = 5
MOMENTUM_TRADER_BASE_SHARES = 3000
MOMENTUM_TRADER_TICK_LENGTH = 1
ENABLE_DATA_AGGREGATOR = False
ENABLE_LOG_WRITER = False

RUN_TRADER_ALGO_IF_TRUE_WEBDIS_IF_FALSE = True
ENABLE_STATS_COLLECTION = False
USE_NTP = True

##############################################
# Inferred Experiment configs
##############################################
NUM_GATEWAYS = (NUM_CLIENTS - 1) // NUM_CLIENTS_PER_GATEWAY + 1
GATEWAY_LATENCIES = [
    HIGH_LATENCY_GATEWAY for i in range(NUM_HIGH_LATENCY_GATEWAYS)
] + [
    LOW_LATENCY_GATEWAY
    for i in range(NUM_GATEWAYS - NUM_HIGH_LATENCY_GATEWAYS)
]

LOGIN_PATH = "/home/steam1994"
