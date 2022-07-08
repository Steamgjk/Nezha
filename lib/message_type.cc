#include "lib/message_type.h"



namespace MessageType {
    char CLIENT_REQUEST = 1;
    char LEADER_REQUEST = 2;
    char SYNC_INDEX = 3;
    char MISSED_INDEX_ASK = 4;
    char MISSED_REQ_ASK = 5;
    char FAST_REPLY = 6;
    char SLOW_REPLY = 7;
    char COMMIT_REPLY = 8;
    char MISSED_REQ = 9;
    char VIEWCHANGE_REQ = 10;
    char VIEWCHANGE = 11;
    char START_VIEW = 12;
    char STATE_TRANSFER_REQUEST = 13;
    char STATE_TRANSFER_REPLY = 14;
    char CRASH_VECTOR_REQUEST = 15;
    char CRASH_VECTOR_REPLY = 16;
    char RECOVERY_REQUEST = 17;
    char RECOVERY_REPLY = 18;
    char SYNC_STATUS_REPORT = 19;
    char COMMIT_INSTRUCTION = 20;
    char SUSPEND_REPLY = 21;
    char ERROR_MSG = 22;
};