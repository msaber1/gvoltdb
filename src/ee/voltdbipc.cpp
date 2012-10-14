/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 Implement the Java ExecutionEngine interface using IPC to a standalone EE
 process. This allows the backend to run without a JVM - useful for many
 debugging tasks.  Represents a single EE in a single process. Accepts
 and executes commands from Java synchronously.
 */

#include "common/Topend.h"

#include "common/debuglog.h"
#include "common/ids.h"
#include "common/serializeio.h"
#include "common/Pool.hpp"
#include "common/FatalException.hpp"
#include "common/SegvException.hpp"
#include "common/RecoveryProtoMessage.h"
#include "common/TheHashinator.h"
#include "common/ThreadLocalPool.h"
#include "execution/VoltDBEngine.h"
#include "logging/LogDefs.h"
#include "logging/LogProxy.h"
#include "logging/StdoutLogProxy.h"
#include "storage/StreamBlock.h"
#include "storage/table.h"

#include <cassert>
#include <cstdlib>
#include <dlfcn.h>
#include <iostream>
#include <signal.h>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>


// Please don't make this different from the JNI result buffer size.
// This determines the size of the EE results buffer and it's nice
// if IPC and JNI are matched.
#define MAX_MSG_SZ (1024*1024*10)

using namespace std;

/* java sends all data with this header */
struct ipc_command {
    int32_t msgsize;
    int32_t command;
    char data[0];
}__attribute__((packed));

/*
 * Structure describing an executePlanFragments message header.
 */
typedef struct {
    struct ipc_command cmd;
    int64_t txnId;
    int64_t lastCommittedTxnId;
    int64_t undoToken;
    int32_t numFragmentIds;
    char data[0];
}__attribute__((packed)) querypfs;

typedef struct {
    struct ipc_command cmd;
    int32_t planFragLength;
    char data[0];
}__attribute__((packed)) loadfrag;

/*
 * Header for a load table request.
 */
typedef struct {
    struct ipc_command cmd;
    int32_t tableId;
    int64_t txnId;
    int64_t lastCommittedTxnId;
    char data[0];
}__attribute__((packed)) load_table_cmd;

/*
 * Header for a stats table request.
 */
typedef struct {
    struct ipc_command cmd;
    int32_t selector;
    int8_t  interval;
    int64_t now;
    int32_t num_locators;
    int32_t locators[0];
}__attribute__((packed)) get_stats_cmd;

/*
 * Header for a saveTableToDisk request
 */
typedef struct {
    struct ipc_command cmd;
    int32_t clusterId;
    int32_t databaseId;
    int32_t tableId;
    char data[0];
}__attribute__((packed)) save_table_to_disk_cmd;

struct undo_token {
    struct ipc_command cmd;
    int64_t token;
}__attribute__((packed));

/*
 * Header for a ActivateCopyOnWrite request
 */
typedef struct {
    struct ipc_command cmd;
    voltdb::CatalogId tableId;
    voltdb::TableStreamType streamType;
}__attribute__((packed)) activate_tablestream;

/*
 * Header for a Copy On Write Serialize More request
 */
typedef struct {
    struct ipc_command cmd;
    voltdb::CatalogId tableId;
    voltdb::TableStreamType streamType;
    int bufferSize;
}__attribute__((packed)) tablestream_serialize_more;

/*
 * Header for an incoming recovery message
 */
typedef struct {
    struct ipc_command cmd;
    int32_t messageLength;
    char message[0];
}__attribute__((packed)) recovery_message;

/*
 * Header for a request for a table hash code
 */
typedef struct {
    struct ipc_command cmd;
    int32_t tableId;
}__attribute__((packed)) table_hash_code;

typedef struct {
    struct ipc_command cmd;
    int32_t partitionCount;
    char data[0];
}__attribute__((packed)) hashinate_msg;

/*
 * Header for an Export action.
 */
typedef struct {
    struct ipc_command cmd;
    int32_t isSync;
    int64_t offset;
    int64_t seqNo;
    int32_t tableSignatureLength;
    char tableSignature[0];
}__attribute__((packed)) export_action;

typedef struct {
    struct ipc_command cmd;
    int32_t tableSignatureLength;
    char tableSignature[0];
}__attribute__((packed)) get_uso;

typedef struct {
    struct ipc_command cmd;
    int64_t txnId;
    char data[0];
}__attribute__((packed)) catalog_load;


using namespace voltdb;

// must match ERRORCODE_SUCCESS|ERROR in ExecutionEngine.java
enum {
    kErrorCode_None = -1, kErrorCode_Success = 0, kErrorCode_Error = 1,
    /*
     * The following are not error codes but requests for information or functionality
     * from Java. These do not exist in ExecutionEngine.java since they are IPC specific.
     * These constants are mirrored in ExecutionEngine.java.
     * (conveniently, where they don't exist? --paul).
     */
    kErrorCode_RetrieveDependency = 100, //Request for dependency
    kErrorCode_DependencyFound = 101,    //Response to 100
    kErrorCode_DependencyNotFound = 102, //Also response to 100
    kErrorCode_pushExportBuffer = 103, //Indication that el buffer is next
    kErrorCode_CrashVoltDB = 104, //Crash with reason string
    kErrorCode_getQueuedExportBytes = 105 //Retrieve value for stats
};

/**
 * Log a statement on behalf of the IPC log proxy at the specified log level
 * @param LoggerId ID of the logger that received this statement
 * @param level Log level of the statement
 * @param statement null terminated UTF-8 string containing the statement to log
 */
static void log(LoggerId loggerId, LogLevel level, const char *statement) const;

static VoltDBEngine *s_engine = null;
static int s_fd;
static char *s_reusedResultBuffer = null;
static char *s_exceptionBuffer = null;
static bool s_terminate = false;


static int8_t loadCatalog(struct ipc_command *cmd);
static int8_t updateCatalog(struct ipc_command *cmd);
static int8_t initialize(struct ipc_command *cmd);
static int8_t toggleProfiler(struct ipc_command *cmd);
static int8_t releaseUndoToken(struct ipc_command *cmd);
static int8_t undoUndoToken(struct ipc_command *cmd);
static int8_t tick(struct ipc_command *cmd);
static int8_t quiesce(struct ipc_command *cmd);
static int8_t setLogLevels(struct ipc_command *cmd);

static void executePlanFragments(struct ipc_command *cmd);

static void loadFragment(struct ipc_command *cmd);

static void getStats(struct ipc_command *cmd);

static int8_t loadTable(struct ipc_command *cmd);

static int8_t processRecoveryMessage( struct ipc_command *cmd);

static void tableHashCode( struct ipc_command *cmd);

static void hashinate(struct ipc_command* cmd);

static void threadLocalPoolAllocations();

static void sendException( int8_t errorCode);

static int8_t activateTableStream(struct ipc_command *cmd);
static void tableStreamSerializeMore(struct ipc_command *cmd);
static void exportAction(struct ipc_command *cmd);
static void getUSOForExportTable(struct ipc_command *cmd);

static void signalHandler(int signum, siginfo_t *info, void *context);
static void signalDispatcher(int signum, siginfo_t *info, void *context);
static void setupSigHandler(void) const;

// file static help function to do a blocking write.
// exit on a -1.. otherwise return when all bytes
// written.
static void writeOrDie(void* data, ssize_t sz) {
    ssize_t written = 0;
    ssize_t last = 0;
    if (sz == 0) {
        return;
    }
    do {
        last = write(s_fd, reinterpret_cast<unsigned char *>(data) + written, sz - written);
        if (last < 0) {
            printf("\n\nIPC write to JNI returned -1. Exiting\n\n");
            fflush(stdout);
            exit(-1);
        }
        written += last;
    } while (written < sz);
}


static bool execute(struct ipc_command *cmd) {
    int8_t result = kErrorCode_None;

    if (0)
        cout << "IPC client command: " << ntohl(cmd->command) << endl;

    // commands must match java's ExecutionEngineIPC.Command
    // could enumerate but they're only used in this one place.
    switch (ntohl(cmd->command)) {
      case 0:
        result = initialize(cmd);
        break;
      case 2:
        result = loadCatalog(cmd);
        break;
      case 3:
        result = toggleProfiler(cmd);
        break;
      case 4:
        result = tick(cmd);
        break;
      case 5:
        getStats(cmd);
        result = kErrorCode_None;
        break;
      case 6:
        // also writes results directly
        executePlanFragments(cmd);
        result = kErrorCode_None;
        break;
      case 9:
        result = loadTable(cmd);
        break;
      case 10:
        result = releaseUndoToken(cmd);
        break;
      case 11:
        result = undoUndoToken(cmd);
        break;
      case 13:
        result = setLogLevels(cmd);
        break;
      case 16:
        result = quiesce(cmd);
        break;
      case 17:
        result = activateTableStream(cmd);
        break;
      case 18:
        tableStreamSerializeMore(cmd);
        result = kErrorCode_None;
        break;
      case 19:
        result = updateCatalog(cmd);
        break;
      case 20:
        exportAction(cmd);
        result = kErrorCode_None;
        break;
      case 21:
          result = processRecoveryMessage(cmd);
        break;
      case 22:
          tableHashCode(cmd);
          result = kErrorCode_None;
          break;
      case 23:
          hashinate(cmd);
          result = kErrorCode_None;
          break;
      case 24:
          threadLocalPoolAllocations();
          result = kErrorCode_None;
          break;
      case 26:
          loadFragment(cmd);
          result = kErrorCode_None;
          break;
      default:
        printf("IPC command %d not implemented.\n", ntohl(cmd->command));
        fflush(stdout);
        result = kErrorCode_Error;
    }

    // write results for the simple commands. more
    // complex commands write directly in the command
    // implementation.
    if (result != kErrorCode_None) {
        if (result == kErrorCode_Error) {
            char msg[5];
            msg[0] = result;
            *reinterpret_cast<int32_t*>(&msg[1]) = 0;//exception length 0
            writeOrDie(msg, sizeof(int8_t) + sizeof(int32_t));
        } else {
            writeOrDie(&result, sizeof(int8_t));
        }
    }
    return s_terminate;
}

static int8_t loadCatalog(struct ipc_command *cmd) {
    printf("loadCatalog\n");
    assert(s_engine);
    if (!s_engine)
        return kErrorCode_Error;

    catalog_load *msg = reinterpret_cast<catalog_load*>(cmd);
    try {
        if (s_engine->loadCatalog(ntohll(msg->txnId), string(msg->data)) == true) {
            return kErrorCode_Success;
        }
    } catch (SerializableEEException &e) {
        //TODO: Probably not advisable to just squelch a serializable exception from loadCatalog?
    }
    return kErrorCode_Error;
}

static int8_t updateCatalog(struct ipc_command *cmd) {
    assert(s_engine);
    if (!s_engine) {
        return kErrorCode_Error;
    }

    struct updatecatalog {
        struct ipc_command cmd;
        int64_t txnId;
        char data[];
    };
    struct updatecatalog *uc = (struct updatecatalog*)cmd;
    try {
        if (s_engine->updateCatalog(ntohll(uc->txnId), string(uc->data)) == true) {
            return kErrorCode_Success;
        }
    } catch (FatalException e) {
        crashVoltDB(e);
    }
    return kErrorCode_Error;
}

static int8_t initialize(struct ipc_command *cmd) {
    // expect a single initialization.
    assert(!s_engine);
    delete s_engine;

    // voltdbengine::initialize expects catalogids.
    assert(sizeof(CatalogId) == sizeof(int));

    struct initialize {
        struct ipc_command cmd;
        int clusterId;
        long siteId;
        int partitionId;
        int hostId;
        int64_t logLevels;
        int64_t tempTableMemory;
        int32_t totalPartitions;
        int16_t hostnameLength;
        char hostname[0];
    }__attribute__((packed));
    struct initialize * cs = (struct initialize*) cmd;

    printf("initialize: cluster=%d, site=%jd\n",
           ntohl(cs->clusterId), (intmax_t)ntohll(cs->siteId));
    cs->clusterId = ntohl(cs->clusterId);
    cs->siteId = ntohll(cs->siteId);
    cs->partitionId = ntohl(cs->partitionId);
    cs->hostId = ntohl(cs->hostId);
    cs->hostnameLength = ntohs(cs->hostnameLength);
    cs->logLevels = ntohll(cs->logLevels);
    cs->tempTableMemory = ntohll(cs->tempTableMemory);
    cs->totalPartitions = ntohl(cs->totalPartitions);
    string hostname(cs->hostname, cs->hostnameLength);
    try {
        s_engine = new VoltDBEngine(this, new StdoutLogProxy());
        s_engine->getLogManager()->setLogLevels(cs->logLevels);
        s_reusedResultBuffer = new char[MAX_MSG_SZ];
        s_exceptionBuffer = new char[MAX_MSG_SZ];
        s_engine->setBuffers( NULL, 0, s_reusedResultBuffer, MAX_MSG_SZ, s_exceptionBuffer, MAX_MSG_SZ);
        if (s_engine->initialize(cs->clusterId,
                                 cs->siteId,
                                 cs->partitionId,
                                 cs->hostId,
                                 hostname,
                                 cs->tempTableMemory,
                                 cs->totalPartitions) == true) {
            return kErrorCode_Success;
        }
    } catch (FatalException e) {
        crashVoltDB(e);
    }
    return kErrorCode_Error;
}

static int8_t toggleProfiler(struct ipc_command *cmd) {
    assert(s_engine);
    if (!s_engine)
        return kErrorCode_Error;

    struct toggle {
        struct ipc_command cmd;
        int toggle;
    }__attribute__((packed));
    struct toggle * cs = (struct toggle*) cmd;

    printf("toggleProfiler: toggle=%d\n", ntohl(cs->toggle));

    // actually, the engine doesn't implement this now.
    // s_engine->ProfilerStart();
    return kErrorCode_Success;
}

static int8_t releaseUndoToken(struct ipc_command *cmd) {
    assert(s_engine);
    if (!s_engine)
        return kErrorCode_Error;


    struct undo_token * cs = (struct undo_token*) cmd;

    try {
        s_engine->releaseUndoToken(ntohll(cs->token));
    } catch (FatalException e) {
        crashVoltDB(e);
    }

    return kErrorCode_Success;
}

static int8_t undoUndoToken(struct ipc_command *cmd) {
    assert(s_engine);
    if (!s_engine)
        return kErrorCode_Error;


    struct undo_token * cs = (struct undo_token*) cmd;

    try {
        s_engine->undoUndoToken(ntohll(cs->token));
    } catch (FatalException e) {
        crashVoltDB(e);
    }

    return kErrorCode_Success;
}

static int8_t tick(struct ipc_command *cmd) {
    assert (s_engine);
    if (!s_engine)
        return kErrorCode_Error;

    struct tick {
        struct ipc_command cmd;
        int64_t time;
        int64_t lastTxnId;
    }__attribute__((packed));

    struct tick * cs = (struct tick*) cmd;
    //cout << "tick: time=" << cs->time << " txn=" << cs->lastTxnId << endl;

    try {
        // no return code. can't fail!
        s_engine->tick(ntohll(cs->time), ntohll(cs->lastTxnId));
    } catch (FatalException e) {
        crashVoltDB(e);
    }

    return kErrorCode_Success;
}

static int8_t quiesce(struct ipc_command *cmd) {
    struct quiesce {
        struct ipc_command cmd;
        int64_t lastTxnId;
    }__attribute__((packed));

    struct quiesce *cs = (struct quiesce*)cmd;

    try {
        s_engine->quiesce(ntohll(cs->lastTxnId));
    } catch (FatalException e) {
        crashVoltDB(e);
    }

    return kErrorCode_Success;
}

static void executePlanFragments(struct ipc_command *cmd) {
    int errors = 0;

    querypfs *queryCommand = (querypfs*) cmd;

    int32_t numFrags = ntohl(queryCommand->numFragmentIds);

    if (0)
        cout << "querypfs:" << " txnId=" << ntohll(queryCommand->txnId)
                  << " txnId=" << ntohll(queryCommand->txnId)
                  << " lastCommitted=" << ntohll(queryCommand->lastCommittedTxnId)
                  << " undoToken=" << ntohll(queryCommand->undoToken)
                  << " numFragIds=" << numFrags << endl;

    // data has binary packed fragmentIds first
    int64_t *fragmentId = (int64_t*) (&(queryCommand->data));
    int64_t *inputDepId = fragmentId + numFrags;

    // ...and fast serialized parameter sets last.
    char* offset = queryCommand->data + (sizeof(int64_t) * numFrags * 2);
    int sz = static_cast<int> (ntohl(cmd->msgsize) - sizeof(querypfs) - sizeof(int32_t) * ntohl(queryCommand->numFragmentIds));
    s_engine->deserializeParameterSet(offset, sz);

    try {
        // and reset to space for the results output
        s_engine->resetReusedResultOutputBuffer(1);//1 byte to add status code
        s_engine->setUndoToken(ntohll(queryCommand->undoToken));
        for (int i = 0; i < numFrags; ++i) {
            if (s_engine->executeQuery(ntohll(fragmentId[i]),
                                       1,
                                       (int32_t)(ntohll(inputDepId[i])), // Java sends int64 but EE wants int32
                                       ntohll(queryCommand->txnId),
                                       ntohll(queryCommand->lastCommittedTxnId),
                                       i == 0 ? true : false, //first
                                       i == numFrags - 1 ? true : false)) { //last
                ++errors;
            }
        }
        s_engine->resizePlanCache(); // shrink cache if need be
    }
    catch (FatalException e) {
        crashVoltDB(e);
    }

    // write the results array back across the wire
    if (errors == 0) {
        // write the results array back across the wire
        const int32_t size = s_engine->getResultsSize();
        char *resultBuffer = s_engine->getReusedResultBuffer();
        resultBuffer[0] = kErrorCode_Success;
        writeOrDie(resultBuffer, size);
    } else {
        sendException(kErrorCode_Error);
    }
}

static void sendException(int8_t errorCode) {
    writeOrDie(&errorCode, sizeof(int8_t));

    const void* exceptionData =
      s_engine->getExceptionOutputSerializer()->data();
    int32_t exceptionLength =
      static_cast<int32_t>(ntohl(*reinterpret_cast<const int32_t*>(exceptionData)));
    printf("Sending exception length %d\n", exceptionLength);
    fflush(stdout);

    const size_t expectedSize = exceptionLength + sizeof(int32_t);
    writeOrDie(exceptionData, expectedSize);
}

/**
 * Ensure a plan fragment is loaded.
 * Return error code, fragmentid for plan, and cache stats
 */
static void loadFragment(struct ipc_command *cmd) {
    int errors = 0;

    loadfrag *load = (loadfrag*)cmd;

    int32_t planFragLength = ntohl(load->planFragLength);

    int64_t fragId;
    bool wasHit;
    int64_t cacheSize;

    try {
        // execute
        if (s_engine->loadFragment(load->data, planFragLength, fragId, wasHit, cacheSize)) {
            ++errors;
        }
    } catch (FatalException e) {
        crashVoltDB(e);
    }

    // make network suitable
    fragId = htonll(fragId);
    int64_t wasHitLong = htonll((wasHit ? 1 : 0));
    cacheSize = htonll(cacheSize);

    // write the results array back across the wire
    const int8_t successResult = kErrorCode_Success;
    if (errors == 0) {
        writeOrDie(&successResult, sizeof(int8_t));
        writeOrDie(&fragId, sizeof(int64_t));
        writeOrDie(&wasHitLong, sizeof(int64_t));
        writeOrDie(&cacheSize, sizeof(int64_t));
    } else {
        sendException(kErrorCode_Error);
    }
}

static int8_t loadTable(struct ipc_command *cmd) {
    load_table_cmd *loadTableCommand = (load_table_cmd*) cmd;

    if (0) {
        cout << "loadTable:" << " tableId=" << ntohl(loadTableCommand->tableId)
                  << " txnId=" << ntohll(loadTableCommand->txnId) << " lastCommitted="
                  << ntohll(loadTableCommand->lastCommittedTxnId) << endl;
    }

    const int32_t tableId = ntohl(loadTableCommand->tableId);
    const int64_t txnId = ntohll(loadTableCommand->txnId);
    const int64_t lastCommittedTxnId = ntohll(loadTableCommand->lastCommittedTxnId);
    // ...and fast serialized table last.
    void* offset = loadTableCommand->data;
    int sz = static_cast<int> (ntohl(cmd->msgsize) - sizeof(load_table_cmd));
    try {
        ReferenceSerializeInput serialize_in(offset, sz);

        bool success = s_engine->loadTable(tableId, serialize_in, txnId, lastCommittedTxnId);
        if (success) {
            return kErrorCode_Success;
        } else {
            return kErrorCode_Error;
        }
    } catch (FatalException e) {
        crashVoltDB(e);
    }
    return kErrorCode_Error;
}

static int8_t setLogLevels(struct ipc_command *cmd) {
    int64_t logLevels = *((int64_t*)&cmd->data[0]);
    try {
        s_engine->getLogManager()->setLogLevels(logLevels);
    } catch (FatalException e) {
        crashVoltDB(e);
    }
    return kErrorCode_Success;
}

static void terminate() {
    s_terminate = true;
}

int KoltDBIPC::loadNextDependency(int32_t dependencyId, Pool *stringPool, Table* destination) {
    VOLT_DEBUG("iterating java dependency for id %d\n", dependencyId);
    size_t dependencySz = 0;
    char message[5];

    // tell java to send the dependency over the socket
    message[0] = static_cast<int8_t>(kErrorCode_RetrieveDependency);
    *reinterpret_cast<int32_t*>(&message[1]) = htonl(dependencyId);
    writeOrDie(message, sizeof(int8_t) + sizeof(int32_t));

    // read java's response code
    int8_t responseCode;
    ssize_t bytes = read(s_fd, &responseCode, sizeof(int8_t));
    if (bytes != sizeof(int8_t)) {
        printf("Error - blocking read failed. %jd read %jd attempted",
                (intmax_t)bytes, (intmax_t)sizeof(int8_t));
        fflush(stdout);
        assert(false);
        exit(-1);
    }

    // deal with error response codes
    if (kErrorCode_DependencyNotFound == responseCode) {
        return 0;
    } else if (kErrorCode_DependencyFound != responseCode) {
        printf("Received unexpected response code %d to retrieve dependency request\n",
                (int)responseCode);
        fflush(stdout);
        assert(false);
        exit(-1);
    }

    // start reading the dependency. its length is first
    int32_t dependencyLength;
    bytes = read(s_fd, &dependencyLength, sizeof(int32_t));
    if (bytes != sizeof(int32_t)) {
        printf("Error - blocking read failed. %jd read %jd attempted",
                (intmax_t)bytes, (intmax_t)sizeof(int32_t));
        fflush(stdout);
        assert(false);
        exit(-1);
    }

    bytes = 0;
    dependencyLength = ntohl(dependencyLength);
    if (dependencyLength == 0) {
        return 0;
    }
    dependencySz = (size_t)dependencyLength;
    char *dependencyData = new char[dependencyLength];
    while (bytes != dependencyLength) {
        ssize_t oldBytes = bytes;
        bytes += read(s_fd, dependencyData + bytes, dependencyLength - bytes);
        if (oldBytes == bytes) {
            break;
        }
        if (oldBytes > bytes) {
            bytes++;
            break;
        }
    }

    if (bytes != dependencyLength) {
        printf("Error - blocking read failed. %jd read %jd attempted",
                (intmax_t)bytes, (intmax_t)dependencyLength);
        fflush(stdout);
        assert(false);
        exit(-1);
    }

    ReferenceSerializeInput serialize_in(dependencyData, dependencySz);
    destination->loadTuplesFrom(serialize_in, stringPool);
    delete [] dependencyData;
    return 1;
}

void KoltDBIPC::crashVoltDB(FatalException e) {
    const char *reasonBytes = e.m_reason.c_str();
    int32_t reasonLength = static_cast<int32_t>(strlen(reasonBytes));
    int32_t lineno = static_cast<int32_t>(e.m_lineno);
    int32_t filenameLength = static_cast<int32_t>(strlen(e.m_filename));
    int32_t numTraces = static_cast<int32_t>(e.m_traces.size());
    int32_t totalTracesLength = 0;
    for (int ii = 0; ii < static_cast<int>(e.m_traces.size()); ii++) {
        totalTracesLength += static_cast<int32_t>(strlen(e.m_traces[ii].c_str()));
    }
    //sizeof traces text + length prefix per trace, length prefix of reason string, number of traces count,
    //filename length, lineno
    int32_t messageLength =
            static_cast<int32_t>(
                    totalTracesLength +
                    (sizeof(int32_t) * numTraces) +
                    (sizeof(int32_t) * 4) +
                    reasonLength +
                    filenameLength);

    //status code
    s_reusedResultBuffer[0] = static_cast<char>(kErrorCode_CrashVoltDB);
    size_t position = 1;

    //overall message length, not included in messageLength
    *reinterpret_cast<int32_t*>(&s_reusedResultBuffer[position]) = htonl(messageLength);
    position += sizeof(int32_t);

    //reason string
    *reinterpret_cast<int32_t*>(&s_reusedResultBuffer[position]) = htonl(reasonLength);
    position += sizeof(int32_t);
    memcpy( &s_reusedResultBuffer[position], reasonBytes, reasonLength);
    position += reasonLength;

    //filename string
    *reinterpret_cast<int32_t*>(&s_reusedResultBuffer[position]) = htonl(filenameLength);
    position += sizeof(int32_t);
    memcpy( &s_reusedResultBuffer[position], e.m_filename, filenameLength);
    position += filenameLength;

    //lineno
    *reinterpret_cast<int32_t*>(&s_reusedResultBuffer[position]) = htonl(lineno);
    position += sizeof(int32_t);

    //number of traces
    *reinterpret_cast<int32_t*>(&s_reusedResultBuffer[position]) = htonl(numTraces);
    position += sizeof(int32_t);

    for (int ii = 0; ii < static_cast<int>(e.m_traces.size()); ii++) {
        int32_t traceLength = static_cast<int32_t>(strlen(e.m_traces[ii].c_str()));
        *reinterpret_cast<int32_t*>(&s_reusedResultBuffer[position]) = htonl(traceLength);
        position += sizeof(int32_t);
        memcpy( &s_reusedResultBuffer[position], e.m_traces[ii].c_str(), traceLength);
        position += traceLength;
    }

    writeOrDie(s_reusedResultBuffer, 5 + messageLength);
    exit(-1);
}

static void getStats(struct ipc_command *cmd) {
    get_stats_cmd *getStatsCommand = (get_stats_cmd*) cmd;

    const int32_t selector = ntohl(getStatsCommand->selector);
    const int32_t numLocators = ntohl(getStatsCommand->num_locators);
    bool interval = false;
    if (getStatsCommand->interval != 0) {
        interval = true;
    }
    const int64_t now = ntohll(getStatsCommand->now);
    int32_t *locators = new int32_t[numLocators];
    for (int ii = 0; ii < numLocators; ii++) {
        locators[ii] = ntohl(getStatsCommand->locators[ii]);
    }

    s_engine->resetReusedResultOutputBuffer();

    try {
        int result = s_engine->getStats(
                static_cast<int>(selector),
                locators,
                numLocators,
                interval,
                now);

        delete [] locators;

        // write the results array back across the wire
        const int8_t successResult = kErrorCode_Success;
        if (result == 1) {
            writeOrDie(&successResult, sizeof(int8_t));

            // write the dependency tables back across the wire
            // the result set includes the total serialization size
            const int32_t size = s_engine->getResultsSize();
            writeOrDie((s_engine->getReusedResultBuffer()), size);
        } else {
            sendException(kErrorCode_Error);
        }
    } catch (FatalException e) {
        crashVoltDB(e);
    }
}

static int8_t activateTableStream(struct ipc_command *cmd) {
    activate_tablestream *activateTableStreamCommand = (activate_tablestream*) cmd;
    const CatalogId tableId = ntohl(activateTableStreamCommand->tableId);
    const TableStreamType streamType =
            static_cast<TableStreamType>(ntohl(activateTableStreamCommand->streamType));
    try {
        if (s_engine->activateTableStream(tableId, streamType)) {
            return kErrorCode_Success;
        } else {
            return kErrorCode_Error;
        }
    } catch (FatalException e) {
        crashVoltDB(e);
    }
    return kErrorCode_Error;
}

static void tableStreamSerializeMore(struct ipc_command *cmd) {
    tablestream_serialize_more *tableStreamSerializeMore = (tablestream_serialize_more*) cmd;
    const CatalogId tableId = ntohl(tableStreamSerializeMore->tableId);
    const TableStreamType streamType =
            static_cast<TableStreamType>(ntohl(tableStreamSerializeMore->streamType));
    const int bufferLength = ntohl(tableStreamSerializeMore->bufferSize);
    assert(bufferLength < MAX_MSG_SZ - 5);

    if (bufferLength >= MAX_MSG_SZ - 5) {
        char msg[3];
        msg[0] = kErrorCode_Error;
        *reinterpret_cast<int16_t*>(&msg[1]) = 0;//exception length 0
        writeOrDie(msg, sizeof(int8_t) + sizeof(int16_t));
    }

    try {
        ReferenceSerializeOutput out(s_reusedResultBuffer + 5, bufferLength);
        int serialized = s_engine->tableStreamSerializeMore(&out, tableId, streamType);
        s_reusedResultBuffer[0] = kErrorCode_Success;
        *reinterpret_cast<int32_t*>(&s_reusedResultBuffer[1]) = htonl(serialized);

        /*
         * Already put the -1 code into the message.
         * Set it 0 so toWrite has the correct number of bytes
         */
        if (serialized == -1) {
            serialized = 0;
        }
        const ssize_t toWrite = serialized + 5;
        writeOrDie(s_reusedResultBuffer, toWrite);
    } catch (FatalException e) {
        crashVoltDB(e);
    }
}

static int8_t processRecoveryMessage( struct ipc_command *cmd) {
    recovery_message *recoveryMessage = (recovery_message*) cmd;
    const int32_t messageLength = ntohl(recoveryMessage->messageLength);
    ReferenceSerializeInput input(recoveryMessage->message, messageLength);
    RecoveryProtoMsg message(&input);
    s_engine->processRecoveryMessage(&message);
    return kErrorCode_Success;
}

static void tableHashCode( struct ipc_command *cmd) {
    table_hash_code *hashCodeRequest = (table_hash_code*) cmd;
    const int32_t tableId = ntohl(hashCodeRequest->tableId);
    int64_t tableHashCode = s_engine->tableHashCode(tableId);
    char response[9];
    response[0] = kErrorCode_Success;
    *reinterpret_cast<int64_t*>(&response[1]) = htonll(tableHashCode);
    writeOrDie(response, 9);
}

static void exportAction(struct ipc_command *cmd) {
    export_action *action = (export_action*)cmd;

    s_engine->resetReusedResultOutputBuffer();
    int32_t tableSignatureLength = ntohl(action->tableSignatureLength);
    string tableSignature(action->tableSignature, tableSignatureLength);
    int64_t result = s_engine->exportAction(action->isSync,
                                         static_cast<int64_t>(ntohll(action->offset)),
                                         static_cast<int64_t>(ntohll(action->seqNo)),
                                         tableSignature);

    // write offset across bigendian.
    result = htonll(result);
    writeOrDie(&result, sizeof(result));
}

static void getUSOForExportTable(struct ipc_command *cmd) {
    get_uso *get = (get_uso*)cmd;

    s_engine->resetReusedResultOutputBuffer();
    int32_t tableSignatureLength = ntohl(get->tableSignatureLength);
    string tableSignature(get->tableSignature, tableSignatureLength);

    size_t ackOffset;
    int64_t seqNo;
    s_engine->getUSOForExportTable(ackOffset, seqNo, tableSignature);

    // write offset across bigendian.
    int64_t ackOffsetI64 = static_cast<int64_t>(ackOffset);
    ackOffsetI64 = htonll(ackOffsetI64);
    writeOrDie(&ackOffsetI64, sizeof(ackOffsetI64));

    // write the poll data. It is at least 4 bytes of length prefix.
    seqNo = htonll(seqNo);
    writeOrDie(&seqNo, sizeof(seqNo));
}

static void hashinate(struct ipc_command* cmd) {
    hashinate_msg* hash = (hashinate_msg*)cmd;
    NValueArray& params = s_engine->getParameterContainer();

    int32_t partCount = ntohl(hash->partitionCount);
    char* offset = hash->data;
    int sz = static_cast<int> (ntohl(cmd->msgsize) - sizeof(hash));

    int retval = -1;
    try {
        s_engine->deserializeParameterSet(offset, sz);
        retval = TheHashinator::hashinate(params[0], partCount);
        s_engine->purgeStringPool();
    } catch (FatalException e) {
        crashVoltDB(e);
    }

    char response[5];
    response[0] = kErrorCode_Success;
    *reinterpret_cast<int32_t*>(&response[1]) = htonl(retval);
    writeOrDie(response, 5);
}

/*
 * This is used by the signal dispatcher
 */
static VoltIPCTopend *currentVolt = NULL;

static void signalHandler(int signum, siginfo_t *info, void *context) {
    char err_msg[128];
    snprintf(err_msg, 128, "SIGSEGV caught: signal number %d, error value %d,"
             " signal code %d\n\n", info->si_signo, info->si_errno,
             info->si_code);
    string message = err_msg;
    if (s_engine) {
        message.append(s_engine->debug());
    }
    currentVolt->crashVoltDB(SegvException(message.c_str(), context, __FILE__, __LINE__));
}

static void signalDispatcher(int signum, siginfo_t *info, void *context) {
    if (currentVolt != NULL) {
        signalHandler(signum, info, context);
    }
}

static void setupSigHandler(void) const {
#if !defined(MEMCHECK)
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_sigaction = signalDispatcher;
    action.sa_flags = SA_SIGINFO;
    if(sigaction(SIGSEGV, &action, NULL) < 0)
        perror("Failed to setup signal handler for SIGSEGV");
#endif
}

static void threadLocalPoolAllocations() {
    size_t poolAllocations = ThreadLocalPool::getPoolAllocationSize();
    char response[9];
    response[0] = kErrorCode_Success;
    *reinterpret_cast<size_t*>(&response[1]) = htonll(poolAllocations);
    writeOrDie(response, 9);
}

int64_t KoltDBIPC::getQueuedExportBytes(int32_t partitionId, const string &signature) {
    s_reusedResultBuffer[0] = kErrorCode_getQueuedExportBytes;
    *reinterpret_cast<int32_t*>(&s_reusedResultBuffer[1]) = htonl(partitionId);
    *reinterpret_cast<int32_t*>(&s_reusedResultBuffer[5]) = htonl(static_cast<int32_t>(signature.size()));
    ::memcpy(&s_reusedResultBuffer[9], signature.c_str(), signature.size());
    writeOrDie(s_reusedResultBuffer, 9 + signature.size());

    int64_t netval;
    ssize_t bytes = read(s_fd, &netval, sizeof(int64_t));
    if (bytes != sizeof(int64_t)) {
        printf("Error - blocking read failed. %jd read %jd attempted",
                (intmax_t)bytes, (intmax_t)sizeof(int64_t));
        fflush(stdout);
        assert(false);
        exit(-1);
    }
    int64_t retval = ntohll(netval);
    return retval;
}

void KoltDBIPC::pushExportBuffer(
        int64_t exportGeneration,
        int32_t partitionId,
        const string &signature,
        StreamBlock *block,
        bool sync,
        bool endOfStream) {
    int32_t index = 0;
    s_reusedResultBuffer[index++] = kErrorCode_pushExportBuffer;
    *reinterpret_cast<int64_t*>(&s_reusedResultBuffer[index]) = htonll(exportGeneration);
    index += 8;
    *reinterpret_cast<int32_t*>(&s_reusedResultBuffer[index]) = htonl(partitionId);
    index += 4;
    *reinterpret_cast<int32_t*>(&s_reusedResultBuffer[index]) = htonl(static_cast<int32_t>(signature.size()));
    index += 4;
    ::memcpy( &s_reusedResultBuffer[index], signature.c_str(), signature.size());
    index += static_cast<int32_t>(signature.size());
    if (block != NULL) {
        *reinterpret_cast<int64_t*>(&s_reusedResultBuffer[index]) = htonll(block->uso());
    } else {
        *reinterpret_cast<int64_t*>(&s_reusedResultBuffer[index]) = 0;
    }
    index += 8;
    *reinterpret_cast<int8_t*>(&s_reusedResultBuffer[index++]) =
        sync ? static_cast<int8_t>(1) : static_cast<int8_t>(0);
    *reinterpret_cast<int8_t*>(&s_reusedResultBuffer[index++]) =
        endOfStream ? static_cast<int8_t>(1) : static_cast<int8_t>(0);
    if (block != NULL) {
        *reinterpret_cast<int32_t*>(&s_reusedResultBuffer[index]) = htonl(block->rawLength());
        writeOrDie(s_reusedResultBuffer, index + 4);
        writeOrDie(block->rawPtr(), block->rawLength());
    } else {
        *reinterpret_cast<int32_t*>(&s_reusedResultBuffer[index]) = htonl(0);
        writeOrDie(s_reusedResultBuffer, index + 4);
    }
    delete [] block->rawPtr();
}


class VoltIPCTopend : public Topend {
public:
    VoltIPCTopend() { }
    /**
     * Retrieve a dependency from Java via the IPC connection.
     * This method returns 0 if there are no more dependency tables.
     */
    int loadNextDependency(int32_t, Pool*, Table*);

    void crashVoltDB(FatalException e);

    int64_t getQueuedExportBytes(int32_t partitionId, const string& signature);
    void pushExportBuffer(int64_t exportGeneration, int32_t partitionId, const string &signature,
                          StreamBlock *block, bool sync, bool endOfStream);
    void fallbackToEEAllocatedBuffer(char *buffer, size_t length) {}
};

int main(int argc, char **argv) {
    //Create a pool ref to init the thread local in case a poll message comes early
    ThreadLocalPool poolRef;
    const int pid = getpid();
    printf("==%d==\n", pid);
    fflush(stdout);
    int sock = -1;
    int fd = -1;
    /* max message size that can be read from java */
    int max_ipc_message_size = (1024 * 1024 * 2);

    int port = 0;

    // allow called to override port with the first argument
    if (argc == 2) {
        char *portStr = argv[1];
        assert(portStr);
        port = atoi(portStr);
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = INADDR_ANY;


    // read args which presumably configure VoltDBIPC

    // and set up an accept socket.
    if ((sock = socket(AF_INET,SOCK_STREAM, 0)) < 0) {
        printf("Failed to create socket.\n");
        exit(-2);
    }

    if ((bind(sock, (struct sockaddr*) (&address), sizeof(struct sockaddr_in))) != 0) {
        printf("Failed to bind socket.\n");
        exit(-3);
    }

    socklen_t address_len = sizeof(struct sockaddr_in);
    if (getsockname( sock, reinterpret_cast<sockaddr*>(&address), &address_len)) {
        printf("Failed to find socket address\n");
        exit(-4);
    }

    port = ntohs(address.sin_port);
    printf("==%d==\n", port);
    fflush(stdout);

    if ((listen(sock, 1)) != 0) {
        printf("Failed to listen on socket.\n");
        exit(-5);
    }
    printf("listening\n");
    fflush(stdout);

    struct sockaddr_in client_addr;
    socklen_t addr_size = sizeof(struct sockaddr_in);
    s_fd = accept(sock, (struct sockaddr*) (&client_addr), &addr_size);
    if (s_fd < 0) {
        printf("Failed to accept socket.\n");
        exit(-6);
    }

    int flag = 1;
    int ret = setsockopt(s_fd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(flag) );
    if (ret == -1) {
      printf("Couldn't setsockopt(TCP_NODELAY)\n");
      exit( EXIT_FAILURE );
    }

    // requests larger than this will cause havoc.
    // cry havoc and let loose the dogs of war
    char* data = (char*) malloc(max_ipc_message_size);
    memset(data, 0, max_ipc_message_size);

    // instantiate voltdbipc to interface to EE.
    VoltIPCTopend topend();
    currentVolt = &topend;
    setupSigHandler();

    while (true) {
        size_t bytesread = 0;

        // read the header
        while (bytesread < 4) {
            size_t b = read(fd, data + bytesread, 4 - bytesread);
            if (b == 0) {
                printf("client eof\n");
                goto done;
            } else if (b == -1) {
                printf("client error\n");
                goto done;
            }
            bytesread += b;
        }

        // read the message body in to the same data buffer
        int msg_size = ntohl(((struct ipc_command*) data)->msgsize);
        //printf("Received message size %d\n", msg_size);
        if (msg_size > max_ipc_message_size) {
            max_ipc_message_size = msg_size;
            char* newdata = (char*) malloc(max_ipc_message_size);
            memset(newdata, 0, max_ipc_message_size);
            memcpy(newdata, data, 4);
            free(data);
            data = newdata;
        }

        while (bytesread < msg_size) {
            size_t b = read(fd, data + bytesread, msg_size - bytesread);
            if (b == 0) {
                printf("client eof\n");
                goto done;
            } else if (b == -1) {
                printf("client error\n");
                goto done;
            }
            bytesread += b;
        }

        // dispatch the request
        struct ipc_command *cmd = (struct ipc_command*) data;

        // size at least length + command
        if (ntohl(cmd->msgsize) < sizeof(struct ipc_command)) {
            printf("bytesread=%zx cmd=%d msgsize=%d\n",
                   bytesread, cmd->command, ntohl(cmd->msgsize));
            for (int ii = 0; ii < bytesread; ++ii) {
                printf("%x ", data[ii]);
            }
            assert(ntohl(cmd->msgsize) >= sizeof(struct ipc_command));
        }
        bool terminate = execute(cmd);
        if (terminate) {
            goto done;
        }
    }

  done:
    close(sock);
    close(s_fd);
    // Disable the engine's part in signal handling before deleting.
    VoltDBEngine *engine = s_engine;
    s_engine = NULL;
    delete engine; 
    currentVolt = NULL;
    delete [] s_reusedResultBuffer;
    delete [] s_exceptionBuffer;
    free(data);
    fflush(stdout);
    return 0;
}
