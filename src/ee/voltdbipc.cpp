/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
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
struct ipc_command
{
    int32_t msgsize;
    int32_t command;
}__attribute__((packed));

using namespace voltdb;

// must match ERRORCODE_SUCCESS|ERROR in ExecutionEngine.java
static const int8_t kErrorCode_None = -1;
static const int8_t kErrorCode_Success = 0;
static const int8_t kErrorCode_Error = 1;
/*
 * The following are not error codes but requests for information or functionality from Java.
 * These do not exist in ExecutionEngine.java since they are IPC specific.
 * These constants are mirrored in ExecutionEngineIPC.java.
 */
static const int8_t kErrorCode_RetrieveDependency = 100;   //Request for dependency
static const int8_t kErrorCode_DependencyFound = 101;      //Response to 100
static const int8_t kErrorCode_DependencyNotFound = 102;   //Also response to 100
static const int8_t kErrorCode_pushExportBuffer = 103;     //Indication that el buffer is next
static const int8_t kErrorCode_CrashVoltDB = 104;          //Crash with reason string
static const int8_t kErrorCode_getQueuedExportBytes = 105; //Retrieve value for stats

/*
 * This is used by the signal dispatcher and error paths to cleanly take down the Java driver.
 */
static Topend *s_currentTopEnd = NULL;

static VoltDBEngine *s_engine = NULL;
static int s_fd = -1;
static char s_reusedResultBuffer[MAX_MSG_SZ+1];//+1 for ipc's error code prefix.
static char s_exceptionBuffer[MAX_MSG_SZ];
static bool s_terminate = false;

static inline int64_t ntoh(const int64_t& datum) { return ntohll(datum); }
static inline int32_t ntoh(const int32_t& datum) { return ntohl(datum); }
static inline int16_t ntoh(const int16_t& datum) { return ntohs(datum); }

static inline int64_t hton(const size_t&  datum) { return static_cast<int64_t>(htonll(datum)); }
static inline int64_t hton(const int64_t& datum) { return htonll(datum); }
static inline int32_t hton(const int32_t& datum) { return htonl(datum); }
static inline int16_t hton(const int16_t& datum) { return htons(datum); }

// file static help function to do a blocking write.
// exit on a -1.. otherwise return when all bytes are written.
static void writeOrDie(const void* data, ssize_t sz)
{
    ssize_t written = 0;
    while (written < sz) {
        ssize_t last = write(s_fd, reinterpret_cast<const unsigned char *>(data) + written, sz - written);
        if (last < 0) {
            printf("\n\nIPC write to JNI returned -1. Exiting\n\n");
            fflush(stdout);
            exit(-1);
        }
        written += last;
    }
}


static void startSerializedResult(char byte)
{
    s_reusedResultBuffer[0] = byte;
}

template<typename T>
static inline size_t serializeResult(size_t position, const T& value)
{
    *reinterpret_cast<T*>(s_reusedResultBuffer+position) = hton(value);
    return position + sizeof(T);
}

template<typename T>
static inline size_t serializeStringResult(size_t position, const T& stringLength, const char* bytes)
{
    position = serializeResult(position, stringLength);
    memcpy(s_reusedResultBuffer+position, bytes, stringLength);
    return position + stringLength;
}

static inline size_t serializeByte(size_t position, char byte)
{
    s_reusedResultBuffer[position] = byte;
    return position + 1;
}

static void sendSerializedResult(size_t position)
{
    writeOrDie(s_reusedResultBuffer, position);
}

static void sendException()
{
    const int32_t* exceptionData = reinterpret_cast<const int32_t*>(s_exceptionBuffer);
    // The exception is serialized preceded by its normalized 4-byte length.
    // Denormalize the length and use it to size up the write of the length-prefixed exception.
    int32_t exceptionLength = ntoh(*exceptionData);
    printf("Sending exception length %d\n", exceptionLength);
    fflush(stdout);
    writeOrDie(&kErrorCode_Error, sizeof(kErrorCode_Error));
    size_t expectedSize = exceptionLength + sizeof(int32_t);
    writeOrDie(exceptionData, expectedSize);
}

inline static void sendEmptyException()
{
    // Write 1-byte generic error code with 4-byte exception length set to 0.
    static int8_t msg[5] = { kErrorCode_Error, 0, 0, 0, 0 };
    writeOrDie(msg, sizeof(msg));
}

inline static void sendErrorCode(int8_t result)
{
    writeOrDie(&result, 1);
}

template<typename T> inline static void sendSuccessWithNormalizedResult(const T& datum)
{
    s_reusedResultBuffer[0] = static_cast<char>(kErrorCode_Success);
    *(T*)(s_reusedResultBuffer+1) = hton(datum);
    writeOrDie(s_reusedResultBuffer, 1+sizeof(T));
}


static int8_t unexpected(ipc_command* cmd)
{
    printf("IPC command %d not implemented.\n", ntoh(cmd->command));
    fflush(stdout);
    return kErrorCode_Error;
}

static int8_t loadCatalog(ipc_command *cmd)
{
    struct cmd_structure
    {
        int64_t timestamp;
        char data[0];
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    //printf("loadCatalog\n");
    try {
        if (s_engine->loadCatalog(ntoh(cs->timestamp), string(cs->data))) {
            return kErrorCode_Success;
        }
    //TODO: FatalException and SerializableException should be universally caught and handled in "main",
    // rather than in hard-to-maintain "execute method" boilerplate code like this.
    } catch (SerializableEEException &e) {} //TODO: We don't really want to quietly SQUASH non-fatal exceptions.

    return kErrorCode_Error;
}

static int8_t updateCatalog(ipc_command *cmd)
{
    struct cmd_structure
    {
        int64_t timestamp;
        char data[];
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    if (s_engine->updateCatalog(ntoh(cs->timestamp), string(cs->data))) {
        return kErrorCode_Success;
    }
    return kErrorCode_Error;
}

static int8_t initialize(ipc_command *cmd)
{
    struct cmd_structure
    {
        int32_t clusterId;
        int64_t siteId;
        int32_t partitionId;
        int32_t hostId;
        int64_t logLevels;
        int64_t tempTableMemory;
        int32_t totalPartitions;
        int16_t hostnameLength;
        char hostname[0];
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    // expect a single initialization.
    assert(!s_engine);
    delete s_engine;

    // voltdbengine::initialize expects catalogids.
    assert(sizeof(CatalogId) == sizeof(int));
    int32_t clusterId = ntoh(cs->clusterId);
    int64_t siteId = ntoh(cs->siteId);
    printf("initialize: cluster=%d, site=%ld\n", (int)clusterId, (long)siteId);
    int32_t partitionId = ntoh(cs->partitionId);
    int32_t hostId = ntoh(cs->hostId);
    int16_t hostnameLength = ntoh(cs->hostnameLength);
    int64_t logLevels = ntoh(cs->logLevels);
    int64_t tempTableMemory = ntoh(cs->tempTableMemory);
    int32_t totalPartitions = ntoh(cs->totalPartitions);
    string hostname(cs->hostname, hostnameLength);

    // The engine has uses for the VoltDBIPCTopEnd singleton beyond what it is used for
    // in this module -- crashing and access to logging.
    // That's why VoltDBIPCTopEnd implements the Topend interface.
    s_engine = new VoltDBEngine(s_currentTopEnd);
    s_currentTopEnd->getLogManager().setLogLevels(logLevels);
    // offset by +1 reserves 1 byte for ipc to add its prefix code without disturbing the engine.
    s_engine->setBuffers(s_reusedResultBuffer+1, MAX_MSG_SZ, s_exceptionBuffer, MAX_MSG_SZ);
    if (s_engine->initialize(clusterId,
                             siteId,
                             partitionId,
                             hostId,
                             hostname,
                             tempTableMemory,
                             totalPartitions)) {
        return kErrorCode_Success;
    }
    return kErrorCode_Error;
}

static int8_t toggleProfiler(ipc_command *cmd)
{
    struct cmd_structure
    {
        int toggle;
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    printf("toggleProfiler: toggle=%d\n", ntoh(cs->toggle));

    // actually, the engine doesn't implement this now.
    // s_engine->ProfilerStart();
    return kErrorCode_Success;
}

static int8_t releaseUndoToken(ipc_command *cmd)
{
    struct cmd_structure
    {
        int64_t token;
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    s_engine->releaseUndoToken(ntoh(cs->token));
    return kErrorCode_Success;
}

static int8_t undoUndoToken(ipc_command *cmd)
{
    struct cmd_structure
    {
        int64_t token;
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    s_engine->undoUndoToken(ntoh(cs->token));
    return kErrorCode_Success;
}

static int8_t tick(ipc_command *cmd)
{
    struct cmd_structure
    {
        int64_t time;
        int64_t lastSpHandle;
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    // no return code. can't fail!
    s_engine->tick(ntoh(cs->time), ntoh(cs->lastSpHandle));
    return kErrorCode_Success;
}

static int8_t quiesce(ipc_command *cmd)
{
    struct cmd_structure
    {
        int64_t lastSpHandle;
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    s_engine->quiesce(ntoh(cs->lastSpHandle));
    return kErrorCode_Success;
}

static int8_t executePlanFragments(ipc_command *cmd)
{
    struct cmd_structure
    {
        int64_t spHandle;
        int64_t lastCommittedSpHandle;
        int64_t uniqueId;
        int64_t undoToken;
        int32_t numFragmentIds;
        char data[0];
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    int errors = 0;
    int32_t numFrags = ntoh(cs->numFragmentIds);

    // data has binary packed fragmentIds first
    int64_t *fragmentId = reinterpret_cast<int64_t*>(cs->data);
    // then, the same number of dependency ids
    int64_t *inputDepId = fragmentId + numFrags;
    // ...and fast serialized parameter set last.
    const char* parameterset = reinterpret_cast<const char*>(inputDepId + numFrags);
    long int usedsize = parameterset - reinterpret_cast<const char*>(cmd);
    int sz = static_cast<int>(ntoh(cmd->msgsize) - usedsize);

    if (0)
        cout << "executepfs:"
                  << " spHandle=" << ntoh(cs->spHandle)
                  << " lastCommittedSphandle=" << ntoh(cs->lastCommittedSpHandle)
                  << " uniqueId=" << ntoh(cs->uniqueId)
                  << " undoToken=" << ntoh(cs->undoToken)
                  << " numFragIds=" << numFrags << endl;

    s_engine->deserializeParameterSet(parameterset, sz);
    s_engine->resetReusedResultOutputBuffer();
    s_engine->setUndoToken(ntoh(cs->undoToken));
    for (int i = 0; i < numFrags; ++i) {
        if (s_engine->executeQuery(ntoh(fragmentId[i]),
                                   1,
                                   (int32_t)(ntoh(inputDepId[i])), // Java sends int64 but EE wants int32
                                   ntoh(cs->spHandle),
                                   ntoh(cs->lastCommittedSpHandle),
                                   ntoh(cs->uniqueId),
                                   i == 0 ? true : false, //first
                                   i == numFrags - 1 ? true : false)) { //last
            ++errors;
        }
    }
    s_engine->resizePlanCache(); // shrink cache if need be

    // write the results array back across the wire
    if (errors != 0) {
        sendException();
        return kErrorCode_None;
    }
    // write the results array back across the wire
    const int32_t size = s_engine->getResultsSize();
    s_reusedResultBuffer[0] = static_cast<char>(kErrorCode_Success);
    writeOrDie(s_reusedResultBuffer, size);
    return kErrorCode_None;
}

/**
 * Ensure a plan fragment is loaded.
 * Return error code, fragmentid for plan, and cache stats
 */
static int8_t loadFragment(ipc_command *cmd)
{
    struct cmd_structure
    {
        int32_t planFragLength;
        char data[0];
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    int32_t planFragLength = ntoh(cs->planFragLength);

    int64_t fragId;
    bool wasHit;
    int64_t cacheSize;

    if (s_engine->loadFragment(cs->data, planFragLength, fragId, wasHit, cacheSize)) {
        sendException();
        return kErrorCode_None;
    }

    // make network suitable
    fragId = hton(fragId);
    int64_t wasHitLong = hton((wasHit ? (int64_t)1 : (int64_t)0));
    cacheSize = hton(cacheSize);

    // write the results array back across the wire
    const int8_t successResult = kErrorCode_Success;
    writeOrDie(&successResult, sizeof(int8_t));
    writeOrDie(&fragId, sizeof(int64_t));
    writeOrDie(&wasHitLong, sizeof(int64_t));
    writeOrDie(&cacheSize, sizeof(int64_t));
    return kErrorCode_None;
}

static int8_t loadTable(ipc_command *cmd)
{
    struct cmd_structure
    {
        int32_t tableId;
        int64_t spHandle;
        int64_t lastCommittedSpHandle;
        char data[0];
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    const int32_t tableId = ntoh(cs->tableId);
    const int64_t spHandle = ntoh(cs->spHandle);
    const int64_t lastCommittedSpHandle = ntoh(cs->lastCommittedSpHandle);
    if (0) {
        cout << "loadTable:" << " tableId=" << tableId
                  << " spHandle=" << spHandle << " lastCommittedSpHandle=" << lastCommittedSpHandle << endl;
    }

    // ...and fast serialized table last.
    const char* tableData = cs->data;
    int sz = static_cast<int>(ntoh(cmd->msgsize) - sizeof(cmd_structure));
    ReferenceSerializeInput serialize_in(tableData, sz);
    if (s_engine->loadTable(tableId, serialize_in, spHandle, lastCommittedSpHandle)) {
        return kErrorCode_Success;
    }
    return kErrorCode_Error;
}

static int8_t setLogLevels(ipc_command *cmd)
{
    struct cmd_structure
    {
        int64_t logLevels;
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    if (s_currentTopEnd == NULL) {
        return kErrorCode_Error;
    }
    int64_t logLevels = ntoh(cs->logLevels);
    s_currentTopEnd->getLogManager().setLogLevels(logLevels);
    return kErrorCode_Success;
}

static int8_t getStats(struct ipc_command *cmd)
{
    struct cmd_structure
    {
        int32_t selector;
        int8_t  interval;
        int64_t now;
        int32_t num_locators;
        int32_t locators[0];
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    const int32_t selector = ntoh(cs->selector);
    const int32_t numLocators = ntoh(cs->num_locators);
    bool interval = cs->interval != 0;
    const int64_t now = ntoh(cs->now);
    int32_t locators[numLocators];
    for (int ii = 0; ii < numLocators; ii++) {
        locators[ii] = ntoh(cs->locators[ii]);
    }

    if (s_engine->getStats(static_cast<int>(selector), locators, numLocators, interval, now) != 1) {
        sendException();
        return kErrorCode_None;
    }
    s_reusedResultBuffer[0] = static_cast<char>(kErrorCode_Success);
    // write the results array back across the wire
    // the result set includes the total serialization size
    const int32_t size = s_engine->getResultsSize();
    writeOrDie(s_reusedResultBuffer, size+1);
    return kErrorCode_None;
}

static int8_t activateTableStream(ipc_command *cmd)
{
    struct cmd_structure
    {
        CatalogId tableId;
        TableStreamType streamType;
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    const CatalogId tableId = ntoh(cs->tableId);
    const TableStreamType streamType = static_cast<TableStreamType>(ntoh(cs->streamType));
    if (s_engine->activateTableStream(tableId, streamType)) {
        return kErrorCode_Success;
    }
    return kErrorCode_Error;
}

static int8_t tableStreamSerializeMore(ipc_command *cmd)
{
    struct cmd_structure
    {
        CatalogId tableId;
        TableStreamType streamType;
        int bufferSize;
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    const CatalogId tableId = ntoh(cs->tableId);
    const TableStreamType streamType = static_cast<TableStreamType>(ntoh(cs->streamType));
    const int bufferLength = ntoh(cs->bufferSize);
    assert(bufferLength + 5 < MAX_MSG_SZ);

    if (bufferLength + 5 >= MAX_MSG_SZ) {
        return kErrorCode_Error;
    }

    startSerializedResult(static_cast<char>(kErrorCode_Success));
    ReferenceSerializeOutput out(s_reusedResultBuffer + 5, bufferLength);
    int32_t serialized = s_engine->tableStreamSerializeMore(&out, tableId, streamType);
    serializeResult(1, serialized);

    /*
     * Already put the -1 code into the message.
     * Set it to 0 so toWrite has the correct number of bytes
     */
    if (serialized == -1) {
        serialized = 0;
    }
    const ssize_t toWrite = serialized + 5;
    sendSerializedResult(toWrite);
    return kErrorCode_None;
}

static int8_t recoveryMessage(ipc_command *cmd)
{
    struct cmd_structure
    {
        int32_t messageLength;
        char message[0];
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    const int32_t messageLength = ntoh(cs->messageLength);
    ReferenceSerializeInput input(cs->message, messageLength);
    RecoveryProtoMsg message(&input);
    s_engine->processRecoveryMessage(&message);
    return kErrorCode_Success;
}

static int8_t tableHashCode(ipc_command *cmd)
{
    struct cmd_structure
    {
        int32_t tableId;
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    const int32_t tableId = ntoh(cs->tableId);
    int64_t tableHashCode = s_engine->tableHashCode(tableId);
    sendSuccessWithNormalizedResult(tableHashCode);
    return kErrorCode_None;
}

static int8_t exportAction(ipc_command *cmd)
{
    struct cmd_structure
    {
        int32_t isSync;
        int64_t offset;
        int64_t seqNo;
        int32_t tableSignatureLength;
        char tableSignature[0];
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    s_engine->resetReusedResultOutputBuffer();
    int32_t tableSignatureLength = ntoh(cs->tableSignatureLength);
    string tableSignature(cs->tableSignature, tableSignatureLength);
    int64_t result = s_engine->exportAction(ntoh(cs->isSync),
                                            ntoh(cs->offset),
                                            ntoh(cs->seqNo),
                                            tableSignature);

    // write offset across bigendian.
    result = hton(result);
    writeOrDie(&result, sizeof(result));
    return kErrorCode_None;
}

static int8_t getUSOs(ipc_command *cmd)
{
    struct cmd_structure
    {
        int32_t tableSignatureLength;
        char tableSignature[0];
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    s_engine->resetReusedResultOutputBuffer();
    int32_t tableSignatureLength = ntoh(cs->tableSignatureLength);
    string tableSignature(cs->tableSignature, tableSignatureLength);

    size_t ackOffset;
    int64_t seqNo;
    s_engine->getUSOForExportTable(ackOffset, seqNo, tableSignature);

    // write offset across bigendian.
    int64_t ackOffsetI64 = static_cast<int64_t>(ackOffset);
    ackOffsetI64 = hton(ackOffsetI64);
    writeOrDie(&ackOffsetI64, sizeof(ackOffsetI64));

    // write the poll data. It is at least 4 bytes of length prefix.
    seqNo = hton(seqNo);
    writeOrDie(&seqNo, sizeof(seqNo));
    return kErrorCode_None;
}

static int8_t hashinate(ipc_command* cmd)
{
    struct cmd_structure
    {
        int32_t partitionCount;
        char data[0];
    }__attribute__((packed));
    cmd_structure* cs = reinterpret_cast<cmd_structure*>(cmd+1);

    int32_t partitionCount = ntoh(cs->partitionCount);
    char* paramData = cs->data;
    int sz = static_cast<int> (ntoh(cmd->msgsize) - sizeof(cmd_structure));

    s_engine->deserializeParameterSet(paramData, sz);
    int retval = s_engine->hashinate(partitionCount);

    char response[5];
    response[0] = kErrorCode_Success;
    *reinterpret_cast<int32_t*>(&response[1]) = hton(retval);
    writeOrDie(response, 5);
    return kErrorCode_None;
}

static int8_t getPoolAllocations(ipc_command* cmd)
{
    size_t poolAllocations = ThreadLocalPool::getPoolAllocationSize();
    sendSuccessWithNormalizedResult(poolAllocations);
    return kErrorCode_None;
}


static void signalHandler(int signum, siginfo_t *info, void *context) {
    char err_msg[128];
    snprintf(err_msg, 128, "SIGSEGV caught: signal number %d, error value %d,"
             " signal code %d\n\n", info->si_signo, info->si_errno,
             info->si_code);
    string message = err_msg;
    if (s_engine) {
        message.append(s_engine->debug());
    }
    s_currentTopEnd->crashVoltDB(SegvException(message.c_str(), context, __FILE__, __LINE__));
}

static void signalDispatcher(int signum, siginfo_t *info, void *context)
{
    if (s_currentTopEnd != NULL) {
        signalHandler(signum, info, context);
    }
}

static void setupSigHandler(void)
{
#if !defined(MEMCHECK)
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_sigaction = signalDispatcher;
    action.sa_flags = SA_SIGINFO;
    if(sigaction(SIGSEGV, &action, NULL) < 0)
        perror("Failed to setup signal handler for SIGSEGV");
#endif
}

class VoltDBIPCTopEnd : public Topend {
public:
    VoltDBIPCTopEnd() : Topend(new StdoutLogProxy()) { }
    /**
     * Retrieve a dependency from Java via the IPC connection.
     * This method returns 0 if there are no more dependency tables.
     */
    int loadNextDependency(int32_t dependencyId, Pool* stringPool, Table* destination)
    {
        VOLT_DEBUG("iterating java dependency for id %d\n", dependencyId);
        // tell java to send the dependency over the socket
        startSerializedResult(static_cast<char>(kErrorCode_RetrieveDependency));
        size_t position = 1;
        position = serializeResult(position, dependencyId);
        sendSerializedResult(position);

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
        bytes = read(s_fd, &dependencyLength, sizeof(dependencyLength));
        if (bytes != sizeof(dependencyLength)) {
            printf("Error - blocking read failed. %jd read %jd attempted",
                    (intmax_t)bytes, (intmax_t)sizeof(dependencyLength));
            fflush(stdout);
            assert(false);
            exit(-1);
        }

        bytes = 0;
        dependencyLength = ntoh(dependencyLength);
        if (dependencyLength == 0) {
            return 0;
        }
        char dependencyData[dependencyLength];
        while (bytes != dependencyLength) {
            ssize_t oldBytes = bytes;
            bytes += read(s_fd, dependencyData + bytes, dependencyLength - bytes);
            if (oldBytes == bytes) {
                break;
            }
            if (oldBytes > bytes) {
                // read failed with -1 return, so +1 to compensate when reporting bytes read
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

        ReferenceSerializeInput serialize_in(dependencyData, (size_t)dependencyLength);
        destination->loadTuplesFrom(serialize_in, stringPool);
        return 1;
    }

    void crashVoltDB(const FatalException& e)
    {
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
        startSerializedResult(static_cast<char>(kErrorCode_CrashVoltDB));
        size_t position = 1;

        //overall message length, not included in messageLength
        position = serializeResult(position, messageLength);
        position = serializeStringResult(position, reasonLength, reasonBytes);
        position = serializeStringResult(position, filenameLength, e.m_filename);
        position = serializeResult(position, lineno);
        position = serializeResult(position, numTraces);

        for (int32_t ii = 0; ii < numTraces; ii++) {
            const char* trace = e.m_traces[ii].c_str();
            int32_t traceLength = static_cast<int32_t>(strlen(trace));
            position = serializeStringResult(position, traceLength, trace);
        }
        assert( position == 5 + messageLength);
        sendSerializedResult(position);
        exit(-1);
    }


    int64_t getQueuedExportBytes(int32_t partitionId, const string &signature)
    {
        startSerializedResult(static_cast<char>(kErrorCode_getQueuedExportBytes));
        size_t position = 1;
        position = serializeResult(position, partitionId);
        position = serializeStringResult(position, static_cast<int32_t>(signature.size()), signature.c_str());
        sendSerializedResult(position);

        int64_t netval;
        ssize_t bytes = read(s_fd, &netval, sizeof(int64_t));
        if (bytes != sizeof(int64_t)) {
            printf("Error - blocking read failed. %jd read %jd attempted",
                    (intmax_t)bytes, (intmax_t)sizeof(int64_t));
            fflush(stdout);
            assert(false);
            exit(-1);
        }
        int64_t retval = ntoh(netval);
        return retval;
    }

    void pushExportBuffer(int64_t exportGeneration, int32_t partitionId, const string &signature,
                          StreamBlock *block, bool sync, bool endOfStream)
    {
        startSerializedResult(static_cast<char>(kErrorCode_pushExportBuffer));
        size_t position = 1;
        position = serializeResult(position, exportGeneration);
        position = serializeResult(position, partitionId);
        position = serializeStringResult(position, static_cast<int32_t>(signature.size()), signature.c_str());
        int64_t uso = 0;
        int32_t rawLength = 0;
        if (block) {
            uso = static_cast<int64_t>(block->uso());
            rawLength = block->rawLength();
        }
        position = serializeResult(position, uso);
        position = serializeByte(position, sync ? static_cast<char>(1) : static_cast<char>(0));
        position = serializeByte(position, endOfStream ? static_cast<char>(1) : static_cast<char>(0));
        position = serializeResult(position, rawLength);
        sendSerializedResult(position);
        if (block == NULL) {
            return;
        }
        writeOrDie(block->rawPtr(), block->rawLength());
        delete [] block->rawPtr();
    }

    void fallbackToEEAllocatedBuffer(char *buffer, size_t length) { /* Do nothing */ }
};


int main(int argc, char **argv) {
    //Create a pool ref to init the thread local in case a poll message comes early
    ThreadLocalPool poolRef;
    const int pid = getpid();
    printf("==%d==\n", pid);
    fflush(stdout);
    int sock = -1;
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


    // read args which presumably configure the VoltDBIPCTopEnd and related statics in this module

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

    port = ntoh(address.sin_port);
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
    VoltDBIPCTopEnd topend;
    s_currentTopEnd = &topend;
    setupSigHandler();

    while (true) {
        size_t bytesread = 0;

        // read the header
        while (bytesread < 4) {
            size_t b = read(s_fd, data + bytesread, 4 - bytesread);
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
        int msg_size = ntoh(((ipc_command*) data)->msgsize);
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
            size_t b = read(s_fd, data + bytesread, msg_size - bytesread);
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
        ipc_command *cmd = reinterpret_cast<ipc_command*>(data);

        // size at least length + command
        if (ntoh(cmd->msgsize) < sizeof(ipc_command)) {
            printf("bytesread=%zx cmd=%d msgsize=%d\n",
                   bytesread, cmd->command, (int)ntoh(cmd->msgsize));
            for (int ii = 0; ii < bytesread; ++ii) {
                printf("%x ", data[ii]);
            }
            assert(ntoh(cmd->msgsize) >= sizeof(ipc_command));
        }

        if (0) {
            cout << "IPC client command: " << ntoh(cmd->command) << endl;
        }

        typedef int8_t (*dispatchable)(ipc_command*);
        static const dispatchable method[] = {
                // commands must match java's ExecutionEngineIPC.Command
                // could enumerate but they're only used as offsets in this one place.
                initialize,               //      Initialize(0),
                unexpected,               //      // (1),
                loadCatalog,              //      LoadCatalog(2),
                toggleProfiler,           //      ToggleProfiler(3),
                tick,                     //      Tick(4),
                getStats,                 //      GetStats(5),
                executePlanFragments,     //      ExecutePlanFragments(6),
                unexpected,               //      // (7),
                unexpected,               //      // (8),
                loadTable,                //      LoadTable(9),
                releaseUndoToken,         //      ReleaseUndoToken(10),
                undoUndoToken,            //      UndoUndoToken(11),
                unexpected,               //      // (12),
                setLogLevels,             //      SetLogLevels(13),
                unexpected,               //      // (14),
                unexpected,               //      // (15),
                quiesce,                  //      Quiesce(16),
                activateTableStream,      //      ActivateTableStream(17),
                tableStreamSerializeMore, //      TableStreamSerializeMore(18),
                updateCatalog,            //      UpdateCatalog(19),
                exportAction,             //      ExportAction(20),
                recoveryMessage,          //      RecoveryMessage(21),
                tableHashCode,            //      TableHashCode(22),
                hashinate,                //      Hashinate(23),
                getPoolAllocations,       //      GetPoolAllocations(24),
                getUSOs,                  //      GetUSOs(25),
                loadFragment,             //      LoadFragment(26),
            };

        int8_t result;
        int command = ntoh(cmd->command);
        if (command < 0 || command >= (sizeof(method) / sizeof(dispatchable))) {
            result = unexpected(cmd);
        } else {
            try {
                assert(s_engine == NULL || command == 0);
                if (s_engine != NULL && command != 0) {
                    result = kErrorCode_Error;
                } else {
                    result = (*( method[command] ))(cmd);
                }
            } catch (FatalException e) {
                s_currentTopEnd->crashVoltDB(e);
            }
        }

        // Write results for the simple commands.
        // More complex commands write directly in the command implementation.
        if (result == kErrorCode_Error) {
            sendEmptyException();
        } else if (result != kErrorCode_None) {
            sendErrorCode(result);
        }

        if (s_terminate) {
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
    s_currentTopEnd = NULL;
    free(data);
    fflush(stdout);
    return 0;
}
