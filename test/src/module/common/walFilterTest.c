/***********************************************************************************************************************************
Test wal filter
***********************************************************************************************************************************/

#include "command/archive/get/file.h"
#include "common/harnessConfig.h"
#include "common/harnessInfo.h"
#include "common/harnessPostgres.h"
#include "common/harnessStorage.h"
#include "common/harnessWal.h"
#include "common/io/bufferRead.h"
#include "common/io/io.h"
#include "common/partialRestore.h"
#include "common/type/json.h"
#include "common/walFilter/versions/recordProcessGPDB6.h"
#include "info/infoArchive.h"
#include "postgres/interface/crc32.h"

typedef enum WalFlags
{
    NO_SWITCH_WAL = 1 << 0,
} WalFlags;

typedef struct XRecordInfo
{
    uint8_t rmid;
    uint8_t info;
    uint32_t body_size;
    void *body;
} XRecordInfo;

static Buffer *
testFilter(IoFilter *filter, Buffer *wal, size_t inputSize, size_t outputSize)
{
    Buffer *filtred = bufNew(1024 * 1024);
    Buffer *output = bufNew(outputSize);
    ioBufferSizeSet(inputSize);

    IoRead *read = ioBufferReadNew(wal);
    ioFilterGroupAdd(ioReadFilterGroup(read), filter);
    ioReadOpen(read);

    while (!ioReadEof(read))
    {
        ioRead(read, output);
        bufCat(filtred, output);
        bufUsedZero(output);
    }

    ioReadClose(read);
    bufFree(output);
    ioFilterFree(filter);

    return filtred;
}

static inline List *
emptyFilterList(void)
{
    return buildFilterList(jsonReadNew(STRDEF("[]")));
}

static void
build_wal(Buffer *wal, XRecordInfo *records, size_t count, WalFlags flags)
{
    for (size_t i = 0; i < count; ++i)
    {
        XLogRecord *record = hrnGpdbCreateXRecord(records[i].rmid, records[i].info, records[i].body_size, records[i].body);
        hrnGpdbWalInsertXRecordSimple(wal, record);
    }
    if (!(flags & NO_SWITCH_WAL))
    {
        XLogRecord *record = hrnGpdbCreateXRecord(0, XLOG_SWITCH, 0, NULL);
        hrnGpdbWalInsertXRecordSimple(wal, record);
        size_t to_write = XLOG_BLCKSZ - bufUsed(wal) % XLOG_BLCKSZ;
        memset(bufRemainsPtr(wal), 0, to_write);
        bufUsedInc(wal, to_write);
    }
}

static void
fill_last_page(Buffer *wal)
{
    if (bufUsed(wal) % XLOG_BLCKSZ == 0)
    {
        return;
    }
    size_t to_write = XLOG_BLCKSZ - bufUsed(wal) % XLOG_BLCKSZ;
    memset(bufRemainsPtr(wal), 0, to_write);
    bufUsedInc(wal, to_write);
}

static void
test_get_relfilenode(uint8_t rmid, uint8_t info, bool expect_not_skip)
{
    RelFileNode node = {1, 2, 3};

    XLogRecord *record = hrnGpdbCreateXRecord(rmid, info, sizeof(node), &node);

    RelFileNode *node_result = NULL;
    RelFileNode node_expect = {1, 2, 3};
    TEST_RESULT_BOOL(getRelFileNodeGPDB6(record, &node_result), expect_not_skip, "RelFileNode is different from expected");
    if (expect_not_skip)
        TEST_RESULT_BOOL(memcmp(&node_expect, node_result, sizeof(RelFileNode)), 0, "RelFileNode is different from expected");
    else
    {
        TEST_RESULT_PTR(node_result, NULL, "node_result is not empty");
    }
    memFree(record);
}

/***********************************************************************************************************************************
Test Run
***********************************************************************************************************************************/
static void
testRun(void)
{
    FUNCTION_HARNESS_VOID();
    Buffer *wal;
    IoFilter *filter;
    Buffer *result;
    XLogRecord *record;

    if (testBegin("read begin of the record from prev file"))
    {
        Buffer *wal2;
        StringList *argBaseList = strLstNew();
        hrnCfgArgRawZ(argBaseList, cfgOptFork, CFGOPTVAL_FORK_GPDB_Z);
        hrnCfgArgRawZ(argBaseList, cfgOptPgPath, TEST_PATH "/pg");
        hrnCfgArgRawZ(argBaseList, cfgOptRepoPath, TEST_PATH "/repo");
        hrnCfgArgRawZ(argBaseList, cfgOptStanza, "test1");
        HRN_CFG_LOAD(cfgCmdArchiveGet, argBaseList);

        HRN_PG_CONTROL_OVERRIDE_VERSION_PUT(
            storagePgWrite(), PG_VERSION_94, 9420600, .systemId = HRN_PG_SYSTEMID_94, .catalogVersion = 301908232,
            .pageSize = 32768, .walSegmentSize = 64 * 1024 * 1024);

        HRN_INFO_PUT(
            storageRepoWrite(), INFO_ARCHIVE_PATH_FILE,
            "[db]\n"
            "db-id=1\n"
            "db-system-id=7374327172765320188\n"
            "db-version=\"9.4\"\n"
            "\n"
            "[db:history]\n"
            "1={\"db-id\":" HRN_PG_SYSTEMID_94_Z ",\"db-version\":\"9.4\"}");

        HRN_STORAGE_PATH_CREATE(storageRepoIdxWrite(0), STORAGE_REPO_ARCHIVE "/9.4-1");
        ArchiveGetFile archiveInfo = {
            .file = STRDEF(
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000002-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd"),
            .repoIdx = 0,
            .archiveId = STRDEF("9.4-1"),
            .cipherType = cipherTypeNone,
            .cipherPassArchive = STRDEF("")
        };

        TEST_TITLE("simple read begin from prev file");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());
        {
            Buffer *wal1 = bufNew(32768);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32688, NULL);
            hrnGpdbWalInsertXRecordSimple(wal1, record);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordP(wal1, record, INCOMPLETE_RECORD);
            fill_last_page(wal1);

            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                wal1);
        }

        wal2 = bufNew(1024 * 1024);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, NO_FLAGS, .segno = 2, .begin_offset = 132 - 8);
        record = hrnGpdbCreateXRecord(0, XLOG_SWITCH, 0, NULL);
        hrnGpdbWalInsertXRecordSimple(wal2, record);

        fill_last_page(wal2);
        result = testFilter(filter, wal2, bufSize(wal2), bufSize(wal2));
        TEST_RESULT_BOOL(bufEq(wal2, result), true, "WAL not the same");
        bufFree(wal2);
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");

        TEST_TITLE("incomplete record in the beginning of prev file");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());
        {
            Buffer *wal1 = bufNew(32768);
            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordP(wal1, record, NO_FLAGS, .begin_offset = 132 - 8);
            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32560, NULL);
            hrnGpdbWalInsertXRecordSimple(wal1, record);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordP(wal1, record, INCOMPLETE_RECORD);
            fill_last_page(wal1);

            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                wal1);
        }
        wal2 = bufNew(1024 * 1024);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, NO_FLAGS, .segno = 2, .begin_offset = 132 - 8);
        record = hrnGpdbCreateXRecord(0, XLOG_SWITCH, 0, NULL);
        hrnGpdbWalInsertXRecordSimple(wal2, record);

        fill_last_page(wal2);
        result = testFilter(filter, wal2, bufSize(wal2), bufSize(wal2));
        TEST_RESULT_BOOL(bufEq(wal2, result), true, "WAL not the same");
        bufFree(wal2);
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");

        TEST_TITLE("override record in the beginning of prev file");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());
        {
            Buffer *wal1 = bufNew(32768);
            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32688, NULL);
            hrnGpdbWalInsertXRecordP(wal1, record, OVERWRITE, .begin_offset = 100);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordP(wal1, record, INCOMPLETE_RECORD);
            fill_last_page(wal1);

            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                wal1);
        }
        wal2 = bufNew(1024 * 1024);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, NO_FLAGS, .segno = 2, .begin_offset = 132 - 8);
        record = hrnGpdbCreateXRecord(0, XLOG_SWITCH, 0, NULL);
        hrnGpdbWalInsertXRecordSimple(wal2, record);

        fill_last_page(wal2);
        result = testFilter(filter, wal2, bufSize(wal2), bufSize(wal2));
        TEST_RESULT_BOOL(bufEq(wal2, result), true, "WAL not the same");
        bufFree(wal2);
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");

        TEST_TITLE("no prev file");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());
        {
            Buffer *zeros = bufNew(32768);
            memset(bufPtr(zeros), 0, bufUsed(zeros));
            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000033-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                zeros);
        }

        wal2 = bufNew(1024 * 1024);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, NO_FLAGS, .segno = 2, .begin_offset = 132 - 8);
        record = hrnGpdbCreateXRecord(0, XLOG_SWITCH, 0, NULL);
        hrnGpdbWalInsertXRecordSimple(wal2, record);
        fill_last_page(wal2);
        result = testFilter(filter, wal2, bufSize(wal2), bufSize(wal2));
        TEST_RESULT_BOOL(bufEq(wal2, result), true, "WAL not the same");
        bufFree(wal2);
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000033-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");

        TEST_TITLE("no WAL files in repository");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());

        wal2 = bufNew(1024 * 1024);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, INCOMPLETE_RECORD, .segno = 2, .begin_offset = 132 - 8);
        record = hrnGpdbCreateXRecord(0, XLOG_SWITCH, 0, NULL);
        hrnGpdbWalInsertXRecordSimple(wal2, record);

        fill_last_page(wal2);
        TEST_ERROR(testFilter(
                       filter,
                       wal2,
                       bufSize(wal2),
                       bufSize(wal2)),
                   FormatError,
                   "no WAL files were found in the repository");
        bufFree(wal2);

        TEST_TITLE("usefully part of the record in prev file");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());
        {
            Buffer *wal1 = bufNew(32768);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32680, NULL);
            hrnGpdbWalInsertXRecordSimple(wal1, record);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordP(wal1, record, INCOMPLETE_RECORD);
            fill_last_page(wal1);

            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                wal1);
        }

        wal2 = bufNew(1024 * 1024);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, NO_FLAGS, .segno = 2, .begin_offset = 132 - 16);
        record = hrnGpdbCreateXRecord(0, XLOG_SWITCH, 0, NULL);
        hrnGpdbWalInsertXRecordSimple(wal2, record);

        fill_last_page(wal2);
        result = testFilter(filter, wal2, bufSize(wal2), bufSize(wal2));
        TEST_RESULT_BOOL(bufEq(wal2, result), true, "WAL not the same");
        bufFree(wal2);
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");

        TEST_TITLE("record is too big");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());
        {
            Buffer *wal1 = bufNew(32768);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32688, NULL);
            hrnGpdbWalInsertXRecordSimple(wal1, record);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 65536, NULL);
            hrnGpdbWalInsertXRecordP(wal1, record, INCOMPLETE_RECORD);
            fill_last_page(wal1);

            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                wal1);
        }

        wal2 = bufNew(1024 * 1024);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 65536, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, 0, .segno = 2, .begin_offset = 65568  - 8);
        record = hrnGpdbCreateXRecord(0, XLOG_SWITCH, 0, NULL);
        hrnGpdbWalInsertXRecordSimple(wal2, record);

        fill_last_page(wal2);
        TEST_ERROR(testFilter(filter, wal2, 32768, 32768), FormatError, "record is too big");
        bufFree(wal2);
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");

        TEST_TITLE("multiply WAL files");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());
        {
            Buffer *wal1 = bufNew(32768);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32688, NULL);
            hrnGpdbWalInsertXRecordSimple(wal1, record);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordP(wal1, record, INCOMPLETE_RECORD);
            fill_last_page(wal1);

            Buffer *zeros = bufNew(32768);
            memset(bufPtr(zeros), 0, bufUsed(zeros));

            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000033-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                zeros);
            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                wal1);
            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000040-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                zeros);
        }

        wal2 = bufNew(1024 * 1024);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, 0, .segno = 2, .begin_offset = 132 - 8);
        record = hrnGpdbCreateXRecord(0, XLOG_SWITCH, 0, NULL);
        hrnGpdbWalInsertXRecordSimple(wal2, record);

        fill_last_page(wal2);
        result = testFilter(filter, wal2, bufSize(wal2), bufSize(wal2));
        TEST_RESULT_BOOL(bufEq(wal2, result), true, "WAL not the same");
        bufFree(wal2);
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000033-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000040-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");

        TEST_TITLE("compressed and encrypted WAL file");
        HRN_STORAGE_PATH_REMOVE(storageRepoWrite(), STORAGE_REPO_ARCHIVE, .recurse = true);
        HRN_INFO_PUT(
            storageRepoWrite(), INFO_ARCHIVE_PATH_FILE,
            "[cipher]\n"
            "cipher-pass=\"" TEST_CIPHER_PASS_ARCHIVE "\"\n"
            "[db]\n"
            "db-id=1\n"
            "db-system-id=7374327172765320188\n"
            "db-version=\"9.4\"\n"
            "\n"
            "[db:history]\n"
            "1={\"db-id\":" HRN_PG_SYSTEMID_94_Z ",\"db-version\":\"9.4\"}",
            .cipherType = cipherTypeAes256Cbc);

        hrnCfgArgRawStrId(argBaseList, cfgOptRepoCipherType,  cipherTypeAes256Cbc);
        hrnCfgEnvRawZ(cfgOptRepoCipherPass, TEST_CIPHER_PASS);
        HRN_CFG_LOAD(cfgCmdArchiveGet, argBaseList);

        archiveInfo.cipherType = cipherTypeAes256Cbc;
        archiveInfo.cipherPassArchive = STRDEF(TEST_CIPHER_PASS_ARCHIVE);

        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());
        {
            Buffer *wal1 = bufNew(32768);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32688, NULL);
            hrnGpdbWalInsertXRecordSimple(wal1, record);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordP(wal1, record, INCOMPLETE_RECORD);
            fill_last_page(wal1);

            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                wal1,
                .compressType = compressTypeGz,
                .cipherType = cipherTypeAes256Cbc,
                .cipherPass = TEST_CIPHER_PASS_ARCHIVE);
        }

        wal2 = bufNew(1024 * 1024);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, 0, .segno = 2, .begin_offset = 132 - 8);
        record = hrnGpdbCreateXRecord(0, XLOG_SWITCH, 0, NULL);
        hrnGpdbWalInsertXRecordSimple(wal2, record);

        fill_last_page(wal2);
        result = testFilter(filter, wal2, bufSize(wal2), bufSize(wal2));
        TEST_RESULT_BOOL(bufEq(wal2, result), true, "WAL not the same");
        bufFree(wal2);
        HRN_STORAGE_PATH_REMOVE(storageRepoWrite(), STORAGE_REPO_ARCHIVE, .recurse = true);
        hrnCfgEnvRemoveRaw(cfgOptRepoCipherPass);
    }

    if (testBegin("read end of the record from next file"))
    {
        Buffer *wal2;
        StringList *argBaseList = strLstNew();
        hrnCfgArgRawZ(argBaseList, cfgOptFork, CFGOPTVAL_FORK_GPDB_Z);
        hrnCfgArgRawZ(argBaseList, cfgOptPgPath, TEST_PATH "/pg");
        hrnCfgArgRawZ(argBaseList, cfgOptRepoPath, TEST_PATH "/repo");
        hrnCfgArgRawZ(argBaseList, cfgOptStanza, "test1");
        HRN_CFG_LOAD(cfgCmdArchiveGet, argBaseList);

        HRN_PG_CONTROL_OVERRIDE_VERSION_PUT(
            storagePgWrite(), PG_VERSION_94, 9420600, .systemId = HRN_PG_SYSTEMID_94, .catalogVersion = 301908232,
            .pageSize = 32768, .walSegmentSize = 64 * 1024 * 1024);

        HRN_INFO_PUT(
            storageRepoWrite(), INFO_ARCHIVE_PATH_FILE,
            "[db]\n"
            "db-id=1\n"
            "db-system-id=7374327172765320188\n"
            "db-version=\"9.4\"\n"
            "\n"
            "[db:history]\n"
            "1={\"db-id\":" HRN_PG_SYSTEMID_94_Z ",\"db-version\":\"9.4\"}");

        HRN_STORAGE_PATH_CREATE(storageRepoIdxWrite(0), STORAGE_REPO_ARCHIVE "/9.4-1");
        ArchiveGetFile archiveInfo = {
            .file = STRDEF(
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd"),
            .repoIdx = 0,
            .archiveId = STRDEF("9.4-1"),
            .cipherType = cipherTypeNone,
            .cipherPassArchive = STRDEF("")
        };

        TEST_TITLE("simple read end from next file");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());
        {
            Buffer *wal1 = bufNew(32768);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordP(wal1, record, 0, .begin_offset = 100);
            fill_last_page(wal1);

            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000002-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                wal1);
        }

        wal2 = bufNew(1024 * 1024);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32664, NULL);
        hrnGpdbWalInsertXRecordSimple(wal2, record);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, INCOMPLETE_RECORD, .segno = 1);

        fill_last_page(wal2);
        result = testFilter(filter, wal2, bufSize(wal2), bufSize(wal2));
        TEST_RESULT_BOOL(bufEq(wal2, result), true, "WAL not the same");
        bufFree(wal2);
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000002-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");

        TEST_TITLE("no files in repository");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());

        wal2 = bufNew(32768);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32680, NULL);
        hrnGpdbWalInsertXRecordSimple(wal2, record);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, INCOMPLETE_RECORD, .segno = 1);

        fill_last_page(wal2);
        TEST_ERROR(
            testFilter(filter, wal2, bufSize(wal2), bufSize(wal2)), FormatError, "no WAL files were found in the repository");
        bufFree(wal2);

        TEST_TITLE("multiply WAL files");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());
        {
            Buffer *wal1 = bufNew(32768);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordP(wal1, record, 0, .begin_offset = 100);
            fill_last_page(wal1);

            Buffer *zeros = bufNew(32768);
            memset(bufPtr(zeros), 0, bufUsed(zeros));

            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                zeros);
            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000002-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                wal1);
            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000040-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                zeros);
        }

        wal2 = bufNew(32768);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32664, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, NO_FLAGS, .segno = 1);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, INCOMPLETE_RECORD, .segno = 1);

        fill_last_page(wal2);
        result = testFilter(filter, wal2, bufSize(wal2), bufSize(wal2));
        TEST_RESULT_BOOL(bufEq(wal2, result), true, "WAL not the same");
        bufFree(wal2);
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000002-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000040-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");

        TEST_TITLE("simple read end from next file");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());
        {
            Buffer *zeros = bufNew(32768);
            memset(bufPtr(zeros), 0, bufUsed(zeros));

            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                zeros);
        }

        wal2 = bufNew(1024 * 1024);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32664, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, NO_FLAGS, .segno = 2);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, INCOMPLETE_RECORD, .segno = 2);

        fill_last_page(wal2);
        result = testFilter(filter, wal2, bufSize(wal2), bufSize(wal2));
        TEST_RESULT_BOOL(bufEq(wal2, result), true, "WAL not the same");
        bufFree(wal2);
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");

        TEST_TITLE("read more then one page from next file");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());
        {
            Buffer *wal1 = bufNew(98304);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 65504, NULL);
            hrnGpdbWalInsertXRecordP(wal1, record, 0, .begin_offset = 65536 - 32);
            fill_last_page(wal1);

            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000002-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                wal1);
        }

        wal2 = bufNew(1024 * 1024);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32664, NULL);
        hrnGpdbWalInsertXRecordSimple(wal2, record);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 65504, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, INCOMPLETE_RECORD, .segno = 1);

        fill_last_page(wal2);
        result = testFilter(filter, wal2, bufSize(wal2), bufSize(wal2));
        TEST_RESULT_BOOL(bufEq(wal2, result), true, "WAL not the same");
        bufFree(wal2);
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000002-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");

        TEST_TITLE("Unexpected WAL end");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());
        {
            Buffer *wal1 = bufNew(98304);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 65504, NULL);
            hrnGpdbWalInsertXRecordP(wal1, record, 0, .begin_offset = 65536 - 32);
            fill_last_page(wal1);
            bufResize(wal1, 65536);
            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000002-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                wal1);
        }

        wal2 = bufNew(1024 * 1024);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32664, NULL);
        hrnGpdbWalInsertXRecordSimple(wal2, record);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 65504, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, INCOMPLETE_RECORD, .segno = 1);

        fill_last_page(wal2);
        TEST_ERROR(testFilter(filter, wal2, bufSize(wal2), bufSize(wal2)), FormatError, "Unexpected WAL end");

        bufFree(wal2);
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000002-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");

        TEST_TITLE("usefully part of header in the next file");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());
        {
            Buffer *wal1 = bufNew(32768);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordP(wal1, record, 0, .begin_offset = 124);
            fill_last_page(wal1);

            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000002-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                wal1);
        }

        wal2 = bufNew(1024 * 1024);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32688, NULL);
        hrnGpdbWalInsertXRecordSimple(wal2, record);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, INCOMPLETE_RECORD, .segno = 1);

        fill_last_page(wal2);
        result = testFilter(filter, wal2, bufSize(wal2), bufSize(wal2));
        TEST_RESULT_BOOL(bufEq(wal2, result), true, "WAL not the same");
        bufFree(wal2);
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000002-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");

        TEST_TITLE("compressed and encrypted WAL file");
        HRN_STORAGE_PATH_REMOVE(storageRepoWrite(), STORAGE_REPO_ARCHIVE, .recurse = true);
        HRN_INFO_PUT(
            storageRepoWrite(), INFO_ARCHIVE_PATH_FILE,
            "[cipher]\n"
            "cipher-pass=\"" TEST_CIPHER_PASS_ARCHIVE
            "\"\n"
            "[db]\n"
            "db-id=1\n"
            "db-system-id=7374327172765320188\n"
            "db-version=\"9.4\"\n"
            "\n"
            "[db:history]\n"
            "1={\"db-id\":" HRN_PG_SYSTEMID_94_Z ",\"db-version\":\"9.4\"}",
            .cipherType = cipherTypeAes256Cbc);

        hrnCfgArgRawStrId(argBaseList, cfgOptRepoCipherType,  cipherTypeAes256Cbc);
        hrnCfgEnvRawZ(cfgOptRepoCipherPass, TEST_CIPHER_PASS);
        HRN_CFG_LOAD(cfgCmdArchiveGet, argBaseList);

        archiveInfo.cipherType = cipherTypeAes256Cbc;
        archiveInfo.cipherPassArchive = STRDEF(TEST_CIPHER_PASS_ARCHIVE);

        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo,  emptyFilterList());
        {
            Buffer *wal1 = bufNew(32768);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordP(wal1, record, 0, .begin_offset = 100);
            fill_last_page(wal1);

            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                wal1, .compressType = compressTypeGz, .cipherType = cipherTypeAes256Cbc, .cipherPass = TEST_CIPHER_PASS_ARCHIVE);
        }

        wal2 = bufNew(1024 * 1024);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32664, NULL);
        hrnGpdbWalInsertXRecordSimple(wal2, record);
        record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
        hrnGpdbWalInsertXRecordP(wal2, record, INCOMPLETE_RECORD, .segno = 1);

        fill_last_page(wal2);
        result = testFilter(filter, wal2, bufSize(wal2), bufSize(wal2));
        TEST_RESULT_BOOL(bufEq(wal2, result), true, "WAL not the same");
        bufFree(wal2);
        HRN_STORAGE_PATH_REMOVE(storageRepoWrite(), STORAGE_REPO_ARCHIVE, .recurse = true);
        hrnCfgEnvRemoveRaw(cfgOptRepoCipherPass);
        HRN_STORAGE_REMOVE(
            storageRepoWrite(),
            STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000002-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");
    }

    if (testBegin("read valid wal"))
    {
        TEST_TITLE("one simple record");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {RM_XLOG_ID, XLOG_NOOP, 100}
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("split header");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {RM_XLOG_ID, XLOG_NOOP, 29968},
                {RM_XLOG_ID, XLOG_NOOP, 2720},
                {RM_XLOG_ID, XLOG_NOOP, 100}
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("split body");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {RM_XLOG_ID, XLOG_NOOP, 6000},
                {RM_XLOG_ID, XLOG_NOOP, 6000},
                {RM_XLOG_ID, XLOG_NOOP, 6000},
                {RM_XLOG_ID, XLOG_NOOP, 6000},
                {RM_XLOG_ID, XLOG_NOOP, 6000},
                {RM_XLOG_ID, XLOG_NOOP, 2000},
                {RM_XLOG_ID, XLOG_NOOP, 1000}
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("not enough input buffer - begin of record");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {RM_XLOG_ID, XLOG_NOOP, 9968},
                {RM_XLOG_ID, XLOG_NOOP, 9968},
                {RM_XLOG_ID, XLOG_NOOP, 9968},
                {RM_XLOG_ID, XLOG_NOOP, 2696},
                {RM_XLOG_ID, XLOG_NOOP, 100},
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        result = testFilter(filter, wal, XLOG_BLCKSZ, XLOG_BLCKSZ);
        TEST_RESULT_BOOL(bufEq(wal, result), true, "not enough input buffer - begin of record");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("not enough input buffer - header");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {RM_XLOG_ID, XLOG_NOOP, 9968},
                {RM_XLOG_ID, XLOG_NOOP, 9968},
                {RM_XLOG_ID, XLOG_NOOP, 9968},
                {RM_XLOG_ID, XLOG_NOOP, 2688},
                {RM_XLOG_ID, XLOG_NOOP, 100},
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        result = testFilter(filter, wal, XLOG_BLCKSZ, XLOG_BLCKSZ);
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("not enough input buffer - body");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {RM_XLOG_ID, XLOG_NOOP, 9968},
                {RM_XLOG_ID, XLOG_NOOP, 9968},
                {RM_XLOG_ID, XLOG_NOOP, 9968},
                {RM_XLOG_ID, XLOG_NOOP, 2500},
                {RM_XLOG_ID, XLOG_NOOP, 1000},
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        result = testFilter(filter, wal, XLOG_BLCKSZ, XLOG_BLCKSZ);
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("copy data after wal switch");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(XLOG_BLCKSZ * 3);
            XRecordInfo walRecords[] = {
                {RM_XLOG_ID, XLOG_NOOP, 100}
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);

            size_t to_write = XLOG_BLCKSZ * 3 - bufUsed(wal);
            memset(bufRemainsPtr(wal), 0, to_write);
            bufUsedInc(wal, to_write);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("copy data after wal switch from beginning of page");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(XLOG_BLCKSZ * 3);
            XRecordInfo walRecords[] = {
                {RM_XLOG_ID, XLOG_NOOP, 9968},
                {RM_XLOG_ID, XLOG_NOOP, 9968},
                {RM_XLOG_ID, XLOG_NOOP, 9968},
                {RM_XLOG_ID, XLOG_NOOP, 2664}
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);

            size_t to_write = XLOG_BLCKSZ * 3 - bufUsed(wal);
            memset(bufRemainsPtr(wal), 0, to_write);
            bufUsedInc(wal, to_write);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("override record in header");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(XLOG_BLCKSZ * 2);

            XLogRecord *record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32688, NULL);
            hrnGpdbWalInsertXRecordSimple(wal, record);
            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 3000, NULL);
            hrnGpdbWalInsertXRecordP(wal, record, INCOMPLETE_RECORD);
            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordP(wal, record, OVERWRITE);
            record = hrnGpdbCreateXRecord(0, XLOG_SWITCH, 0, NULL);
            hrnGpdbWalInsertXRecordSimple(wal, record);

            size_t to_write = XLOG_BLCKSZ * 2 - bufUsed(wal);
            memset(bufRemainsPtr(wal), 0, to_write);
            bufUsedInc(wal, to_write);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("override record in header");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(XLOG_BLCKSZ * 2);

            XLogRecord *record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 30000, NULL);
            hrnGpdbWalInsertXRecordSimple(wal, record);
            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 3000, NULL);
            hrnGpdbWalInsertXRecordP(wal, record, INCOMPLETE_RECORD);
            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordP(wal, record, OVERWRITE);
            record = hrnGpdbCreateXRecord(0, XLOG_SWITCH, 0, NULL);
            hrnGpdbWalInsertXRecordSimple(wal, record);

            size_t to_write = XLOG_BLCKSZ * 2 - bufUsed(wal);
            memset(bufRemainsPtr(wal), 0, to_write);
            bufUsedInc(wal, to_write);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("override record at the beginning");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XLogRecord *record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordP(wal, record, OVERWRITE, .begin_offset = 100);
            XRecordInfo walRecords[] = {
                {RM_XLOG_ID, XLOG_NOOP, 100}
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("valid full page image with max size");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            uint8_t info = XLOG_FPI;
            info |= XLR_BKP_BLOCK(0);
            info |= XLR_BKP_BLOCK(1);
            info |= XLR_BKP_BLOCK(2);
            info |= XLR_BKP_BLOCK(3);

            XLogRecord *record = hrnGpdbCreateXRecord(0, info,
                                                      1 + XLR_MAX_BKP_BLOCKS * (sizeof(BkpBlock) + BLCKSZ), NULL);
            record->xl_len = 1;
            memset(XLogRecGetData(record), 0, 1);
            memset(XLogRecGetData(record) + 1, 0, XLR_MAX_BKP_BLOCKS * (sizeof(BkpBlock) + BLCKSZ));
            record->xl_crc = 2843410330;

            hrnGpdbWalInsertXRecordSimple(wal, record);
            record = hrnGpdbCreateXRecord(0, XLOG_SWITCH, 0, NULL);
            hrnGpdbWalInsertXRecordSimple(wal, record);
            fill_last_page(wal);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("long record");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {RM_XLOG_ID, XLOG_NOOP, 196608}
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);
    }

    if (testBegin("read invalid wal"))
    {
        TEST_TITLE("wrong header magic");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XLogRecord *record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);

            hrnGpdbWalInsertXRecordP(wal, record, 0, .magic = 0xDEAD);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError, "wrong page magic");
        bufFree(wal);

        TEST_TITLE("XLP_FIRST_IS_CONTRECORD in the beginning of the record");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {RM_XLOG_ID, XLOG_NOOP, 29968},
                {RM_XLOG_ID, XLOG_NOOP, 2696},
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), NO_SWITCH_WAL);
            XLogRecord *record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordP(wal, record, COND_FLAG);
            fill_last_page(wal);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError,
                   "should not be XLP_FIRST_IS_CONTRECORD");
        bufFree(wal);

        TEST_TITLE("no XLP_FIRST_IS_CONTRECORD in split header");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {RM_XLOG_ID, XLOG_NOOP, 29968},
                {RM_XLOG_ID, XLOG_NOOP, 2688},
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), NO_SWITCH_WAL);
            XLogRecord *record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordP(wal, record, NO_COND_FLAG);
            fill_last_page(wal);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError,
                   "should be XLP_FIRST_IS_CONTRECORD");
        bufFree(wal);

        TEST_TITLE("no XLP_FIRST_IS_CONTRECORD in split body");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {RM_XLOG_ID, XLOG_NOOP, 29968},
                {RM_XLOG_ID, XLOG_NOOP, 2588},
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), NO_SWITCH_WAL);
            XLogRecord *record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 500, NULL);
            hrnGpdbWalInsertXRecordP(wal, record, NO_COND_FLAG);
            fill_last_page(wal);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError,
                   "should be XLP_FIRST_IS_CONTRECORD");
        bufFree(wal);

        TEST_TITLE("zero rem_len");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {RM_XLOG_ID, XLOG_NOOP, 29968},
                {RM_XLOG_ID, XLOG_NOOP, 2588},
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), NO_SWITCH_WAL);
            XLogRecord *record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 500, NULL);
            hrnGpdbWalInsertXRecordP(wal, record, ZERO_REM_LEN);
            fill_last_page(wal);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError,
                   "invalid contrecord length: expect: 428, get 0");
        bufFree(wal);

        TEST_TITLE("zero rem_len");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {RM_XLOG_ID, XLOG_NOOP, 29968},
                {RM_XLOG_ID, XLOG_NOOP, 2588},
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), NO_SWITCH_WAL);
            XLogRecord *record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 500, NULL);
            hrnGpdbWalInsertXRecordP(wal, record, WRONG_REM_LEN);
            fill_last_page(wal);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError,
                   "invalid contrecord length: expect: 428, get 1");
        bufFree(wal);

        TEST_TITLE("non zero length of xlog switch record body");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {RM_XLOG_ID, XLOG_NOOP, 100}
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), NO_SWITCH_WAL);
            XLogRecord *record = hrnGpdbCreateXRecord(0, XLOG_SWITCH, 100, NULL);
            hrnGpdbWalInsertXRecordSimple(wal, record);
            fill_last_page(wal);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError, "invalid xlog switch record");
        bufFree(wal);

        TEST_TITLE("record with zero length");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {0, XLOG_NOOP, 0}
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError, "record with zero length");
        bufFree(wal);

        TEST_TITLE("invalid record length");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XLogRecord *record = hrnGpdbCreateXRecord(0, XLOG_NOOP, 100, NULL);
            record->xl_tot_len = 60;
            hrnGpdbWalInsertXRecordSimple(wal, record);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError, "invalid record length");
        bufFree(wal);

        TEST_TITLE("invalid record length 2");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XLogRecord *record = hrnGpdbCreateXRecord(0, XLOG_NOOP, 100, NULL);
            record = memResize(record, 100 + XLR_MAX_BKP_BLOCKS * (sizeof(BkpBlock) + BLCKSZ) + 1);
            memset(((char *) record) + 100, 0, XLR_MAX_BKP_BLOCKS * (sizeof(BkpBlock) + BLCKSZ) + 1);
            record->xl_tot_len = 100 + XLR_MAX_BKP_BLOCKS * (sizeof(BkpBlock) + BLCKSZ) + 1;
            record->xl_len = 10;
            hrnGpdbWalInsertXRecordSimple(wal, record);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError, "invalid record length");
        bufFree(wal);

        TEST_TITLE("invalid resource manager ID");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XLogRecord *record = hrnGpdbCreateXRecord(UINT8_MAX, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordSimple(wal, record);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError, "invalid resource manager ID 255");
        bufFree(wal);

        TEST_TITLE("invalid backup block size in record");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            uint8_t info = XLOG_FPI;
            info |= XLR_BKP_BLOCK(0);
            XLogRecord *record = hrnGpdbCreateXRecord(0, info, 100, NULL);
            hrnGpdbWalInsertXRecordSimple(wal, record);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError,
                   "invalid backup block size in record");
        bufFree(wal);

        TEST_TITLE("incorrect hole size in record");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            uint8_t info = XLOG_FPI;
            info |= XLR_BKP_BLOCK(0);
            XLogRecord *record = hrnGpdbCreateXRecord(0, info, 100 + sizeof(BkpBlock) + BLCKSZ, NULL);
            record->xl_len = 100;

            BkpBlock *blkp = (BkpBlock *) (XLogRecGetData(record) + 100);
            blkp->hole_offset = BLCKSZ;
            blkp->hole_length = BLCKSZ;

            hrnGpdbWalInsertXRecordSimple(wal, record);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError, "incorrect hole size in record");
        bufFree(wal);

        TEST_TITLE("invalid backup block size in record");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            uint8_t info = XLOG_FPI;
            info |= XLR_BKP_BLOCK(0);
            XLogRecord *record = hrnGpdbCreateXRecord(0, info, 1000, NULL);
            record->xl_len = 100;

            BkpBlock *blkp = (BkpBlock *) (XLogRecGetData(record) + 100);
            blkp->hole_offset = 0;
            blkp->hole_length = 0;

            hrnGpdbWalInsertXRecordSimple(wal, record);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError,
                   "invalid backup block size in record");
        bufFree(wal);

        TEST_TITLE("incorrect total length in record");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            uint8_t info = XLOG_FPI;
            info |= XLR_BKP_BLOCK(0);
            XLogRecord *record = hrnGpdbCreateXRecord(0, info, sizeof(BkpBlock) + BLCKSZ + 100 + 200,
                                                      NULL);
            record->xl_len = 100;

            BkpBlock *blkp = (BkpBlock *) (XLogRecGetData(record) + 100);
            blkp->hole_offset = 0;
            blkp->hole_length = 0;

            hrnGpdbWalInsertXRecordSimple(wal, record);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError,
                   "incorrect total length in record");
        bufFree(wal);

        TEST_TITLE("incorrect resource manager data checksum in record");
        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, NULL,  emptyFilterList());
        {
            wal = bufNew(1024 * 1024);
            XLogRecord *record = hrnGpdbCreateXRecord(0, XLOG_NOOP, 100, NULL);
            record->xl_crc = 10;

            hrnGpdbWalInsertXRecordSimple(wal, record);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError,
                   "incorrect resource manager data checksum in record. expect: 10, but got: 942755737");
        bufFree(wal);
    }

    if (testBegin("get RelFileNode from XRecord body - Greenplum 6"))
    {
        #define RM_XLOG_ID 0
        #define RM_TRANSACTION_ID 1
        #define RM_STORAGE_ID 2
        #define RM_CLOG_ID 3
        #define RM_DATABASE_ID 4
        #define RM_TABLESPACE_ID 5
        #define RM_MULTIXACT_ID 6
        #define RM_RELMAP_ID 7
        #define RM_STANDBY_ID 8
        #define RM_HEAP2_ID 9
        #define RM_HEAP_ID 10
        #define RM_BTREE_ID 11
        #define RM_HASH_ID 12
        #define RM_GIN_ID 13
        #define RM_GIST_ID 14
        #define RM_SEQUENCE_ID 15
        #define RM_SPGIST_ID 16
        #define RM_BITMAP_ID 17
        #define RM_DISTRIBUTEDLOG_ID 18
        #define RM_APPENDONLY_ID 19

        // xlog
        #define XLOG_CHECKPOINT_SHUTDOWN        0x00
        #define XLOG_CHECKPOINT_ONLINE          0x10
        #define XLOG_NOOP                       0x20
        #define XLOG_NEXTOID                    0x30
        #define XLOG_SWITCH                     0x40
        #define XLOG_BACKUP_END                 0x50
        #define XLOG_PARAMETER_CHANGE           0x60
        #define XLOG_RESTORE_POINT              0x70
        #define XLOG_FPW_CHANGE                 0x80
        #define XLOG_END_OF_RECOVERY            0x90
        #define XLOG_FPI                        0xA0
        #define XLOG_NEXTRELFILENODE            0xB0
        #define XLOG_OVERWRITE_CONTRECORD       0xC0

        // storage
        #define XLOG_SMGR_CREATE    0x10
        #define XLOG_SMGR_TRUNCATE  0x20

        // heap
        #define XLOG_HEAP2_REWRITE      0x00
        #define XLOG_HEAP2_CLEAN        0x10
        #define XLOG_HEAP2_FREEZE_PAGE  0x20
        #define XLOG_HEAP2_CLEANUP_INFO 0x30
        #define XLOG_HEAP2_VISIBLE      0x40
        #define XLOG_HEAP2_MULTI_INSERT 0x50
        #define XLOG_HEAP2_LOCK_UPDATED 0x60
        #define XLOG_HEAP2_NEW_CID      0x70
        #define XLOG_HEAP_INSERT        0x00
        #define XLOG_HEAP_DELETE        0x10
        #define XLOG_HEAP_UPDATE        0x20

        #define XLOG_HEAP_HOT_UPDATE    0x40
        #define XLOG_HEAP_NEWPAGE       0x50
        #define XLOG_HEAP_LOCK          0x60
        #define XLOG_HEAP_INPLACE       0x70
        #define XLOG_HEAP_INIT_PAGE     0x80

        // btree
        #define XLOG_BTREE_INSERT_LEAF  0x00
        #define XLOG_BTREE_INSERT_UPPER 0x10
        #define XLOG_BTREE_INSERT_META  0x20
        #define XLOG_BTREE_SPLIT_L      0x30
        #define XLOG_BTREE_SPLIT_R      0x40
        #define XLOG_BTREE_SPLIT_L_ROOT 0x50
        #define XLOG_BTREE_SPLIT_R_ROOT 0x60
        #define XLOG_BTREE_DELETE       0x70
        #define XLOG_BTREE_UNLINK_PAGE  0x80
        #define XLOG_BTREE_UNLINK_PAGE_META 0x90
        #define XLOG_BTREE_NEWROOT      0xA0
        #define XLOG_BTREE_MARK_PAGE_HALFDEAD 0xB0
        #define XLOG_BTREE_VACUUM       0xC0
        #define XLOG_BTREE_REUSE_PAGE   0xD0

        // gin
        #define XLOG_GIN_CREATE_INDEX  0x00
        #define XLOG_GIN_CREATE_PTREE  0x10
        #define XLOG_GIN_INSERT  0x20
        #define XLOG_GIN_SPLIT  0x30
        #define XLOG_GIN_VACUUM_PAGE    0x40
        #define XLOG_GIN_VACUUM_DATA_LEAF_PAGE  0x90
        #define XLOG_GIN_DELETE_PAGE    0x50
        #define XLOG_GIN_UPDATE_META_PAGE 0x60
        #define XLOG_GIN_INSERT_LISTPAGE  0x70
        #define XLOG_GIN_DELETE_LISTPAGE  0x80

        // gist
        #define XLOG_GIST_PAGE_UPDATE       0x00
        #define XLOG_GIST_PAGE_SPLIT        0x30
        #define XLOG_GIST_CREATE_INDEX      0x50

        // sequence
        #define XLOG_SEQ_LOG            0x00

        // spgist
        #define XLOG_SPGIST_CREATE_INDEX    0x00
        #define XLOG_SPGIST_ADD_LEAF        0x10
        #define XLOG_SPGIST_MOVE_LEAFS      0x20
        #define XLOG_SPGIST_ADD_NODE        0x30
        #define XLOG_SPGIST_SPLIT_TUPLE     0x40
        #define XLOG_SPGIST_PICKSPLIT       0x50
        #define XLOG_SPGIST_VACUUM_LEAF     0x60
        #define XLOG_SPGIST_VACUUM_ROOT     0x70
        #define XLOG_SPGIST_VACUUM_REDIRECT 0x80

        // bitmap
        #define XLOG_BITMAP_INSERT_LOVITEM  0x20
        #define XLOG_BITMAP_INSERT_META     0x30
        #define XLOG_BITMAP_INSERT_BITMAP_LASTWORDS 0x40

        #define XLOG_BITMAP_INSERT_WORDS        0x50

        #define XLOG_BITMAP_UPDATEWORD          0x70
        #define XLOG_BITMAP_UPDATEWORDS         0x80

        // appendonly
        #define XLOG_APPENDONLY_INSERT          0x00
        #define XLOG_APPENDONLY_TRUNCATE        0x10

        XLogRecord *record;

        TEST_TITLE("XLOG");
        test_get_relfilenode(RM_XLOG_ID, XLOG_CHECKPOINT_SHUTDOWN, false);
        test_get_relfilenode(RM_XLOG_ID, XLOG_CHECKPOINT_ONLINE, false);
        test_get_relfilenode(RM_XLOG_ID, XLOG_NOOP, false);
        test_get_relfilenode(RM_XLOG_ID, XLOG_NEXTOID, false);
        test_get_relfilenode(RM_XLOG_ID, XLOG_SWITCH, false);
        test_get_relfilenode(RM_XLOG_ID, XLOG_BACKUP_END, false);
        test_get_relfilenode(RM_XLOG_ID, XLOG_PARAMETER_CHANGE, false);
        test_get_relfilenode(RM_XLOG_ID, XLOG_RESTORE_POINT, false);
        test_get_relfilenode(RM_XLOG_ID, XLOG_FPW_CHANGE, false);
        test_get_relfilenode(RM_XLOG_ID, XLOG_END_OF_RECOVERY, false);

        test_get_relfilenode(RM_XLOG_ID, XLOG_FPI, true);

        record = hrnGpdbCreateXRecord(RM_XLOG_ID, 0xD0, 100, NULL);
        TEST_ERROR(getRelFileNodeGPDB6(record, NULL), FormatError,
                   "XLOG UNKNOWN: 208");
        memFree(record);

        TEST_TITLE("Storage");
        test_get_relfilenode(RM_STORAGE_ID, XLOG_SMGR_CREATE, true);

        record = hrnGpdbCreateXRecord(RM_STORAGE_ID, XLOG_SMGR_TRUNCATE, 100, NULL);

        {
            xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(record);
            xlrec->rnode.spcNode = 1;
            xlrec->rnode.dbNode = 2;
            xlrec->rnode.relNode = 3;

            RelFileNode *node = NULL;
            RelFileNode node_expect = {1, 2, 3};
            TEST_RESULT_BOOL(getRelFileNodeGPDB6(record, &node), 1, "wrong result from get_relfilenode");
            TEST_RESULT_BOOL(memcmp(&node_expect, node, sizeof(RelFileNode)), 0, "RelFileNode is different from expected");
        }
        memFree(record);

        record = hrnGpdbCreateXRecord(RM_STORAGE_ID, 0x30, 100, NULL);
        TEST_ERROR(getRelFileNodeGPDB6(record, NULL), FormatError,
                   "Storage UNKNOWN: 48");
        memFree(record);

        TEST_TITLE("Heap2");
        test_get_relfilenode(RM_HEAP2_ID, XLOG_HEAP2_REWRITE, false);

        test_get_relfilenode(RM_HEAP2_ID, XLOG_HEAP2_CLEAN, true);
        test_get_relfilenode(RM_HEAP2_ID, XLOG_HEAP2_FREEZE_PAGE, true);
        test_get_relfilenode(RM_HEAP2_ID, XLOG_HEAP2_CLEANUP_INFO, true);
        test_get_relfilenode(RM_HEAP2_ID, XLOG_HEAP2_VISIBLE, true);
        test_get_relfilenode(RM_HEAP2_ID, XLOG_HEAP2_MULTI_INSERT, true);
        test_get_relfilenode(RM_HEAP2_ID, XLOG_HEAP2_LOCK_UPDATED, true);

        record = hrnGpdbCreateXRecord(RM_HEAP2_ID, XLOG_HEAP2_NEW_CID, 100, NULL);

        {
            xl_heap_new_cid *xlrec = (xl_heap_new_cid *) XLogRecGetData(record);
            xlrec->target.node.spcNode = 1;
            xlrec->target.node.dbNode = 2;
            xlrec->target.node.relNode = 3;

            RelFileNode *node = NULL;
            RelFileNode node_expect = {1, 2, 3};
            TEST_RESULT_BOOL(getRelFileNodeGPDB6(record, &node), 1, "wrong result from get_relfilenode");
            TEST_RESULT_BOOL(memcmp(&node_expect, node, sizeof(RelFileNode)), 0, "RelFileNode is different from expected");
        }
        memFree(record);

        TEST_TITLE("Heap");
        test_get_relfilenode(RM_HEAP_ID, XLOG_HEAP_INSERT, true);
        test_get_relfilenode(RM_HEAP_ID, XLOG_HEAP_DELETE, true);
        test_get_relfilenode(RM_HEAP_ID, XLOG_HEAP_UPDATE, true);
        test_get_relfilenode(RM_HEAP_ID, XLOG_HEAP_HOT_UPDATE, true);
        test_get_relfilenode(RM_HEAP_ID, XLOG_HEAP_NEWPAGE, true);
        test_get_relfilenode(RM_HEAP_ID, XLOG_HEAP_LOCK, true);
        test_get_relfilenode(RM_HEAP_ID, XLOG_HEAP_INPLACE, true);

        TEST_TITLE("Btree");
        test_get_relfilenode(RM_BTREE_ID, XLOG_BTREE_INSERT_LEAF, true);
        test_get_relfilenode(RM_BTREE_ID, XLOG_BTREE_INSERT_UPPER, true);
        test_get_relfilenode(RM_BTREE_ID, XLOG_BTREE_SPLIT_L, true);
        test_get_relfilenode(RM_BTREE_ID, XLOG_BTREE_SPLIT_R, true);
        test_get_relfilenode(RM_BTREE_ID, XLOG_BTREE_SPLIT_L_ROOT, true);
        test_get_relfilenode(RM_BTREE_ID, XLOG_BTREE_SPLIT_R_ROOT, true);
        test_get_relfilenode(RM_BTREE_ID, XLOG_BTREE_VACUUM, true);
        test_get_relfilenode(RM_BTREE_ID, XLOG_BTREE_DELETE, true);
        test_get_relfilenode(RM_BTREE_ID, XLOG_BTREE_MARK_PAGE_HALFDEAD, true);
        test_get_relfilenode(RM_BTREE_ID, XLOG_BTREE_UNLINK_PAGE_META, true);
        test_get_relfilenode(RM_BTREE_ID, XLOG_BTREE_UNLINK_PAGE, true);
        test_get_relfilenode(RM_BTREE_ID, XLOG_BTREE_NEWROOT, true);
        test_get_relfilenode(RM_BTREE_ID, XLOG_BTREE_REUSE_PAGE, true);

        record = hrnGpdbCreateXRecord(RM_BTREE_ID, 0xF0, 100, NULL);
        TEST_ERROR(getRelFileNodeGPDB6(record, NULL), FormatError,
                   "Btree UNKNOWN: 240");
        memFree(record);

        TEST_TITLE("GIN");
        test_get_relfilenode(RM_GIN_ID, XLOG_GIN_CREATE_INDEX, true);
        test_get_relfilenode(RM_GIN_ID, XLOG_GIN_CREATE_PTREE, true);
        test_get_relfilenode(RM_GIN_ID, XLOG_GIN_INSERT, true);
        test_get_relfilenode(RM_GIN_ID, XLOG_GIN_SPLIT, true);
        test_get_relfilenode(RM_GIN_ID, XLOG_GIN_VACUUM_PAGE, true);
        test_get_relfilenode(RM_GIN_ID, XLOG_GIN_VACUUM_DATA_LEAF_PAGE, true);
        test_get_relfilenode(RM_GIN_ID, XLOG_GIN_DELETE_PAGE, true);
        test_get_relfilenode(RM_GIN_ID, XLOG_GIN_UPDATE_META_PAGE, true);
        test_get_relfilenode(RM_GIN_ID, XLOG_GIN_INSERT_LISTPAGE, true);
        test_get_relfilenode(RM_GIN_ID, XLOG_GIN_DELETE_LISTPAGE, true);

        record = hrnGpdbCreateXRecord(RM_GIN_ID, 0xA0, 100, NULL);
        TEST_ERROR(getRelFileNodeGPDB6(record, NULL), FormatError,
                   "GIN UNKNOWN: 160");
        memFree(record);

        TEST_TITLE("GIST");
        test_get_relfilenode(RM_GIST_ID, XLOG_GIST_PAGE_UPDATE, true);
        test_get_relfilenode(RM_GIST_ID, XLOG_GIST_PAGE_SPLIT, true);
        test_get_relfilenode(RM_GIST_ID, XLOG_GIST_CREATE_INDEX, true);

        record = hrnGpdbCreateXRecord(RM_GIST_ID, 0x60, 100, NULL);
        TEST_ERROR(getRelFileNodeGPDB6(record, NULL), FormatError,
                   "GIST UNKNOWN: 96");
        memFree(record);

        TEST_TITLE("Sequence");
        test_get_relfilenode(RM_SEQUENCE_ID, XLOG_SEQ_LOG, true);

        record = hrnGpdbCreateXRecord(RM_SEQUENCE_ID, 0x10, 100, NULL);
        TEST_ERROR(getRelFileNodeGPDB6(record, NULL), FormatError,
                   "Sequence UNKNOWN: 16");
        memFree(record);

        TEST_TITLE("SPGIST");
        test_get_relfilenode(RM_SPGIST_ID, XLOG_SPGIST_CREATE_INDEX, true);
        test_get_relfilenode(RM_SPGIST_ID, XLOG_SPGIST_ADD_LEAF, true);
        test_get_relfilenode(RM_SPGIST_ID, XLOG_SPGIST_MOVE_LEAFS, true);
        test_get_relfilenode(RM_SPGIST_ID, XLOG_SPGIST_ADD_NODE, true);
        test_get_relfilenode(RM_SPGIST_ID, XLOG_SPGIST_SPLIT_TUPLE, true);
        test_get_relfilenode(RM_SPGIST_ID, XLOG_SPGIST_PICKSPLIT, true);
        test_get_relfilenode(RM_SPGIST_ID, XLOG_SPGIST_VACUUM_LEAF, true);
        test_get_relfilenode(RM_SPGIST_ID, XLOG_SPGIST_VACUUM_ROOT, true);
        test_get_relfilenode(RM_SPGIST_ID, XLOG_SPGIST_VACUUM_REDIRECT, true);

        record = hrnGpdbCreateXRecord(RM_SPGIST_ID, 0x90, 100, NULL);
        TEST_ERROR(getRelFileNodeGPDB6(record, NULL), FormatError,
                   "SPGIST UNKNOWN: 144");
        memFree(record);

        TEST_TITLE("Bitmap");
        test_get_relfilenode(RM_BITMAP_ID, XLOG_BITMAP_INSERT_LOVITEM, true);
        test_get_relfilenode(RM_BITMAP_ID, XLOG_BITMAP_INSERT_META, true);
        test_get_relfilenode(RM_BITMAP_ID, XLOG_BITMAP_INSERT_BITMAP_LASTWORDS, true);
        test_get_relfilenode(RM_BITMAP_ID, XLOG_BITMAP_INSERT_WORDS, true);
        test_get_relfilenode(RM_BITMAP_ID, XLOG_BITMAP_UPDATEWORD, true);
        test_get_relfilenode(RM_BITMAP_ID, XLOG_BITMAP_UPDATEWORDS, true);

        record = hrnGpdbCreateXRecord(RM_BITMAP_ID, 0x90, 100, NULL);
        TEST_ERROR(getRelFileNodeGPDB6(record, NULL), FormatError,
                   "Bitmap UNKNOWN: 144");
        memFree(record);

        TEST_TITLE("Appendonly");
        test_get_relfilenode(RM_APPENDONLY_ID, XLOG_APPENDONLY_INSERT, true);
        test_get_relfilenode(RM_APPENDONLY_ID, XLOG_APPENDONLY_TRUNCATE, true);

        record = hrnGpdbCreateXRecord(RM_APPENDONLY_ID, 0x30, 100, NULL);
        TEST_ERROR(getRelFileNodeGPDB6(record, NULL), FormatError,
                   "Appendonly UNKNOWN: 48");
        memFree(record);

        TEST_TITLE("Resource managers without Relfilenode");
        test_get_relfilenode(RM_TRANSACTION_ID, 0, false);
        test_get_relfilenode(RM_CLOG_ID, 0, false);
        test_get_relfilenode(RM_DATABASE_ID, 0, false);
        test_get_relfilenode(RM_TABLESPACE_ID, 0, false);
        test_get_relfilenode(RM_MULTIXACT_ID, 0, false);
        test_get_relfilenode(RM_RELMAP_ID, 0, false);
        test_get_relfilenode(RM_STANDBY_ID, 0, false);
        test_get_relfilenode(RM_DISTRIBUTEDLOG_ID, 0, false);

        TEST_TITLE("Unsupported hash resource manager");
        record = hrnGpdbCreateXRecord(RM_HASH_ID, 0, 100, NULL);
        TEST_ERROR(getRelFileNodeGPDB6(record, NULL), FormatError,
                   "Not supported in greenplum. shouldn't be here");
        memFree(record);

        TEST_TITLE("Unknown resource manager");
        record = hrnGpdbCreateXRecord(RM_APPENDONLY_ID + 1, 0, 100, NULL);
        TEST_ERROR(getRelFileNodeGPDB6(record, NULL), FormatError,
                   "Unknown resource manager");
        memFree(record);

        TEST_TITLE("filter record when begin or end of the record is in the other file");

        ArchiveGetFile archiveInfo = {
            .file =
                STRDEF(STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000002-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd"),
            .repoIdx = 0,
            .archiveId = STRDEF("9.4-1"),
            .cipherType = cipherTypeNone,
            .cipherPassArchive = STRDEF("")
        };

        filter = walFilterNew(PG_VERSION_94,  CFGOPTVAL_FORK_GPDB, &archiveInfo, emptyFilterList());
        RelFileNode node1 = {
            .dbNode = 30000,
            .spcNode = 1000,
            .relNode = 17000
        };
        RelFileNode node2 = {
            .dbNode = 30000,
            .spcNode = 1000,
            .relNode = 18000
        };
        {
            Buffer *wal1 = bufNew(32768);

            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32688, NULL);
            hrnGpdbWalInsertXRecordSimple(wal1, record);

            record = hrnGpdbCreateXRecord(RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(node1), &node1);
            hrnGpdbWalInsertXRecordP(wal1, record, INCOMPLETE_RECORD);
            fill_last_page(wal1);

            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                wal1);

            Buffer *wal4 = bufNew(32768);
            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, sizeof(node2), &node2);
            hrnGpdbWalInsertXRecordP(wal4, record, 0, .begin_offset = (SizeOfXLogRecord + sizeof(node2)) - 16);
            HRN_STORAGE_PUT(
                storageRepoWrite(),
                STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000003-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                wal4);
        }
        Buffer *wal2;
        {
            wal2 = bufNew(1024 * 1024);
            record = hrnGpdbCreateXRecord(RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(node1), &node1);
            hrnGpdbWalInsertXRecordP(wal2, record, 0, .segno = 2, .begin_offset = (sizeof(node1) + SizeOfXLogRecord) - 8);
            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32595, NULL);
            hrnGpdbWalInsertXRecordSimple(wal2, record);
            record = hrnGpdbCreateXRecord(RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(node2), &node2);
            hrnGpdbWalInsertXRecordP(wal2, record, INCOMPLETE_RECORD, .segno = 2);

            fill_last_page(wal2);
        }
        Buffer *wal3;
        {
            wal3 = bufNew(1024 * 1024);
            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, sizeof(node1), &node1);
            hrnGpdbWalInsertXRecordP(wal3, record, 0, .segno = 2, .begin_offset = (sizeof(node1) + SizeOfXLogRecord) - 8);
            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, 32595, NULL);
            hrnGpdbWalInsertXRecordSimple(wal3, record);
            record = hrnGpdbCreateXRecord(RM_XLOG_ID, XLOG_NOOP, sizeof(node2), &node2);
            hrnGpdbWalInsertXRecordP(wal3, record, INCOMPLETE_RECORD, .segno = 2);

            fill_last_page(wal3);
        }

        result = testFilter(filter, wal2, bufSize(wal2), bufSize(wal2));
        TEST_RESULT_BOOL(bufEq(wal3, result), true, "WAL not the same");
        bufFree(wal2);
        HRN_STORAGE_REMOVE(storageRepoWrite(),
                           STORAGE_REPO_ARCHIVE "/9.4-1/0000000100000000/000000010000000000000001-abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");

// xlog
        #undef XLOG_CHECKPOINT_SHUTDOWN
        #undef XLOG_CHECKPOINT_ONLINE
        #undef XLOG_NOOP
        #undef XLOG_NEXTOID
        #undef XLOG_SWITCH
        #undef XLOG_BACKUP_END
        #undef XLOG_PARAMETER_CHANGE
        #undef XLOG_RESTORE_POINT
        #undef XLOG_FPW_CHANGE
        #undef XLOG_END_OF_RECOVERY
        #undef XLOG_FPI
        #undef XLOG_NEXTRELFILENODE
        #undef XLOG_OVERWRITE_CONTRECORD

        // storage
        #undef XLOG_SMGR_CREATE
        #undef XLOG_SMGR_TRUNCATE

        // heap
        #undef XLOG_HEAP2_REWRITE
        #undef XLOG_HEAP2_CLEAN
        #undef XLOG_HEAP2_FREEZE_PAGE
        #undef XLOG_HEAP2_CLEANUP_INFO
        #undef XLOG_HEAP2_VISIBLE
        #undef XLOG_HEAP2_MULTI_INSERT
        #undef XLOG_HEAP2_LOCK_UPDATED
        #undef XLOG_HEAP2_NEW_CID
        #undef XLOG_HEAP_INSERT
        #undef XLOG_HEAP_DELETE
        #undef XLOG_HEAP_UPDATE

        #undef XLOG_HEAP_HOT_UPDATE
        #undef XLOG_HEAP_NEWPAGE
        #undef XLOG_HEAP_LOCK
        #undef XLOG_HEAP_INPLACE
        #undef XLOG_HEAP_INIT_PAGE

        // btree
        #undef XLOG_BTREE_INSERT_LEAF
        #undef XLOG_BTREE_INSERT_UPPER
        #undef XLOG_BTREE_INSERT_META
        #undef XLOG_BTREE_SPLIT_L
        #undef XLOG_BTREE_SPLIT_R
        #undef XLOG_BTREE_SPLIT_L_ROOT
        #undef XLOG_BTREE_SPLIT_R_ROOT
        #undef XLOG_BTREE_DELETE
        #undef XLOG_BTREE_UNLINK_PAGE
        #undef XLOG_BTREE_UNLINK_PAGE_META
        #undef XLOG_BTREE_NEWROOT
        #undef XLOG_BTREE_MARK_PAGE_HALFDEAD
        #undef XLOG_BTREE_VACUUM
        #undef XLOG_BTREE_REUSE_PAGE

        // gin
        #undef XLOG_GIN_CREATE_INDEX
        #undef XLOG_GIN_CREATE_PTREE
        #undef XLOG_GIN_INSERT
        #undef XLOG_GIN_SPLIT
        #undef XLOG_GIN_VACUUM_PAGE
        #undef XLOG_GIN_VACUUM_DATA_LEAF_PAGE
        #undef XLOG_GIN_DELETE_PAGE
        #undef XLOG_GIN_UPDATE_META_PAGE
        #undef XLOG_GIN_INSERT_LISTPAGE
        #undef XLOG_GIN_DELETE_LISTPAGE

        // gist
        #undef XLOG_GIST_PAGE_UPDATE
        #undef XLOG_GIST_PAGE_SPLIT
        #undef XLOG_GIST_CREATE_INDEX

        // sequence
        #undef XLOG_SEQ_LOG

        // spgist
        #undef XLOG_SPGIST_CREATE_INDEX
        #undef XLOG_SPGIST_ADD_LEAF
        #undef XLOG_SPGIST_MOVE_LEAFS
        #undef XLOG_SPGIST_ADD_NODE
        #undef XLOG_SPGIST_SPLIT_TUPLE
        #undef XLOG_SPGIST_PICKSPLIT
        #undef XLOG_SPGIST_VACUUM_LEAF
        #undef XLOG_SPGIST_VACUUM_ROOT
        #undef XLOG_SPGIST_VACUUM_REDIRECT

        // bitmap
        #undef XLOG_BITMAP_INSERT_LOVITEM
        #undef XLOG_BITMAP_INSERT_META
        #undef XLOG_BITMAP_INSERT_BITMAP_LASTWORDS

        #undef XLOG_BITMAP_INSERT_WORDS

        #undef XLOG_BITMAP_UPDATEWORD
        #undef XLOG_BITMAP_UPDATEWORDS

        // appendonly
        #undef XLOG_APPENDONLY_INSERT
        #undef XLOG_APPENDONLY_TRUNCATE

        #undef RM_XLOG_ID
        #undef RM_TRANSACTION_ID
        #undef RM_STORAGE_ID
        #undef RM_CLOG_ID
        #undef RM_DATABASE_ID
        #undef RM_TABLESPACE_ID
        #undef RM_MULTIXACT_ID
        #undef RM_RELMAP_ID
        #undef RM_STANDBY_ID
        #undef RM_HEAP2_ID
        #undef RM_HEAP_ID
        #undef RM_BTREE_ID
        #undef RM_HASH_ID
        #undef RM_GIN_ID
        #undef RM_GIST_ID
        #undef RM_SEQUENCE_ID
        #undef RM_SPGIST_ID
        #undef RM_BITMAP_ID
        #undef RM_DISTRIBUTEDLOG_ID
        #undef RM_APPENDONLY_ID
    }

    if (testBegin("filter - Greenplum 6"))
    {
        #define RM_HEAP_ID 10
        #define RM_XLOG_ID 0

        #define XLOG_NOOP 0x20
        #define XLOG_HEAP_INSERT 0x00

        TEST_TITLE("Filter");
        const String *jsonstr = STRDEF("[\n"
                                       "  {\n"
                                       "    \"dbOid\": 20000,\n"
                                       "    \"tables\": [\n"
                                       "      {\n"
                                       "        \"tablespace\": 1600,\n"
                                       "        \"relfilenode\": 16384\n"
                                       "      },\n"
                                       "      {\n"
                                       "        \"tablespace\": 1601,\n"
                                       "        \"relfilenode\": 16385\n"
                                       "      }\n"
                                       "    ]\n"
                                       "  },\n"
                                       "  {\n"
                                       "    \"dbOid\": 20001,\n"
                                       "    \"tables\": [\n"
                                       "      {\n"
                                       "        \"tablespace\": 1700,\n"
                                       "        \"relfilenode\": 16386\n"
                                       "      }\n"
                                       "    ]\n"
                                       "  },\n"
                                       "  {\n"
                                       "    \"dbOid\": 20002,\n"
                                       "    \"tables\": []\n"
                                       "  }\n"
                                       "]");

        JsonRead *json = jsonReadNew(jsonstr);
        List *filterList = buildFilterList(json);
        TEST_RESULT_PTR_NE(filterList, NULL, "filter list is empty");
        TEST_RESULT_UINT(lstSize(filterList), 3, "wrong filter list length");

        filter = walFilterNew(PG_VERSION_94, CFGOPTVAL_FORK_GPDB, NULL, filterList);
        wal = bufNew(1024 * 1024);
        Buffer *expect_wal = bufNew(1024 * 1024);

        {
            RelFileNode nodes[] = {
                // records that should pass the filter
                {1600, 20000, 16384},
                {1601, 20000, 16385},
                {1700, 20001, 16386},
                // we should filter out all record for this database expect system catalog
                {1700, 20002, 13836},
                // should not be filter out
                {1600, 20002, 11612},
                // should be filter out
                {1600, 20002, 19922},

                {1800, 20000, 35993},
                {1800, 20000, 25928},
                {2000, 20001, 48457},
                // should pass filter
                {2000, 20001, 5445},
                // should be filter out
                {2000, 20003, 1000}
            };

            XRecordInfo records[] = {
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[0]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[1]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[2]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[3]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[4]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[5]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[6]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[7]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[8]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[9]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[10]},
            };

            XRecordInfo records_expected[] = {
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[0]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[1]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[2]},

                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[3]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[4]},
                {RM_XLOG_ID, XLOG_NOOP,        sizeof(RelFileNode), &nodes[5]},

                {RM_XLOG_ID, XLOG_NOOP,        sizeof(RelFileNode), &nodes[6]},
                {RM_XLOG_ID, XLOG_NOOP,        sizeof(RelFileNode), &nodes[7]},
                {RM_XLOG_ID, XLOG_NOOP,        sizeof(RelFileNode), &nodes[8]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[9]},
                {RM_XLOG_ID, XLOG_NOOP, sizeof(RelFileNode), &nodes[10]},
            };

            build_wal(wal, records, LENGTH_OF(records), 0);
            build_wal(expect_wal, records_expected, LENGTH_OF(records_expected), 0);
        }
        Buffer *result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(expect_wal, result), true, "filtered wal is different from expected");

        TEST_TITLE("Filter - empty filer list");
        filter = walFilterNew(PG_VERSION_94, CFGOPTVAL_FORK_GPDB, NULL, emptyFilterList());
        wal = bufNew(1024 * 1024);
        expect_wal = bufNew(1024 * 1024);

        {
            RelFileNode nodes[] = {
                {1600, 1, 1000},     // template1
                {1600, 12809, 1000},     // template0
                {1700, 12812, 1000},     // postgres system catalog
                {1700, 12812, 17000},     // postgres
                {1700, 16399, 17000},     // user database
            };

            XRecordInfo records[] = {
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[0]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[1]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[2]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[3]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[4]},
            };

            XRecordInfo records_expected[] = {
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[0]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[1]},
                {RM_HEAP_ID, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[2]},
                {RM_XLOG_ID, XLOG_NOOP, sizeof(RelFileNode), &nodes[3]},
                {RM_XLOG_ID, XLOG_NOOP, sizeof(RelFileNode), &nodes[4]},
            };

            build_wal(wal, records, LENGTH_OF(records), 0);
            build_wal(expect_wal, records_expected, LENGTH_OF(records_expected), 0);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(expect_wal, result), true, "filtered wal is different from expected");

        #undef RM_HEAP_ID
        #undef RM_XLOG_ID

        #undef XLOG_NOOP
        #undef XLOG_HEAP_INSERT
    }

    if (testBegin("Unsupported Greenplum verison"))
    {
        TEST_ERROR(walFilterNew(PG_VERSION_94, CFGOPTVAL_FORK_POSTGRESQL, NULL, emptyFilterList()), VersionNotSupportedError, "WAL filtering is unsupported for this Greenplum version");
        TEST_ERROR(walFilterNew(PG_VERSION_95, CFGOPTVAL_FORK_GPDB, NULL, emptyFilterList()), VersionNotSupportedError, "WAL filtering is unsupported for this Greenplum version");
    }

    FUNCTION_HARNESS_RETURN_VOID();
}
