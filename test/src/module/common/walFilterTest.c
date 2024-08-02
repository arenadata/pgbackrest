/***********************************************************************************************************************************
Test wal filter
***********************************************************************************************************************************/

#include "common/harnessConfig.h"
#include "common/harnessWal.h"
#include "common/io/bufferRead.h"
#include "common/io/io.h"
#include "common/type/json.h"
#include "common/walFilter/record_process.h"
#include "postgres/interface/crc32.h"

extern void build_filter_list(JsonRead *json, RelFileNode **filter_list, size_t *filter_list_len);

typedef enum WalFlags
{
    NO_SWITCH_WAL = 1 << 0,
} WalFlags;

typedef struct XRecordInfo
{
    enum ResourceManager rmid;
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
        XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_XLOG, XLOG_SWITCH, 0, NULL);
        hrnGpdbWalInsertXRecordSimple(wal, record);
        size_t to_write = XLOG_BLCKSZ - bufUsed(wal) % XLOG_BLCKSZ;
        memset(bufRemainsPtr(wal), 0, to_write);
        bufUsedInc(wal, to_write);
    }
}

static void
fill_last_page(Buffer *wal)
{
    size_t to_write = XLOG_BLCKSZ - bufUsed(wal) % XLOG_BLCKSZ;
    memset(bufRemainsPtr(wal), 0, to_write);
    bufUsedInc(wal, to_write);
}

static void
test_get_relfilenode(enum ResourceManager rmid, uint8_t info, bool expect_not_skip)
{
    RelFileNode node = {1, 2, 3};

    XLogRecord *record = hrnGpdbCreateXRecord(rmid, info, sizeof(node), &node);

    RelFileNode node_result = {0};
    RelFileNode node_expect = {1, 2, 3};
    TEST_RESULT_BOOL(get_relfilenode(record, &node_result), expect_not_skip, "RelFileNode is different from expected");
    if (expect_not_skip)
        TEST_RESULT_BOOL(memcmp(&node_expect, &node_result, sizeof(RelFileNode)), 0, "RelFileNode is different from expected");
    else
    {
        TEST_RESULT_UINT(node_result.spcNode, 0, "");
        TEST_RESULT_UINT(node_result.dbNode, 0, "");
        TEST_RESULT_UINT(node_result.relNode, 0, "");
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
    if (testBegin("read valid wal"))
    {
        TEST_TITLE("one simple record");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 100}
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        Buffer *result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("split header");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 29968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 2720},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 100}
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("split body");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 6000},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 6000},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 6000},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 6000},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 6000},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 2000},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 1000}
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("not enough input buffer - begin of record");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 9968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 9968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 9968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 2696},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 100},
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        result = testFilter(filter, wal, XLOG_BLCKSZ, XLOG_BLCKSZ);
        TEST_RESULT_BOOL(bufEq(wal, result), true, "not enough input buffer - begin of record");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("not enough input buffer - header");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 9968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 9968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 9968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 2688},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 100},
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        result = testFilter(filter, wal, XLOG_BLCKSZ, XLOG_BLCKSZ);
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("not enough input buffer - body");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 9968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 9968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 9968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 2500},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 1000},
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        result = testFilter(filter, wal, XLOG_BLCKSZ, XLOG_BLCKSZ);
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("partial record at the beginning");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, 100, NULL);
            hrnGpdbWalInsertXRecord(wal, record, 0, 0, 100);
            XRecordInfo walRecords[] = {
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 100}
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("copy data after wal switch");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(XLOG_BLCKSZ * 3);
            XRecordInfo walRecords[] = {
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 100}
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
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(XLOG_BLCKSZ * 3);
            XRecordInfo walRecords[] = {
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 9968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 9968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 9968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 2664}
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

        TEST_TITLE("partial record at the end");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(XLOG_BLCKSZ * 2);

            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, XLOG_BLCKSZ, NULL);
            hrnGpdbWalInsertXRecordSimple(wal, record);
            record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, XLOG_BLCKSZ - 1000, NULL);
            hrnGpdbWalInsertXRecordSimple(wal, record);
            record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, 3000, NULL);
            hrnGpdbWalInsertXRecord(wal, record, INCOMPLETE_RECORD, 0, 0);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("override record in header");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(XLOG_BLCKSZ * 2);

            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, 32688, NULL);
            hrnGpdbWalInsertXRecordSimple(wal, record);
            record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, 3000, NULL);
            hrnGpdbWalInsertXRecord(wal, record, INCOMPLETE_RECORD, 0, 0);
            record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, 100, NULL);
            hrnGpdbWalInsertXRecord(wal, record, OVERWRITE, 0, 0);
            record = hrnGpdbCreateXRecord(ResourceManager_XLOG, XLOG_SWITCH, 0, NULL);
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
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(XLOG_BLCKSZ * 2);

            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, 30000, NULL);
            hrnGpdbWalInsertXRecordSimple(wal, record);
            record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, 3000, NULL);
            hrnGpdbWalInsertXRecord(wal, record, INCOMPLETE_RECORD, 0, 0);
            record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, 100, NULL);
            hrnGpdbWalInsertXRecord(wal, record, OVERWRITE, 0, 0);
            record = hrnGpdbCreateXRecord(ResourceManager_XLOG, XLOG_SWITCH, 0, NULL);
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
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, 100, NULL);
            hrnGpdbWalInsertXRecord(wal, record, OVERWRITE, 0, 100);
            XRecordInfo walRecords[] = {
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 100}
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);

        TEST_TITLE("valid full page image with max size");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            uint8_t info = XLOG_FPI;
            info |= XLR_BKP_BLOCK(0);
            info |= XLR_BKP_BLOCK(1);
            info |= XLR_BKP_BLOCK(2);
            info |= XLR_BKP_BLOCK(3);

            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_XLOG, info,
                                                      1 + XLR_MAX_BKP_BLOCKS * (sizeof(BkpBlock) + BLCKSZ), NULL);
            record->xl_len = 1;
            memset(XLogRecGetData(record), 0, 1);
            memset(XLogRecGetData(record) + 1, 0, XLR_MAX_BKP_BLOCKS * (sizeof(BkpBlock) + BLCKSZ));
            record->xl_crc = 2843410330;

            hrnGpdbWalInsertXRecordSimple(wal, record);
            record = hrnGpdbCreateXRecord(ResourceManager_XLOG, XLOG_SWITCH, 0, NULL);
            hrnGpdbWalInsertXRecordSimple(wal, record);
            fill_last_page(wal);
        }
        result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        TEST_RESULT_BOOL(bufEq(wal, result), true, "WAL not the same");
        bufFree(wal);
        bufFree(result);
    }

    if (testBegin("read invalid wal"))
    {
        TEST_TITLE("wrong header magic");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, 100, NULL);

            hrnGpdbWalInsertXRecord(wal, record, 0, 0xDEAD, 0);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError, "wrong page magic");
        bufFree(wal);

        TEST_TITLE("XLP_FIRST_IS_CONTRECORD in the beginning of the record");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 29968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 2696},
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), NO_SWITCH_WAL);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, 100, NULL);
            hrnGpdbWalInsertXRecord(wal, record, COND_FLAG, 0, 0);
            fill_last_page(wal);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError,
                   "should not be XLP_FIRST_IS_CONTRECORD");
        bufFree(wal);

        TEST_TITLE("no XLP_FIRST_IS_CONTRECORD in split header");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 29968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 2688},
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), NO_SWITCH_WAL);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, 100, NULL);
            hrnGpdbWalInsertXRecord(wal, record, NO_COND_FLAG, 0, 0);
            fill_last_page(wal);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError,
                   "should be XLP_FIRST_IS_CONTRECORD");
        bufFree(wal);

        TEST_TITLE("no XLP_FIRST_IS_CONTRECORD in split body");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 29968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 2588},
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), NO_SWITCH_WAL);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, 500, NULL);
            hrnGpdbWalInsertXRecord(wal, record, NO_COND_FLAG, 0, 0);
            fill_last_page(wal);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError,
                   "should be XLP_FIRST_IS_CONTRECORD");
        bufFree(wal);

        TEST_TITLE("zero rem_len");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 29968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 2588},
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), NO_SWITCH_WAL);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, 500, NULL);
            hrnGpdbWalInsertXRecord(wal, record, ZERO_REM_LEN, 0, 0);
            fill_last_page(wal);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError,
                   "invalid contrecord length: expect: 428, get 0");
        bufFree(wal);

        TEST_TITLE("zero rem_len");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 29968},
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 2588},
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), NO_SWITCH_WAL);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_Heap, XLOG_HEAP_INSERT, 500, NULL);
            hrnGpdbWalInsertXRecord(wal, record, WRONG_REM_LEN, 0, 0);
            fill_last_page(wal);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError,
                   "invalid contrecord length: expect: 428, get 1");
        bufFree(wal);

        TEST_TITLE("non zero length of xlog switch record body");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {ResourceManager_Heap, XLOG_HEAP_INSERT, 100}
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), NO_SWITCH_WAL);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_XLOG, XLOG_SWITCH, 100, NULL);
            hrnGpdbWalInsertXRecordSimple(wal, record);
            fill_last_page(wal);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError, "invalid xlog switch record");
        bufFree(wal);

        TEST_TITLE("record with zero length");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XRecordInfo walRecords[] = {
                {ResourceManager_XLOG, XLOG_NOOP, 0}
            };
            build_wal(wal, walRecords, sizeof(walRecords) / sizeof(walRecords[0]), 0);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError, "record with zero length");
        bufFree(wal);

        TEST_TITLE("invalid record length");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_XLOG, XLOG_NOOP, 100, NULL);
            record->xl_tot_len = 60;
            hrnGpdbWalInsertXRecordSimple(wal, record);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError, "invalid record length");
        bufFree(wal);

        TEST_TITLE("invalid record length 2");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_XLOG, XLOG_NOOP, 100, NULL);
            record = memResize(record, 100 + XLR_MAX_BKP_BLOCKS * (sizeof(BkpBlock) + BLCKSZ) + 1);
            memset(((char *) record) + 100, 0, XLR_MAX_BKP_BLOCKS * (sizeof(BkpBlock) + BLCKSZ) + 1);
            record->xl_tot_len = 100 + XLR_MAX_BKP_BLOCKS * (sizeof(BkpBlock) + BLCKSZ) + 1;
            record->xl_len = 10;
            hrnGpdbWalInsertXRecordSimple(wal, record);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError, "invalid record length");
        bufFree(wal);

        TEST_TITLE("invalid resource manager ID");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_Appendonly + 1, XLOG_NOOP, 100, NULL);
            hrnGpdbWalInsertXRecordSimple(wal, record);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError, "invalid resource manager ID 20");
        bufFree(wal);

        TEST_TITLE("invalid backup block size in record");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            uint8_t info = XLOG_FPI;
            info |= XLR_BKP_BLOCK(0);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_XLOG, info, 100, NULL);
            hrnGpdbWalInsertXRecordSimple(wal, record);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError,
                   "invalid backup block size in record");
        bufFree(wal);

        TEST_TITLE("incorrect hole size in record");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            uint8_t info = XLOG_FPI;
            info |= XLR_BKP_BLOCK(0);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_XLOG, info, 100 + sizeof(BkpBlock) + BLCKSZ, NULL);
            record->xl_len = 100;

            BkpBlock *blkp = (BkpBlock *) (XLogRecGetData(record) + 100);
            blkp->hole_offset = BLCKSZ;
            blkp->hole_length = BLCKSZ;

            hrnGpdbWalInsertXRecordSimple(wal, record);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError, "incorrect hole size in record");
        bufFree(wal);

        TEST_TITLE("invalid backup block size in record");
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            uint8_t info = XLOG_FPI;
            info |= XLR_BKP_BLOCK(0);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_XLOG, info, 1000, NULL);
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
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            uint8_t info = XLOG_FPI;
            info |= XLR_BKP_BLOCK(0);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_XLOG, info, sizeof(BkpBlock) + BLCKSZ + 100 + 200,
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
        filter = walFilterNew(jsonReadNew(STRDEF("[]")));
        {
            wal = bufNew(1024 * 1024);
            XLogRecord *record = hrnGpdbCreateXRecord(ResourceManager_XLOG, XLOG_NOOP, 100, NULL);
            record->xl_crc = 10;

            hrnGpdbWalInsertXRecordSimple(wal, record);
        }
        TEST_ERROR(testFilter(filter, wal, bufSize(wal), bufSize(wal)), FormatError,
                   "incorrect resource manager data checksum in record. expect: 10, but got: 942755737");
        bufFree(wal);
    }

    if (testBegin("parse recovery_filter.json"))
    {
        TEST_TITLE("valid config");
        const String *jsonstr = STRDEF("[\n"
                                       "  {\n"
                                       "    \"dbName\": \"db1\",\n"
                                       "    \"dbOid\": 20000,\n"
                                       "    \"tables\": [\n"
                                       "      {\n"
                                       "        \"tablefqn\": \"t1\",\n"
                                       "        \"tableOid\": 16384,\n"
                                       "        \"tablespace\": 1600,\n"
                                       "        \"relfilenode\": 16384\n"
                                       "      },\n"
                                       "      {\n"
                                       "        \"tablefqn\": \"t2\",\n"
                                       "        \"tableOid\": 16387,\n"
                                       "        \"tablespace\": 1601,\n"
                                       "        \"relfilenode\": 16385\n"
                                       "      }\n"
                                       "    ]\n"
                                       "  },\n"
                                       "  {\n"
                                       "    \"dbName\": \"db2\",\n"
                                       "    \"dbOid\": 20001,\n"
                                       "    \"tables\": [\n"
                                       "      {\n"
                                       "        \"tablefqn\": \"t3\",\n"
                                       "        \"tableOid\": 16390,\n"
                                       "        \"tablespace\": 1700,\n"
                                       "        \"relfilenode\": 16386\n"
                                       "      }\n"
                                       "    ]\n"
                                       "  },\n"
                                       "  {\n"
                                       "    \"dbName\": \"db3\",\n"
                                       "    \"dbOid\": 20002,\n"
                                       "    \"tables\": []\n"
                                       "  }\n"
                                       "]");

        JsonRead *json = jsonReadNew(jsonstr);
        RelFileNode *filter_list = NULL;
        size_t filter_list_len = 0;
        build_filter_list(json, &filter_list, &filter_list_len);
        TEST_RESULT_PTR_NE(filter_list, NULL, "filter list is empty");
        TEST_RESULT_UINT(filter_list_len, 4, "wrong filter list length");

        RelFileNode filter_list_expected[] = {
            {1600, 20000, 16384},
            {1601, 20000, 16385},
            {1700, 20001, 16386},
            {0,    20002, 0}
        };

        TEST_RESULT_INT(memcmp(filter_list_expected, filter_list, sizeof(RelFileNode) * 4), 0, "filter list is different from expected");
    }

    if (testBegin("get RelFileNode from XRecord body")){
        XLogRecord *record;

        TEST_TITLE("XLOG");
        test_get_relfilenode(ResourceManager_XLOG, XLOG_CHECKPOINT_SHUTDOWN, false);
        test_get_relfilenode(ResourceManager_XLOG, XLOG_CHECKPOINT_ONLINE, false);
        test_get_relfilenode(ResourceManager_XLOG, XLOG_NOOP, false);
        test_get_relfilenode(ResourceManager_XLOG, XLOG_NEXTOID, false);
        test_get_relfilenode(ResourceManager_XLOG, XLOG_SWITCH, false);
        test_get_relfilenode(ResourceManager_XLOG, XLOG_BACKUP_END, false);
        test_get_relfilenode(ResourceManager_XLOG, XLOG_PARAMETER_CHANGE, false);
        test_get_relfilenode(ResourceManager_XLOG, XLOG_RESTORE_POINT, false);
        test_get_relfilenode(ResourceManager_XLOG, XLOG_FPW_CHANGE, false);
        test_get_relfilenode(ResourceManager_XLOG, XLOG_END_OF_RECOVERY, false);

        test_get_relfilenode(ResourceManager_XLOG, XLOG_FPI, true);

        record = hrnGpdbCreateXRecord(ResourceManager_XLOG, 0xD0, 100, NULL);
        TEST_ERROR(get_relfilenode(record, NULL), FormatError,
                   "XLOG UNKNOWN: 208");
        memFree(record);

        TEST_TITLE("Storage");
        test_get_relfilenode(ResourceManager_Storage, XLOG_SMGR_CREATE, true);

        record = hrnGpdbCreateXRecord(ResourceManager_Storage, XLOG_SMGR_TRUNCATE, 100, NULL);

        {
            xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(record);
            xlrec->rnode.spcNode = 1;
            xlrec->rnode.dbNode = 2;
            xlrec->rnode.relNode = 3;

            RelFileNode node = {0};
            RelFileNode node_expect = {1, 2, 3};
            TEST_RESULT_BOOL(get_relfilenode(record, &node), 1, "wrong result from get_relfilenode");
            TEST_RESULT_BOOL(memcmp(&node_expect, &node, sizeof(RelFileNode)), 0, "RelFileNode is different from expected");
        }
        memFree(record);

        record = hrnGpdbCreateXRecord(ResourceManager_Storage, 0x30, 100, NULL);
        TEST_ERROR(get_relfilenode(record, NULL), FormatError,
                   "Storage UNKNOWN: 48");
        memFree(record);

        TEST_TITLE("Heap2");
        test_get_relfilenode(ResourceManager_Heap2, XLOG_HEAP2_REWRITE, false);

        test_get_relfilenode(ResourceManager_Heap2, XLOG_HEAP2_CLEAN, true);
        test_get_relfilenode(ResourceManager_Heap2, XLOG_HEAP2_FREEZE_PAGE, true);
        test_get_relfilenode(ResourceManager_Heap2, XLOG_HEAP2_CLEANUP_INFO, true);
        test_get_relfilenode(ResourceManager_Heap2, XLOG_HEAP2_VISIBLE, true);
        test_get_relfilenode(ResourceManager_Heap2, XLOG_HEAP2_MULTI_INSERT, true);
        test_get_relfilenode(ResourceManager_Heap2, XLOG_HEAP2_LOCK_UPDATED, true);

        record = hrnGpdbCreateXRecord(ResourceManager_Heap2, XLOG_HEAP2_NEW_CID, 100, NULL);

        {
            xl_heap_new_cid *xlrec = (xl_heap_new_cid *) XLogRecGetData(record);
            xlrec->target.node.spcNode = 1;
            xlrec->target.node.dbNode = 2;
            xlrec->target.node.relNode = 3;

            RelFileNode node = {0};
            RelFileNode node_expect = {1, 2, 3};
            TEST_RESULT_BOOL(get_relfilenode(record, &node), 1, "wrong result from get_relfilenode");
            TEST_RESULT_BOOL(memcmp(&node_expect, &node, sizeof(RelFileNode)), 0, "RelFileNode is different from expected");
        }
        memFree(record);

        TEST_TITLE("Heap");
        test_get_relfilenode(ResourceManager_Heap, XLOG_HEAP_INSERT, true);
        test_get_relfilenode(ResourceManager_Heap, XLOG_HEAP_DELETE, true);
        test_get_relfilenode(ResourceManager_Heap, XLOG_HEAP_UPDATE, true);
        test_get_relfilenode(ResourceManager_Heap, XLOG_HEAP_HOT_UPDATE, true);
        test_get_relfilenode(ResourceManager_Heap, XLOG_HEAP_NEWPAGE, true);
        test_get_relfilenode(ResourceManager_Heap, XLOG_HEAP_LOCK, true);
        test_get_relfilenode(ResourceManager_Heap, XLOG_HEAP_INPLACE, true);

        TEST_TITLE("Btree");
        test_get_relfilenode(ResourceManager_Btree, XLOG_BTREE_INSERT_LEAF, true);
        test_get_relfilenode(ResourceManager_Btree, XLOG_BTREE_INSERT_UPPER, true);
        test_get_relfilenode(ResourceManager_Btree, XLOG_BTREE_SPLIT_L, true);
        test_get_relfilenode(ResourceManager_Btree, XLOG_BTREE_SPLIT_R, true);
        test_get_relfilenode(ResourceManager_Btree, XLOG_BTREE_SPLIT_L_ROOT, true);
        test_get_relfilenode(ResourceManager_Btree, XLOG_BTREE_SPLIT_R_ROOT, true);
        test_get_relfilenode(ResourceManager_Btree, XLOG_BTREE_VACUUM, true);
        test_get_relfilenode(ResourceManager_Btree, XLOG_BTREE_DELETE, true);
        test_get_relfilenode(ResourceManager_Btree, XLOG_BTREE_MARK_PAGE_HALFDEAD, true);
        test_get_relfilenode(ResourceManager_Btree, XLOG_BTREE_UNLINK_PAGE_META, true);
        test_get_relfilenode(ResourceManager_Btree, XLOG_BTREE_UNLINK_PAGE, true);
        test_get_relfilenode(ResourceManager_Btree, XLOG_BTREE_NEWROOT, true);
        test_get_relfilenode(ResourceManager_Btree, XLOG_BTREE_REUSE_PAGE, true);

        record = hrnGpdbCreateXRecord(ResourceManager_Btree, 0xF0, 100, NULL);
        TEST_ERROR(get_relfilenode(record, NULL), FormatError,
                   "Btree UNKNOWN: 240");
        memFree(record);

        TEST_TITLE("GIN");
        test_get_relfilenode(ResourceManager_Gin, XLOG_GIN_CREATE_INDEX, true);
        test_get_relfilenode(ResourceManager_Gin, XLOG_GIN_CREATE_PTREE, true);
        test_get_relfilenode(ResourceManager_Gin, XLOG_GIN_INSERT, true);
        test_get_relfilenode(ResourceManager_Gin, XLOG_GIN_SPLIT, true);
        test_get_relfilenode(ResourceManager_Gin, XLOG_GIN_VACUUM_PAGE, true);
        test_get_relfilenode(ResourceManager_Gin, XLOG_GIN_VACUUM_DATA_LEAF_PAGE, true);
        test_get_relfilenode(ResourceManager_Gin, XLOG_GIN_DELETE_PAGE, true);
        test_get_relfilenode(ResourceManager_Gin, XLOG_GIN_UPDATE_META_PAGE, true);
        test_get_relfilenode(ResourceManager_Gin, XLOG_GIN_INSERT_LISTPAGE, true);
        test_get_relfilenode(ResourceManager_Gin, XLOG_GIN_DELETE_LISTPAGE, true);

        record = hrnGpdbCreateXRecord(ResourceManager_Gin, 0xA0, 100, NULL);
        TEST_ERROR(get_relfilenode(record, NULL), FormatError,
                   "GIN UNKNOWN: 160");
        memFree(record);

        TEST_TITLE("GIST");
        test_get_relfilenode(ResourceManager_Gist, XLOG_GIST_PAGE_UPDATE, true);
        test_get_relfilenode(ResourceManager_Gist, XLOG_GIST_PAGE_SPLIT, true);
        test_get_relfilenode(ResourceManager_Gist, XLOG_GIST_CREATE_INDEX, true);

        record = hrnGpdbCreateXRecord(ResourceManager_Gist, 0x60, 100, NULL);
        TEST_ERROR(get_relfilenode(record, NULL), FormatError,
                   "GIST UNKNOWN: 96");
        memFree(record);

        TEST_TITLE("Sequence");
        test_get_relfilenode(ResourceManager_Sequence, XLOG_SEQ_LOG, true);

        record = hrnGpdbCreateXRecord(ResourceManager_Sequence, 0x10, 100, NULL);
        TEST_ERROR(get_relfilenode(record, NULL), FormatError,
                   "Sequence UNKNOWN: 16");
        memFree(record);

        TEST_TITLE("SPGIST");
        test_get_relfilenode(ResourceManager_SPGist, XLOG_SPGIST_CREATE_INDEX, true);
        test_get_relfilenode(ResourceManager_SPGist, XLOG_SPGIST_ADD_LEAF, true);
        test_get_relfilenode(ResourceManager_SPGist, XLOG_SPGIST_MOVE_LEAFS, true);
        test_get_relfilenode(ResourceManager_SPGist, XLOG_SPGIST_ADD_NODE, true);
        test_get_relfilenode(ResourceManager_SPGist, XLOG_SPGIST_SPLIT_TUPLE, true);
        test_get_relfilenode(ResourceManager_SPGist, XLOG_SPGIST_PICKSPLIT, true);
        test_get_relfilenode(ResourceManager_SPGist, XLOG_SPGIST_VACUUM_LEAF, true);
        test_get_relfilenode(ResourceManager_SPGist, XLOG_SPGIST_VACUUM_ROOT, true);
        test_get_relfilenode(ResourceManager_SPGist, XLOG_SPGIST_VACUUM_REDIRECT, true);

        record = hrnGpdbCreateXRecord(ResourceManager_SPGist, 0x90, 100, NULL);
        TEST_ERROR(get_relfilenode(record, NULL), FormatError,
                   "SPGIST UNKNOWN: 144");
        memFree(record);

        TEST_TITLE("Bitmap");
        test_get_relfilenode(ResourceManager_Bitmap, XLOG_BITMAP_INSERT_LOVITEM, true);
        test_get_relfilenode(ResourceManager_Bitmap, XLOG_BITMAP_INSERT_META, true);
        test_get_relfilenode(ResourceManager_Bitmap, XLOG_BITMAP_INSERT_BITMAP_LASTWORDS, true);
        test_get_relfilenode(ResourceManager_Bitmap, XLOG_BITMAP_INSERT_WORDS, true);
        test_get_relfilenode(ResourceManager_Bitmap, XLOG_BITMAP_UPDATEWORD, true);
        test_get_relfilenode(ResourceManager_Bitmap, XLOG_BITMAP_UPDATEWORDS, true);

        record = hrnGpdbCreateXRecord(ResourceManager_Bitmap, 0x90, 100, NULL);
        TEST_ERROR(get_relfilenode(record, NULL), FormatError,
                   "Bitmap UNKNOWN: 144");
        memFree(record);

        TEST_TITLE("Appendonly");
        test_get_relfilenode(ResourceManager_Appendonly, XLOG_APPENDONLY_INSERT, true);
        test_get_relfilenode(ResourceManager_Appendonly, XLOG_APPENDONLY_TRUNCATE, true);

        record = hrnGpdbCreateXRecord(ResourceManager_Appendonly, 0x30, 100, NULL);
        TEST_ERROR(get_relfilenode(record, NULL), FormatError,
                   "Appendonly UNKNOWN: 48");
        memFree(record);

        TEST_TITLE("Resource managers without Relfilenode");
        test_get_relfilenode(ResourceManager_Transaction, 0, false);
        test_get_relfilenode(ResourceManager_CLOG, 0, false);
        test_get_relfilenode(ResourceManager_Database, 0, false);
        test_get_relfilenode(ResourceManager_Tablespace, 0, false);
        test_get_relfilenode(ResourceManager_MultiXact, 0, false);
        test_get_relfilenode(ResourceManager_RelMap, 0, false);
        test_get_relfilenode(ResourceManager_Standby, 0, false);
        test_get_relfilenode(ResourceManager_DistributedLog, 0, false);

        TEST_TITLE("Unsupported hash resource manager");
        record = hrnGpdbCreateXRecord(ResourceManager_Hash, 0, 100, NULL);
        TEST_ERROR(get_relfilenode(record, NULL), FormatError,
                   "Not supported in greenplum. shouldn't be here");
        memFree(record);

        TEST_TITLE("Unknown resource manager");
        record = hrnGpdbCreateXRecord(ResourceManager_Appendonly + 1, 0, 100, NULL);
        TEST_ERROR(get_relfilenode(record, NULL), FormatError,
                   "Unknown resource manager");
        memFree(record);
    }

    if (testBegin("filter"))
    {
        TEST_TITLE("123");
        filter = walFilterNew(jsonReadNew(STRDEF("[\n"
                                                 "  {\n"
                                                 "    \"dbName\": \"db1\",\n"
                                                 "    \"dbOid\": 20000,\n"
                                                 "    \"tables\": [\n"
                                                 "      {\n"
                                                 "        \"tablefqn\": \"t1\",\n"
                                                 "        \"tableOid\": 16384,\n"
                                                 "        \"tablespace\": 1600,\n"
                                                 "        \"relfilenode\": 16384\n"
                                                 "      },\n"
                                                 "      {\n"
                                                 "        \"tablefqn\": \"t2\",\n"
                                                 "        \"tableOid\": 16387,\n"
                                                 "        \"tablespace\": 1601,\n"
                                                 "        \"relfilenode\": 16385\n"
                                                 "      }\n"
                                                 "    ]\n"
                                                 "  },\n"
                                                 "  {\n"
                                                 "    \"dbName\": \"db2\",\n"
                                                 "    \"dbOid\": 20001,\n"
                                                 "    \"tables\": [\n"
                                                 "      {\n"
                                                 "        \"tablefqn\": \"t3\",\n"
                                                 "        \"tableOid\": 16390,\n"
                                                 "        \"tablespace\": 1700,\n"
                                                 "        \"relfilenode\": 16386\n"
                                                 "      }\n"
                                                 "    ]\n"
                                                 "  },\n"
                                                 "  {\n"
                                                 "    \"dbName\": \"db3\",\n"
                                                 "    \"dbOid\": 20002,\n"
                                                 "    \"tables\": []\n"
                                                 "  }\n"
                                                 "]")));
        wal = bufNew(1024 * 1024);
        Buffer *expect_wal = bufNew(1024 * 1024);

        RelFileNode nodes[] = {
            // records that should pass the filter
            {1600, 20000, 16384},
            {1601, 20000, 16385},
            {1700, 20001, 16386},
            // we should filter out all record for this database expect system catalog
            {1700, 20002, 13836},
            // should not be filter out
            {1600,20002,11612},
            // should be filter out
            {1600, 20002, 19922},

            {1800, 20000, 35993},
            {1800, 20000, 25928},
            {2000, 20001, 48457},
            // should pass filter
            {2000, 20001, 5445}
        };

        XRecordInfo records[] = {
            {ResourceManager_Heap, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[0]},
            {ResourceManager_Heap, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[1]},
            {ResourceManager_Heap, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[2]},
            {ResourceManager_Heap, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[3]},
            {ResourceManager_Heap, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[4]},
            {ResourceManager_Heap, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[5]},
            {ResourceManager_Heap, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[6]},
            {ResourceManager_Heap, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[7]},
            {ResourceManager_Heap, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[8]},
            {ResourceManager_Heap, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[9]},
        };

        XRecordInfo records_expected[] = {
            {ResourceManager_Heap, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[0]},
            {ResourceManager_Heap, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[1]},
            {ResourceManager_Heap, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[2]},

            {ResourceManager_Heap, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[3]},
            {ResourceManager_Heap, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[4]},
            {ResourceManager_XLOG, XLOG_NOOP, sizeof(RelFileNode), &nodes[5]},

            {ResourceManager_XLOG, XLOG_NOOP, sizeof(RelFileNode), &nodes[6]},
            {ResourceManager_XLOG, XLOG_NOOP, sizeof(RelFileNode), &nodes[7]},
            {ResourceManager_XLOG, XLOG_NOOP, sizeof(RelFileNode), &nodes[8]},
            {ResourceManager_Heap, XLOG_HEAP_INSERT, sizeof(RelFileNode), &nodes[9]},
        };

        build_wal(wal, records, sizeof(records)/sizeof(records[0]), 0);
        build_wal(expect_wal, records_expected, sizeof(records_expected)/sizeof(records_expected[0]), 0);

        Buffer *result = testFilter(filter, wal, bufSize(wal), bufSize(wal));
        bool cmp = bufEq(expect_wal, result);
        TEST_RESULT_BOOL(cmp, true, "fltred wal is different from expected");
    }

    FUNCTION_HARNESS_RETURN_VOID();
}
