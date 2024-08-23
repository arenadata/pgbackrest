#include "build.auto.h"

#include "walFilter.h"

#include "common/debug.h"
#include "common/log.h"
#include "common/type/object.h"

#include "config/config.h"
#include "greenplumCommon.h"
#include "postgres/interface/crc32.h"
#include "postgres/version.h"
#include "versions/recordProcessGPDB6.h"

typedef struct WalInterface
{
    unsigned int pgVersion;
    StringId fork;

    uint16_t header_magic;
    void (*validXLogRecordHeader)(const XLogRecord *record);
    void (*validXLogRecord)(const XLogRecord *record);
}WalInterface;

static WalInterface interfaces[] = {
    {PG_VERSION_94, CFGOPTVAL_FORK_GPDB, GPDB6_XLOG_PAGE_MAGIC, validXLogRecordHeaderGPDB6, validXLogRecordGPDB6}
};

typedef struct WalFilter
{
    int current_step;
    const unsigned char *page;

    bool is_begin;

    size_t page_offset;
    size_t input_offset;

    XLogPageHeaderData *current_header;

    size_t record_size;
    XLogRecord *record;
    size_t rec_buf_size;
    // Offset to the body of the record on the current page
    size_t header_offset;
    // How many we read of this record
    size_t got_len;
    // Total size of record on current page
    size_t tot_len;

    size_t headers_count;
    size_t headers_size;
    XLogPageHeaderData *headers;

    WalInterface *walInterface;

    // Records count for debug
    int i;

    size_t filter_list_size;
    RelFileNode *filter_list;

    bool done;
    bool same_input;
    bool is_switch_wal;
} WalFilterState;

/***********************************************************************************************************************************
Render as string for logging
***********************************************************************************************************************************/
static void
walFilterToLog(const WalFilterState *const this, StringStatic *const debugLog)
{
    strStcFmt(
        debugLog,
        "{record_num: %d, step: %d is_begin: %s, page_offset: %zu, input_offset: %zu, record_size: %zu, rec_buf_size: %zu,"
        " header_offset: %zu, got_len: %zu, tot_len: %zu}",
        this->i,
        this->current_step,
        this->is_begin ? "true" : "false",
        this->page_offset,
        this->input_offset,
        this->record_size,
        this->rec_buf_size,
        this->header_offset,
        this->got_len,
        this->tot_len
        );
}

#define FUNCTION_LOG_WAL_FILTER_TYPE                                                                                               \
    ZstDecompress *
#define FUNCTION_LOG_WAL_FILTER_FORMAT(value, buffer, bufferSize)                                                                  \
    FUNCTION_LOG_OBJECT_FORMAT(value, walFilterToLog, buffer, bufferSize)

static inline
void
checkOutputSize(Buffer *output, size_t size)
{
    if (bufRemains(output) <= size)
    {
        bufResize(output, bufSize(output) + size);
    }
}

// Reading the next page from the input buffer. If the input buffer is exhausted, remember the current step and returns true.
// In this case, we should exit the process function to get a new input buffer.
static inline
bool
readPage(WalFilterState *this, const Buffer *const input, int step)
{
    if (this->input_offset >= bufUsed(input))
    {
        this->current_step = step;
        this->input_offset = 0;
        this->same_input = false;
        return true;
    }
    this->page = bufPtrConst(input) + this->input_offset;
    this->input_offset += XLOG_BLCKSZ;
    this->page_offset = 0;
    this->current_step = 0;
    this->current_header = (XLogPageHeaderData *) this->page;
    this->page_offset += MAXALIGN(XLogPageHeaderSize(this->current_header));

    // Make sure that WAL belongs to supported Greenplum version, since magic value is different in different versions.
    if (this->current_header->xlp_magic != this->walInterface->header_magic)
    {
        THROW_FMT(FormatError, "wrong page magic");
    }

    if (this->headers_count >= this->headers_size)
    {
        this->headers_size++;
        MEM_CONTEXT_OBJ_BEGIN(this)
        {
            this->headers = memResize(this->headers, sizeof(XLogPageHeaderData) * (this->headers_size));
        }
        MEM_CONTEXT_OBJ_END();
    }
    this->headers[this->headers_count++] = *this->current_header;

    return false;
}

static inline uint32_t
getRecordSize(const unsigned char *buffer)
{
    return ((XLogRecord *) (buffer))->xl_tot_len;
}

static void
walFilterProcess(THIS_VOID, const Buffer *const input, Buffer *const output)
{
    THIS(WalFilterState);

    FUNCTION_LOG_BEGIN(logLevelTrace);
        FUNCTION_LOG_PARAM(WAL_FILTER, this);
        FUNCTION_LOG_PARAM(BUFFER, input);
        FUNCTION_LOG_PARAM(BUFFER, output);
    FUNCTION_LOG_END();

    // Avoid creating local variables before the record is fully read.
    // Since if the input buffer is exhausted, we can exit the function.
    // Use local variables only if you are sure that the read_page function will not be called between assignment and use.
    size_t wrote;

    if (input == NULL)
    {
        // We have an incomplete record at the end, write it as is
        if (this->current_step != 0)
        {
            checkOutputSize(output, this->got_len);
            memcpy(bufRemainsPtr(output), this->record, this->got_len);
            bufUsedInc(output, this->got_len);
        }
        this->done = true;
        FUNCTION_LOG_RETURN_VOID();
        return;
    }

    // When meeting wal switch record, we write the rest of the file as is.
    if (this->is_switch_wal)
    {
        // Copy rest of current page
        size_t to_write = XLOG_BLCKSZ - this->page_offset;
        checkOutputSize(output, to_write);
        memcpy(bufRemainsPtr(output), this->page + this->page_offset, to_write);
        bufUsedInc(output, to_write);
        this->page_offset = 0;

        if (bufUsed(input) > this->input_offset)
        {
            to_write = bufUsed(input) - this->input_offset;
            checkOutputSize(output, to_write);
            memcpy(bufRemainsPtr(output), bufPtrConst(input) + this->input_offset, to_write);
            bufUsedInc(output, to_write);
            this->input_offset = 0;
        }
        this->same_input = false;
        FUNCTION_LOG_RETURN_VOID();
        return;
    }

    // Go back to the place where the reading was interrupted due to the exhaustion of the input buffer.
    switch (this->current_step)
    {
        case 1:
            goto step1;

        case 2:
            goto step2;

        case 3:
            goto step3;
    }

    if (this->page_offset == 0 || this->page_offset >= XLOG_BLCKSZ)
    {
step1:
        if (readPage(this, input, 1))
        {
            FUNCTION_LOG_RETURN_VOID();
            return;
        }
        // The header encountered at the beginning of the record can be written immediately since the data before it will not be
        // changed.
        checkOutputSize(output, XLogPageHeaderSize(this->current_header));
        memcpy(bufRemainsPtr(output), this->current_header, XLogPageHeaderSize(this->current_header));
        bufUsedInc(output, XLogPageHeaderSize(this->current_header));
        this->headers_count = 0;

        if (this->is_begin)
        {
            this->is_begin = false;
            // There may be a piece of record from the previous file at the beginning of the file, write it as it is.
            if (this->current_header->xlp_info & XLP_FIRST_IS_CONTRECORD &&
                !(this->current_header->xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD))
            {
                checkOutputSize(output, MAXALIGN(this->current_header->xlp_rem_len));
                memcpy(bufRemainsPtr(output), this->page + this->page_offset, MAXALIGN(this->current_header->xlp_rem_len));

                bufUsedInc(output, MAXALIGN(this->current_header->xlp_rem_len));
                this->page_offset += MAXALIGN(this->current_header->xlp_rem_len);
            }
        }
        else if (this->current_header->xlp_info & XLP_FIRST_IS_CONTRECORD)
        {
            THROW_FMT(FormatError, "should not be XLP_FIRST_IS_CONTRECORD");
        }
    }

    // Record header can be split between pages but first field xl_tot_len is always on single page
    this->record_size = getRecordSize(this->page + this->page_offset);

    if (this->rec_buf_size < this->record_size)
    {
        MEM_CONTEXT_OBJ_BEGIN(this)
        {
            this->record = memResize(this->record, this->record_size);
        }
        MEM_CONTEXT_OBJ_END();
        this->rec_buf_size = this->record_size;
    }

    memcpy(this->record, this->page + this->page_offset, Min(SizeOfXLogRecord, XLOG_BLCKSZ - this->page_offset));

    this->tot_len = this->record->xl_tot_len;

    // If header is split read rest of the header from next page
    if (SizeOfXLogRecord > Min(SizeOfXLogRecord, XLOG_BLCKSZ - this->page_offset))
    {
        this->got_len = XLOG_BLCKSZ - this->page_offset;
step2:
        if (readPage(this, input, 2))
        {
            FUNCTION_LOG_RETURN_VOID();
            return;
        }

        if (this->current_header->xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD)
        {
            // This record has been overwritten.
            // Write to the output what we managed to read as is, skipping filtering.
            goto write;
        }

        if (!(this->current_header->xlp_info & XLP_FIRST_IS_CONTRECORD))
        {
            THROW_FMT(FormatError, "should be XLP_FIRST_IS_CONTRECORD");
        }

        memcpy(((char *) this->record) + this->got_len, this->page + this->page_offset,
               SizeOfXLogRecord - this->got_len);
        this->tot_len -= this->got_len;
        this->header_offset = SizeOfXLogRecord - this->got_len;
    }
    else
    {
        this->header_offset = SizeOfXLogRecord;
    }
    this->got_len = SizeOfXLogRecord;

    this->walInterface->validXLogRecordHeader(this->record);
    // Read rest of record on this page
    size_t to_read = Min(this->record->xl_tot_len - SizeOfXLogRecord, XLOG_BLCKSZ - this->page_offset - SizeOfXLogRecord);
    memcpy(XLogRecGetData(this->record), this->page + this->page_offset + this->header_offset, to_read);
    this->got_len += to_read;

    // Move pointer to the next record on the page
    this->page_offset += MAXALIGN(this->tot_len);

    // Rest of the record data is on the next page
    while (this->got_len != this->record->xl_tot_len)
    {
step3:
        if (readPage(this, input, 3))
        {
            FUNCTION_LOG_RETURN_VOID();
            return;
        }

        if (this->current_header->xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD)
        {
            // This record has been overwritten.
            // Write to the output what we managed to read as is, skipping filtering.
            goto write;
        }

        if (!(this->current_header->xlp_info & XLP_FIRST_IS_CONTRECORD))
        {
            THROW_FMT(FormatError, "should be XLP_FIRST_IS_CONTRECORD");
        }

        if (this->current_header->xlp_rem_len == 0 ||
            this->tot_len != (this->current_header->xlp_rem_len + this->got_len))
        {
            THROW_FMT(FormatError, "invalid contrecord length: expect: %zu, get %u",
                      this->record->xl_tot_len - this->got_len, this->current_header->xlp_rem_len);
        }

        size_t to_write = Min(this->current_header->xlp_rem_len, XLOG_BLCKSZ - this->page_offset);
        memcpy(((char *) this->record) + this->got_len, this->page + this->page_offset, to_write);
        this->page_offset += MAXALIGN(to_write);
        this->got_len += to_write;
    }
    this->walInterface->validXLogRecord(this->record);
    // From here we can use local variables
    this->current_step = 0;

    if (this->record->xl_rmid == RM_XLOG_ID && this->record->xl_info == XLOG_SWITCH)
    {
        this->is_switch_wal = true;
    }

    RelFileNode *node;
    // If there are no filters, then we do nothing. This is very useful for debug.
    if (this->filter_list_size != 0 && getRelFileNodeGPDB6(this->record, &node) && node->relNode >= 16384)
    {
        bool is_need_to_filter = true;
        // Do filter
        for (size_t i = 0; i < this->filter_list_size; ++i)
        {
            if (this->filter_list[i].dbNode == node->dbNode &&
                this->filter_list[i].relNode == 0)
            {
                is_need_to_filter = true;
                break;
            }
            if (memcmp(node, &this->filter_list[i], sizeof(RelFileNode)) == 0)
            {
                is_need_to_filter = false;
                break;
            }
        }

        if (is_need_to_filter)
        {
            this->record->xl_rmid = RM_XLOG_ID;
            this->record->xl_info = XLOG_NOOP;
            uint32_t crc = crc32cInit();
            crc = crc32cComp(crc, (unsigned char *) XLogRecGetData(this->record), this->record->xl_len);
            crc = crc32cComp(crc, (unsigned char *) this->record, offsetof(XLogRecord, xl_crc));
            crc = crc32cFinish(crc);

            this->record->xl_crc = crc;
        }
    }
write:
    wrote = 0;
    size_t header_i = 0;
    while (this->got_len != wrote)
    {
        size_t space_on_page = XLOG_BLCKSZ - bufUsed(output) % XLOG_BLCKSZ;
        size_t to_write = Min(space_on_page, this->got_len - wrote);
        checkOutputSize(output, to_write);

        memcpy(bufRemainsPtr(output), ((char *) this->record) + wrote, to_write);
        wrote += to_write;

        bufUsedInc(output, to_write);

        // write header
        if (header_i < this->headers_count){
            to_write = sizeof(this->headers[header_i]);
            checkOutputSize(output, MAXALIGN(to_write));
            memcpy(bufRemainsPtr(output), &this->headers[header_i], to_write);
            bufUsedInc(output, to_write);

            size_t align_size = MAXALIGN(to_write) - to_write;
            memset(bufRemainsPtr(output), 0, align_size);
            bufUsedInc(output, align_size);

            header_i++;
        }
    }

    size_t align_size = MAXALIGN(this->got_len) - this->got_len;
    checkOutputSize(output, align_size);
    memset(bufRemainsPtr(output), 0, align_size);
    bufUsedInc(output, align_size);

    this->got_len = 0;

    this->i++;
    this->same_input = true;
    this->headers_count = 0;

    FUNCTION_LOG_RETURN_VOID();
}

static bool
WalFilterDone(const THIS_VOID)
{
    THIS(const WalFilterState);

    FUNCTION_TEST_BEGIN();
        FUNCTION_TEST_PARAM(WAL_FILTER, this);
    FUNCTION_TEST_END();

    FUNCTION_TEST_RETURN(BOOL, this->done);
}

static bool
WalFilterInputSame(const THIS_VOID)
{
    THIS(const WalFilterState);
    FUNCTION_TEST_BEGIN();
        FUNCTION_TEST_PARAM(WAL_FILTER, this);
    FUNCTION_TEST_END();

    FUNCTION_TEST_RETURN(BOOL, this->same_input);
}

static RelFileNode *
appendRelFileNode(RelFileNode *array, size_t *len, RelFileNode node)
{
    if (array)
    {
        array = memResize(array, sizeof(RelFileNode) * (++(*len)));
    }
    else
    {
        (*len)++;
        array = memNew(sizeof(node));
    }

    array[*len - 1] = node;

    return array;
}

FN_EXTERN void
buildFilterList(JsonRead *json, RelFileNode **filter_list, size_t *filter_list_len)
{
    size_t result_len = 0;
    RelFileNode *filter_list_result = NULL;

    jsonReadArrayBegin(json);

    Oid dbOid = 0;
    // Read array of databases
    while (jsonReadTypeNextIgnoreComma(json) != jsonTypeArrayEnd)
    {
        jsonReadObjectBegin(json);

        // Read database info
        while (jsonReadTypeNextIgnoreComma(json) != jsonTypeObjectEnd)
        {
            String *key1 = jsonReadKey(json);
            if (strEqZ(key1, "dbOid"))
            {
                dbOid = jsonReadUInt(json);
            }
            else if (strEqZ(key1, "tables"))
            {
                jsonReadArrayBegin(json);

                size_t table_count = 0;
                // Read tables
                while (jsonReadTypeNextIgnoreComma(json) != jsonTypeArrayEnd)
                {
                    RelFileNode node = {.dbNode = dbOid};
                    jsonReadObjectBegin(json);
                    table_count++;
                    // Read table info
                    while (jsonReadTypeNextIgnoreComma(json) != jsonTypeObjectEnd)
                    {
                        String *key2 = jsonReadKey(json);
                        if (strEqZ(key2, "relfilenode"))
                        {
                            node.relNode = jsonReadUInt(json);
                        }
                        else if (strEqZ(key2, "tablespace"))
                        {
                            node.spcNode = jsonReadUInt(json);
                        }
                        else
                        {
                            jsonReadSkip(json);
                        }
                    }
                    filter_list_result = appendRelFileNode(filter_list_result, &result_len, node);

                    jsonReadObjectEnd(json);
                }

                // If the database does not have any tables specified, then add RelFileNode where spcNode and dbNode are 0.
                if (table_count == 0)
                {
                    RelFileNode node = {
                        .dbNode = dbOid
                    };
                    filter_list_result = appendRelFileNode(filter_list_result, &result_len, node);
                }

                jsonReadArrayEnd(json);
            }
            else
            {
                jsonReadSkip(json);
            }
        }

        jsonReadObjectEnd(json);
    }

    jsonReadArrayEnd(json);

    *filter_list = filter_list_result;
    *filter_list_len = result_len;
}

FN_EXTERN IoFilter *
walFilterNew(unsigned int pgVersion, StringId fork, RelFileNode *filter_list, size_t filter_list_len)
{
    FUNCTION_LOG_BEGIN(logLevelTrace);
    FUNCTION_LOG_END();

    OBJ_NEW_BEGIN(WalFilterState, .childQty = MEM_CONTEXT_QTY_MAX, .allocQty = MEM_CONTEXT_QTY_MAX)
    {
        memset(this, 0, sizeof(WalFilterState));
        this->is_begin = true;
        this->record = memNew(BLCKSZ);
        this->rec_buf_size = BLCKSZ;
        this->headers = memNew(sizeof(XLogPageHeaderData) * XLR_MAX_BKP_BLOCKS);
        this->headers_size = XLR_MAX_BKP_BLOCKS;
        this->filter_list = filter_list;
        this->filter_list_size = filter_list_len;

        for (unsigned int i = 0; i < LENGTH_OF(interfaces); ++i)
        {
            if (interfaces[i].pgVersion == pgVersion && interfaces[i].fork == fork)
            {
                this->walInterface = &interfaces[i];
                break;
            }
        }
        if (this->walInterface == NULL)
        {
            THROW_FMT(VersionNotSupportedError, "WAL filtering is unsupported for this Greenplum version");
        }
    }
    OBJ_NEW_END();

    FUNCTION_LOG_RETURN(
        IO_FILTER,
        ioFilterNewP(
            WAL_FILTER_TYPE, this, NULL,.done = WalFilterDone, .inOut = walFilterProcess,
            .inputSame = WalFilterInputSame));
}
