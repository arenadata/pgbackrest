#include "build.auto.h"

#include "walFilter.h"

#include "common/debug.h"
#include "common/log.h"
#include "common/type/object.h"

#include "postgres/interface/crc32.h"
#include "postgres_common.h"
#include "record_process.h"

typedef struct WalFilter
{
    int current_step;
    const unsigned char *page;

    bool is_begin;

    size_t page_offset;
    size_t input_offset;

    XLogPageHeader header;
    // The total size of the headers encountered during the reading of the record.
    size_t total_headers_size;

    size_t record_size;
    XLogRecord *record;
    size_t rec_buf_size;
    // Offset to the body of the record on the current page
    size_t header_offset;
    // How many we read of this record
    size_t got_len;
    // Total size of record on current page
    size_t tot_len;

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
check_output_size(Buffer *output, size_t size)
{
    if (bufRemains(output) <= size){
        bufResize(output, bufSize(output) + size);
    }
}

// Reading the next page from the input buffer. If the input buffer is exhausted, remember the current step and returns true.
// In this case, we should exit the process function to get a new input buffer.
static inline
bool
read_page(WalFilterState *this, const Buffer *const input, Buffer *const output, int step)
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
    this->header = (XLogPageHeader) this->page;
    this->page_offset += MAXALIGN(XLogPageHeaderSize(this->header));

    // Make sure that WAL belongs to Greenplum 6, since magic value is different in different versions.
    if (this->header->xlp_magic != GPDB6_XLOG_PAGE_MAGIC)
    {
        THROW_FMT(FormatError, "wrong page magic");
    }

    // Copy page header to output
    // Enlarge output buffer if needed
    // Do not increase the amount of used data in the output buffer, as this may lead to premature flush of the output buffer, which
    // Has not yet has record itself.
    check_output_size(output, this->got_len + XLogPageHeaderSize(this->header));

    memcpy(bufRemainsPtr(output) + this->got_len + this->total_headers_size, this->header,
           XLogPageHeaderSize(this->header));
    this->total_headers_size += XLogPageHeaderSize(this->header);
    return false;
}

static inline uint32_t
get_record_size(const unsigned char *buffer)
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
    bool is_override = false; // true when we meet override record.
    size_t wrote;

    if (input == NULL)
    {
        // We have an incomplete record at the end, write it as is
        if (this->current_step != 0)
        {
            check_output_size(output, this->got_len);
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
        check_output_size(output, to_write);
        memcpy(bufRemainsPtr(output), this->page + this->page_offset, to_write);
        bufUsedInc(output, to_write);
        this->page_offset = 0;

        if (bufUsed(input) > this->input_offset)
        {
            to_write = bufUsed(input) - this->input_offset;
            check_output_size(output, to_write);
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
        if (read_page(this, input, output, 1))
        {
            FUNCTION_LOG_RETURN_VOID();
            return;
        }
        // The header encountered at the beginning of the record can be written immediately since the data before it will not be
        // changed.
        bufUsedInc(output, XLogPageHeaderSize(this->header));
        this->total_headers_size = 0;

        if (this->is_begin)
        {
            this->is_begin = false;
            // There may be a piece of record from the previous file at the beginning of the file, write it as it is.
            if (this->header->xlp_info & XLP_FIRST_IS_CONTRECORD &&
                !(this->header->xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD))
            {
                check_output_size(output, MAXALIGN(this->header->xlp_rem_len));
                memcpy(bufRemainsPtr(output), this->page + this->page_offset, MAXALIGN(this->header->xlp_rem_len));

                bufUsedInc(output, MAXALIGN(this->header->xlp_rem_len));
                this->page_offset += MAXALIGN(this->header->xlp_rem_len);
            }
        }
        else if (this->header->xlp_info & XLP_FIRST_IS_CONTRECORD)
        {
            THROW_FMT(FormatError, "should not be XLP_FIRST_IS_CONTRECORD");
        }
    }

    // Record header can be split between pages but first field xl_tot_len is always on single page
    this->record_size = get_record_size(this->page + this->page_offset);

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
        if (read_page(this, input, output, 2))
        {
            FUNCTION_LOG_RETURN_VOID();
            return;
        }

        if (this->header->xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD)
        {
            // This record has been overwritten.
            // Write to the output what we managed to read as is, skipping filtering.
            is_override = true;
            goto write;
        }

        if (!(this->header->xlp_info & XLP_FIRST_IS_CONTRECORD))
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

    ValidXLogRecordHeader(this->record);
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
        if (read_page(this, input, output, 3))
        {
            FUNCTION_LOG_RETURN_VOID();
            return;
        }

        if (this->header->xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD)
        {
            // This record has been overwritten.
            // Write to the output what we managed to read as is, skipping filtering.
            is_override = true;
            goto write;
        }

        if (!(this->header->xlp_info & XLP_FIRST_IS_CONTRECORD))
        {
            THROW_FMT(FormatError, "should be XLP_FIRST_IS_CONTRECORD");
        }

        if (this->header->xlp_rem_len == 0 ||
            this->tot_len != (this->header->xlp_rem_len + this->got_len))
        {
            THROW_FMT(FormatError, "invalid contrecord length: expect: %zu, get %u",
                      this->record->xl_tot_len - this->got_len, this->header->xlp_rem_len);
        }

        size_t to_write = Min(this->header->xlp_rem_len, XLOG_BLCKSZ - this->page_offset);
        memcpy(((char *) this->record) + this->got_len, this->page + this->page_offset, to_write);
        this->page_offset += MAXALIGN(to_write);
        this->got_len += to_write;
    }
    ValidXLogRecord(this->record);
    // From here we can use local variables
    this->current_step = 0;

    if (this->record->xl_rmid == ResourceManager_XLOG && this->record->xl_info == XLOG_SWITCH)
    {
        this->is_switch_wal = true;
    }

    RelFileNode node;
    if (get_relfilenode(this->record, &node) && node.relNode >= 16384)
    {
        bool is_need_to_filter = true;
        // Do filter
        for (size_t i = 0; i < this->filter_list_size; ++i)
        {
            if (this->filter_list[i].dbNode == node.dbNode &&
                this->filter_list[i].relNode == 0)
            {
                is_need_to_filter = true;
                break;
            }
            if (memcmp(&node, &this->filter_list[i], sizeof(RelFileNode)) == 0)
            {
                is_need_to_filter = false;
                break;
            }
        }
        // If there are no filters, then we do nothing. This is very useful for debug.
        if (is_need_to_filter && this->filter_list_size != 0)
        {
            this->record->xl_rmid = ResourceManager_XLOG;
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
    while (this->got_len != wrote)
    {
        size_t space_on_page = XLOG_BLCKSZ - bufUsed(output) % XLOG_BLCKSZ;
        size_t to_write = Min(space_on_page, this->got_len - wrote);
        check_output_size(output, to_write);

        memcpy(bufRemainsPtr(output), ((char *) this->record) + wrote, to_write);
        wrote += to_write;

        bufUsedInc(output, to_write);
        // Skip the header, it has already been written in read_page.
        if (this->got_len != wrote || is_override)
        {
            size_t header_size = SizeOfXLogShortPHD;
            bufUsedInc(output, header_size);
            is_override = false;
        }
    }

    size_t align_size = MAXALIGN(this->got_len) - this->got_len;
    check_output_size(output, align_size);
    memset(bufRemainsPtr(output), 0, align_size);
    bufUsedInc(output, align_size);

    this->got_len = 0;
    this->total_headers_size = 0;

    this->i++;
    this->same_input = true;

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
append_relfilenode(RelFileNode *array, size_t *len, RelFileNode node)
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
build_filter_list(JsonRead *json, RelFileNode **filter_list, size_t *filter_list_len)
{
    size_t result_len = 0;
    RelFileNode *filter_list_result = NULL;

    jsonReadArrayBegin(json);

    RelFileNode node = {0};
    Oid dbOid = 0;
    // Read array of databases
    while (jsonReadTypeNextIgnoreComma(json) != jsonTypeArrayEnd)
    {
        jsonReadObjectBegin(json);

        // Read database info
        while (jsonReadTypeNextIgnoreComma(json) != jsonTypeObjectEnd)
        {
            String *key1 = jsonReadKey(json);
            if (strCmpZ(key1, "dbOid") == 0)
            {
                dbOid = jsonReadUInt(json);
            }
            else if (strCmpZ(key1, "tables") == 0)
            {
                jsonReadArrayBegin(json);

                size_t table_count = 0;
                // Read tables
                while (jsonReadTypeNextIgnoreComma(json) != jsonTypeArrayEnd)
                {
                    jsonReadObjectBegin(json);
                    table_count++;
                    // Read table info
                    while (jsonReadTypeNextIgnoreComma(json) != jsonTypeObjectEnd)
                    {
                        String *key2 = jsonReadKey(json);
                        if (strCmpZ(key2, "relfilenode") == 0)
                        {
                            node.relNode = jsonReadUInt(json);
                        }
                        else if (strCmpZ(key2, "tablespace") == 0)
                        {
                            node.spcNode = jsonReadUInt(json);
                        }
                        else
                        {
                            jsonReadSkip(json);
                        }
                    }
                    node.dbNode = dbOid;
                    filter_list_result = append_relfilenode(filter_list_result, &result_len, node);
                    memset(&node, 0, sizeof(node));

                    jsonReadObjectEnd(json);
                }

                // If the database does not have any tables specified, then add RelFileNode where spcNode and dbNode are 0.
                if (table_count == 0)
                {
                    node.dbNode = dbOid;
                    filter_list_result = append_relfilenode(filter_list_result, &result_len, node);
                    memset(&node, 0, sizeof(node));
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
walFilterNew(JsonRead *json)
{
    FUNCTION_LOG_BEGIN(logLevelTrace);
    FUNCTION_LOG_END();

    OBJ_NEW_BEGIN(WalFilterState, .childQty = MEM_CONTEXT_QTY_MAX, .allocQty = MEM_CONTEXT_QTY_MAX)
    {
        memset(this, 0, sizeof(WalFilterState));
        this->is_begin = true;
        this->record = memNew(BLCKSZ);
        this->rec_buf_size = BLCKSZ;
        build_filter_list(json, &this->filter_list, &this->filter_list_size);
    }
    OBJ_NEW_END();

    FUNCTION_LOG_RETURN(
        IO_FILTER,
        ioFilterNewP(
            WAL_FILTER_TYPE, this, NULL,.done = WalFilterDone, .inOut = walFilterProcess,
            .inputSame = WalFilterInputSame));
}
