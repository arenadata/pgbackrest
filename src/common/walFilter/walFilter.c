#include "build.auto.h"

#include "walFilter.h"

#include "common/debug.h"
#include "common/log.h"
#include "common/type/object.h"

#include "common/compress/helper.h"
#include "common/partialRestore.h"
#include "config/config.h"
#include "postgres/interface/crc32.h"
#include "postgres/version.h"
#include "postgresCommon.h"
#include "storage/helper.h"
#include "versions/recordProcessGPDB6.h"

typedef enum
{
    noStep, // This means that the process of reading the record is in an uninterrupted state.
    stepBeginOfRecord,
    stepReadHeader,
    stepReadBody,
} ReadStep;

typedef struct WalInterface
{
    unsigned int pgVersion;
    StringId fork;

    uint16_t header_magic;
    void (*validXLogRecordHeader)(const XLogRecord *record, PgPageSize heapPageSize);
    void (*validXLogRecord)(const XLogRecord *record, PgPageSize heapPageSize);
} WalInterface;

static WalInterface interfaces[] = {
    {PG_VERSION_94, CFGOPTVAL_FORK_GPDB, GPDB6_XLOG_PAGE_MAGIC, validXLogRecordHeaderGPDB6, validXLogRecordGPDB6}
};

typedef struct WalFilter
{
    ReadStep currentStep;
    const unsigned char *page;

    PgPageSize heapPageSize;
    PgPageSize walPageSize;
    uint32_t segSize;

    bool is_begin;

    size_t page_offset;
    size_t input_offset;
    XLogRecPtr recPtr;

    XLogPageHeaderData *current_header;

    XLogRecord *record;
    size_t rec_buf_size;
    // Offset to the body of the record on the current page
    size_t header_offset;
    // How many we read of this record
    size_t got_len;
    // Total size of record on current page
    size_t tot_len;

    List *headers;

    WalInterface *walInterface;

    const ArchiveGetFile *archiveInfo;

    // Records count for debug
    uint32_t i;

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
        "{record_num: %u, step: %u is_begin: %s, page_offset: %zu, input_offset: %zu, rec_buf_size: %zu,"
        " header_offset: %zu, got_len: %zu, tot_len: %zu}",
        this->i,
        this->currentStep,
        this->is_begin ? "true" : "false",
        this->page_offset,
        this->input_offset,
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
checkOutputSize(Buffer *const output, const size_t size)
{
    if (bufRemains(output) <= size)
    {
        bufResize(output, bufUsed(output) + size);
    }
}

// Reading the next page from the input buffer. If the input buffer is exhausted, remember the current step and returns true.
// In this case, we should exit the process function to get a new input buffer.
static inline
bool
readPage(WalFilterState *const this, const Buffer *const input, const ReadStep step)
{
    if (this->input_offset >= bufUsed(input))
    {
        ASSERT(step != 0);
        this->currentStep = step;
        this->input_offset = 0;
        this->same_input = false;
        return false;
    }
    this->page = bufPtrConst(input) + this->input_offset;
    this->input_offset += this->walPageSize;
    this->page_offset = 0;
    this->currentStep = noStep;
    this->current_header = (XLogPageHeaderData *) this->page;
    this->page_offset += XLogPageHeaderSize(this->current_header);

    // Make sure that WAL belongs to supported Postgres version, since magic value is different in different versions.
    if (this->current_header->xlp_magic != this->walInterface->header_magic)
    {
        THROW_FMT(FormatError, "%s - wrong page magic", strZ(pgLsnToStr(this->recPtr)));
    }

    lstAdd(this->headers, this->current_header);

    return true;
}

static inline uint32_t
getRecordSize(const unsigned char *const buffer)
{
    return ((XLogRecord *) (buffer))->xl_tot_len;
}

static bool
readRecord(WalFilterState *const this, const Buffer *const input)
{
    // Go back to the place where the reading was interrupted due to the exhaustion of the input buffer.
    switch (this->currentStep)
    {
        case noStep:
            // noting to do
            break;

        case stepBeginOfRecord:
            goto stepBeginOfRecord;

        case stepReadHeader:
            goto stepReadHeader;

        case stepReadBody:
            goto stepReadBody;
    }

    if (this->page_offset == 0 || this->page_offset >= this->walPageSize)
    {
stepBeginOfRecord:
        if (!readPage(this, input, stepBeginOfRecord))
        {
            return false;
        }

        if (this->is_begin)
        {
            this->is_begin = false;
            // There may be an unfinished record from the previous file at the beginning of the file. Just skip it.
            // We should have handled it elsewhere.
            if (this->current_header->xlp_info & XLP_FIRST_IS_CONTRECORD &&
                !(this->current_header->xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD))
            {
                this->page_offset += MAXALIGN(this->current_header->xlp_rem_len);
            }
        }
        else if (this->current_header->xlp_info & XLP_FIRST_IS_CONTRECORD)
        {
            THROW_FMT(FormatError, "%s - should not be XLP_FIRST_IS_CONTRECORD", strZ(pgLsnToStr(this->recPtr)));
        }
    }

    // Record header can be split between pages but first field xl_tot_len is always on single page
    uint32_t record_size = getRecordSize(this->page + this->page_offset);

    if (this->rec_buf_size < record_size)
    {
        MEM_CONTEXT_OBJ_BEGIN(this)
        {
            this->record = memResize(this->record, record_size);
        }
        MEM_CONTEXT_OBJ_END();
        this->rec_buf_size = record_size;
    }

    memcpy(this->record, this->page + this->page_offset, Min(SizeOfXLogRecord, this->walPageSize - this->page_offset));

    this->tot_len = this->record->xl_tot_len;

    // If header is split read rest of the header from next page
    if (SizeOfXLogRecord > this->walPageSize - this->page_offset)
    {
        this->got_len = this->walPageSize - this->page_offset;
stepReadHeader:
        if (!readPage(this, input, stepReadHeader))
        {
            return false;
        }

        if (this->current_header->xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD)
        {
            // This record has been overwritten.
            // Write to the output what we managed to read as is, skipping filtering.
            return true;
        }

        if (!(this->current_header->xlp_info & XLP_FIRST_IS_CONTRECORD))
        {
            THROW_FMT(FormatError, "%s - should be XLP_FIRST_IS_CONTRECORD", strZ(pgLsnToStr(this->recPtr)));
        }

        memcpy(((char *) this->record) + this->got_len, this->page + this->page_offset, SizeOfXLogRecord - this->got_len);
        this->tot_len -= this->got_len;
        this->header_offset = SizeOfXLogRecord - this->got_len;
    }
    else
    {
        this->header_offset = SizeOfXLogRecord;
    }
    this->got_len = SizeOfXLogRecord;

    this->walInterface->validXLogRecordHeader(this->record, this->heapPageSize);
    // Read rest of the record on this page
    size_t to_read = Min(this->record->xl_tot_len - SizeOfXLogRecord, this->walPageSize - this->page_offset - SizeOfXLogRecord);
    memcpy(XLogRecGetData(this->record), this->page + this->page_offset + this->header_offset, to_read);
    this->got_len += to_read;

    // Move pointer to the next record on the page
    this->page_offset += MAXALIGN(this->tot_len);

    // Rest of the record data is on the next page
    while (this->got_len != this->record->xl_tot_len)
    {
stepReadBody:
        if (!readPage(this, input, stepReadBody))
        {
            return false;
        }

        if (this->current_header->xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD)
        {
            // This record has been overwritten.
            // Write to the output what we managed to read as is, skipping filtering.
            return true;
        }

        if (!(this->current_header->xlp_info & XLP_FIRST_IS_CONTRECORD))
        {
            THROW_FMT(FormatError, "%s - should be XLP_FIRST_IS_CONTRECORD", strZ(pgLsnToStr(this->recPtr)));
        }

        if (this->current_header->xlp_rem_len == 0 ||
            this->tot_len != (this->current_header->xlp_rem_len + this->got_len))
        {
            THROW_FMT(FormatError, "%s - invalid contrecord length: expect: %zu, get %u", strZ(pgLsnToStr(this->recPtr)),
                      this->record->xl_tot_len - this->got_len, this->current_header->xlp_rem_len);
        }

        size_t to_write = Min(this->current_header->xlp_rem_len, this->walPageSize - this->page_offset);
        memcpy(((char *) this->record) + this->got_len, this->page + this->page_offset, to_write);
        this->page_offset += MAXALIGN(to_write);
        this->got_len += to_write;
    }
    this->walInterface->validXLogRecord(this->record, this->heapPageSize);

    if (this->record->xl_rmid == RM_XLOG_ID && this->record->xl_info == XLOG_SWITCH)
    {
        this->is_switch_wal = true;
    }
    this->i++;
    return true;
}

static void
filterRecord(WalFilterState *const this)
{
    // In the case of overwrite contrecord, we do not need to try to filter it, since the record may not have a body at all.
    if (this->got_len != this->record->xl_tot_len)
    {
        return;
    }

    const RelFileNode *node = getRelFileNodeGPDB6(this->record);
    if (!node)
    {
        return;
    }

    bool isPassTheFilter = false;

    // isRelationNeeded can allocate memory on the first call. Therefore, we switch the context.
    MEM_CONTEXT_OBJ_BEGIN(this)
    isPassTheFilter = isRelationNeeded(node->dbNode, node->spcNode, node->relNode);
    MEM_CONTEXT_OBJ_END();

    if (isPassTheFilter)
    {
        return;
    }

    this->record->xl_rmid = RM_XLOG_ID;
    this->record->xl_info = XLOG_NOOP;
    uint32_t crc = crc32cInit();
    crc = crc32cComp(crc, (unsigned char *) XLogRecGetData(this->record), this->record->xl_len);
    crc = crc32cComp(crc, (unsigned char *) this->record, offsetof(XLogRecord, xl_crc));
    crc = crc32cFinish(crc);

    this->record->xl_crc = crc;
}

static void
writeRecord(WalFilterState *const this, Buffer *const output)
{
    uint32_t header_i = 0;
    if (this->recPtr % this->walPageSize == 0)
    {
        ASSERT(!lstEmpty(this->headers));
        const XLogPageHeaderData *const header = lstGet(this->headers, header_i);
        const size_t to_write = XLogPageHeaderSize(header);

        bufCatC(output, (const unsigned char *) header, 0, to_write);

        this->recPtr += to_write;
        header_i++;
    }

    size_t wrote = 0;
    while (this->got_len != wrote)
    {
        const size_t space_on_page = this->walPageSize - bufUsed(output) % this->walPageSize;
        const size_t to_write = Min(space_on_page, this->got_len - wrote);

        bufCatC(output, (const unsigned char *) this->record, wrote, to_write);

        wrote += to_write;

        // write header
        if (header_i < lstSize(this->headers))
        {
            bufCatC(output, lstGet(this->headers, header_i), 0, SizeOfXLogShortPHD);

            this->recPtr += SizeOfXLogShortPHD;
            header_i++;
        }
    }

    const size_t align_size = MAXALIGN(this->got_len) - this->got_len;
    checkOutputSize(output, align_size);
    memset(bufRemainsPtr(output), 0, align_size);
    bufUsedInc(output, align_size);

    this->recPtr += this->tot_len;
    this->got_len = 0;
}

static const StorageRead *
getNearWal (WalFilterState *const this, bool isNext)
{
    const String *walSegment = NULL;
    const TimeLineID timeLine = this->current_header->xlp_tli;
    uint64_t segno = this->current_header->xlp_pageaddr / this->segSize;
    const String *const path = strNewFmt("%08X%08X", timeLine, (uint32) (segno / XLogSegmentsPerXLogId(this->segSize)));

    const StringList *const segmentList = storageListP(
        storageRepoIdx(this->archiveInfo->repoIdx),
        strNewFmt(STORAGE_REPO_ARCHIVE "/%s/%s", strZ(this->archiveInfo->archiveId), strZ(path)),
        .expression = strNewFmt("^[0-f]{24}-[0-f]{40}" COMPRESS_TYPE_REGEXP "{0,1}$"));

    if (strLstEmpty(segmentList))
    {
        // an exotic case where we couldn't even find the current file.
        THROW(FormatError, "no WAL files were found in the repository");
    }

    uint64_t segnoDiff = UINT64_MAX;
    for (uint32_t i = 0; i < strLstSize(segmentList); i++)
    {
        const String *const file = strSubN(strLstGet(segmentList, i), 0, 24);
        TimeLineID tli;
        uint64_t fileSegNo = 0;
        XLogFromFileName(strZ(file), &tli, &fileSegNo, this->segSize);

        if (isNext)
        {
            if (fileSegNo - segno < segnoDiff && fileSegNo > segno)
            {
                segnoDiff = fileSegNo - segno;
                walSegment = strLstGet(segmentList, i);
            }
        }
        else
        {
            if (segno - fileSegNo < segnoDiff && fileSegNo < segno)
            {
                segnoDiff = segno - fileSegNo;
                walSegment = strLstGet(segmentList, i);
            }
        }
    }

    // current file is oldest/newest
    if (segnoDiff == UINT64_MAX)
    {
        return NULL;
    }

    const bool compressible =
        this->archiveInfo->cipherType == cipherTypeNone && compressTypeFromName(this->archiveInfo->file) == compressTypeNone;

    const StorageRead *const storageRead = storageNewReadP(
        storageRepoIdx(0),
        strNewFmt(STORAGE_REPO_ARCHIVE "/%s/%s", strZ(this->archiveInfo->archiveId), strZ(walSegment)),
        .compressible = compressible);

    buildArchiveGetPipeLine(ioReadFilterGroup(storageReadIo(storageRead)), this->archiveInfo);
    return storageRead;
}

static bool
readBeginOfRecord(WalFilterState *const this)
{
    bool result = false;
    MEM_CONTEXT_TEMP_BEGIN();

    const StorageRead *const storageRead = getNearWal(this, false);

    if (storageRead == NULL)
    {
        goto end;
    }

    ioReadOpen(storageReadIo(storageRead));
    this->is_begin = true;
    this->input_offset = 0;
    this->page_offset = 0;

    Buffer *const buffer = bufNew(this->walPageSize);
    size_t size = ioRead(storageReadIo(storageRead), buffer);
    bufUsedSet(buffer, size);

    while (true)
    {
        if (!readRecord(this, buffer))
        {
            if (ioReadEof(storageReadIo(storageRead)))
            {
                if (this->got_len >= offsetof(XLogRecord, xl_rmid) + SIZE_OF_STRUCT_MEMBER(XLogRecord, xl_rmid))
                {
                    // xl_info and xl_rmid is in prev file. Nothing to do
                    result = false;
                    break;
                }

                result = true;
                break;
            }
            else
            {
                bufUsedZero(buffer);
                size = ioRead(storageReadIo(storageRead), buffer);
                bufUsedSet(buffer, size);
            }
        }
        lstClearFast(this->headers);
    }
    this->page = NULL;
    ioReadClose(storageReadIo(storageRead));
end:
    MEM_CONTEXT_TEMP_END();
    return result;
}

static void
getEndOfRecord(WalFilterState *const this)
{
    MEM_CONTEXT_TEMP_BEGIN();

    const StorageRead *const storageRead = getNearWal(this, true);

    if (storageRead == NULL)
    {
        THROW_FMT(FormatError, "The file with the end of the %s record is missing", strZ(pgLsnToStr(this->recPtr)));
    }

    ioReadOpen(storageReadIo(storageRead));

    Buffer *const buffer = bufNew(this->walPageSize);
    size_t size = ioRead(storageReadIo(storageRead), buffer);
    bufUsedSet(buffer, size);
    while (this->got_len != this->record->xl_tot_len)
    {
        if (!readRecord(this, buffer))
        {
            if (ioReadEof(storageReadIo(storageRead)))
            {
                THROW_FMT(FormatError, "%s - Unexpected WAL end", strZ(pgLsnToStr(this->recPtr)));
            }

            bufUsedZero(buffer);
            size = ioRead(storageReadIo(storageRead), buffer);
            bufUsedSet(buffer, size);
        }
    }
    ioReadClose(storageReadIo(storageRead));
    MEM_CONTEXT_TEMP_END();
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

    ASSERT(this != NULL);
    ASSERT(output != NULL);
    ASSERT(input == NULL || bufUsed(input) % this->walPageSize == 0);

    // Avoid creating local variables before the record is fully read.
    // Since if the input buffer is exhausted, we can exit the function.

    if (input == NULL)
    {
        // We have an incomplete record at the end
        if (this->currentStep != 0)
        {
            const size_t size_on_page = this->got_len;

            // if xl_info and xl_rmid of the header is in current file then read end of record from next file if it exits
            if (this->got_len >= offsetof(XLogRecord, xl_rmid) + SIZE_OF_STRUCT_MEMBER(XLogRecord, xl_rmid))
            {
                getEndOfRecord(this);
                filterRecord(this);
            }

            bufCatC(output, (const unsigned char *) this->record, 0, size_on_page);
        }
        this->done = true;
        goto end;
    }

    if (this->is_begin)
    {
        this->is_begin = false;

        readPage(this, input, noStep);
        this->recPtr = this->current_header->xlp_pageaddr;
        if (this->current_header->xlp_info & XLP_FIRST_IS_CONTRECORD && !(this->current_header->xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD))
        {
            if (readBeginOfRecord(this))
            {
                // Remember how much we read from the prev file in order to skip this size when writing.
                const size_t offset = this->got_len;
                this->input_offset = 0;
                lstClearFast(this->headers);
                if (!readRecord(this, input))
                {
                    // Since we are at the very beginning of the file, let's assume that the current input buffer is enough to fully
                    // read this record.
                    THROW_FMT(FormatError, "%s - record is too big", strZ(pgLsnToStr(this->recPtr)));
                }
                filterRecord(this);

                ASSERT(offset <= 8);
                memmove(this->record, ((char *) this->record) + offset, this->record->xl_tot_len - offset);
                this->got_len -= offset;

                ASSERT(this->recPtr == this->current_header->xlp_pageaddr);
                writeRecord(this, output);
                this->i++;
                this->same_input = true;
                lstClearFast(this->headers);
                goto end;
            } // else

            this->input_offset = 0;
            readPage(this, input, noStep);
            ASSERT(this->recPtr == this->current_header->xlp_pageaddr);
            bufCatC(output, (const unsigned char *) this->current_header, 0, SizeOfXLogLongPHD);

            this->recPtr += SizeOfXLogLongPHD;
            lstClearFast(this->headers);

            // The header that needs to be modified is in another file or not exists. Just copy it as is.
            bufCatC(output, this->page, this->page_offset, MAXALIGN(this->current_header->xlp_rem_len));

            this->page_offset += MAXALIGN(this->current_header->xlp_rem_len);
            this->recPtr += MAXALIGN(this->current_header->xlp_rem_len);
        }
    }

    // When meeting wal switch record, we write the rest of the file as is.
    if (this->is_switch_wal)
    {
        // Copy rest of current page
        size_t to_write = this->walPageSize - this->page_offset;

        bufCatC(output, this->page, this->page_offset, to_write);
        this->page_offset = 0;

        if (bufUsed(input) > this->input_offset)
        {
            to_write = bufUsed(input) - this->input_offset;

            bufCatC(output, bufPtrConst(input), this->input_offset, to_write);
            this->input_offset = 0;
        }
        this->same_input = false;
        goto end;
    }

    if (!readRecord(this, input))
    {
        goto end;
    }

    filterRecord(this);

    writeRecord(this, output);

    this->i++;
    this->same_input = true;
    lstClearFast(this->headers);
end:
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

FN_EXTERN IoFilter *
walFilterNew(const StringId fork, const PgControl pgControl, const ArchiveGetFile *const archiveInfo)
{
    FUNCTION_LOG_BEGIN(logLevelTrace);
    FUNCTION_LOG_END();

    OBJ_NEW_BEGIN(WalFilterState, .childQty = MEM_CONTEXT_QTY_MAX, .allocQty = MEM_CONTEXT_QTY_MAX)
    {
        memset(this, 0, sizeof(WalFilterState));
        this->is_begin = true;
        this->record = memNew(pgControl.pageSize);
        this->rec_buf_size = pgControl.pageSize;
        this->headers = lstNewP(SizeOfXLogLongPHD);
        this->archiveInfo = archiveInfo;
        this->heapPageSize = pgControl.pageSize;
        this->walPageSize = pgControl.walPageSize;
        this->segSize = pgControl.walSegmentSize;

        for (unsigned int i = 0; i < LENGTH_OF(interfaces); ++i)
        {
            if (interfaces[i].pgVersion == pgControl.version && interfaces[i].fork == fork)
            {
                this->walInterface = &interfaces[i];
                break;
            }
        }
        if (this->walInterface == NULL)
        {
            THROW(VersionNotSupportedError, "WAL filtering is unsupported for this Postgres version");
        }
    }
    OBJ_NEW_END();

    FUNCTION_LOG_RETURN(
        IO_FILTER,
        ioFilterNewP(
            WAL_FILTER_TYPE, this, NULL,.done = WalFilterDone, .inOut = walFilterProcess,
            .inputSame = WalFilterInputSame));
}
