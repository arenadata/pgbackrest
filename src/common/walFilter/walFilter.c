#include "build.auto.h"

#include "walFilter.h"

#include "common/debug.h"
#include "common/log.h"
#include "common/type/object.h"

#include "common/compress/helper.h"
#include "common/crypto/cipherBlock.h"
#include "common/partialRestore.h"
#include "config/config.h"
#include "greenplumCommon.h"
#include "postgres/interface/crc32.h"
#include "postgres/version.h"
#include "storage/helper.h"
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
        "{record_num: %u, step: %d is_begin: %s, page_offset: %zu, input_offset: %zu, record_size: %zu, rec_buf_size: %zu,"
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
checkOutputSize(Buffer *const output, const size_t size)
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
readPage(WalFilterState *const this, const Buffer *const input, const int step)
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

    lstAdd(this->headers, this->current_header);

    return false;
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
    // Read rest of the record on this page
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

    this->current_step = 0;

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

    RelFileNode *node;
    // Pass through the records that are related to the system catalog or system databases (template1, template0 and postgres)
    if (getRelFileNodeGPDB6(this->record, &node))
    {
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
}

static void
writeRecord(WalFilterState *const this, Buffer *const output)
{
    uint32_t header_i = 0;
    if (bufUsed(output) % XLOG_BLCKSZ == 0)
    {
        ASSERT(!lstEmpty(this->headers));
        const XLogPageHeaderData *const header = lstGet(this->headers, header_i);
        const size_t to_write = XLogPageHeaderSize(header);
        checkOutputSize(output, to_write);
        memcpy(bufRemainsPtr(output), header, to_write);
        bufUsedInc(output, to_write);
        header_i++;
    }

    size_t wrote = 0;
    while (this->got_len != wrote)
    {
        const size_t space_on_page = XLOG_BLCKSZ - bufUsed(output) % XLOG_BLCKSZ;
        size_t to_write = Min(space_on_page, this->got_len - wrote);
        checkOutputSize(output, to_write);

        memcpy(bufRemainsPtr(output), ((char *) this->record) + wrote, to_write);
        wrote += to_write;

        bufUsedInc(output, to_write);

        // write header
        if (header_i < lstSize(this->headers))
        {
            to_write = SizeOfXLogShortPHD;
            checkOutputSize(output, to_write);
            memcpy(bufRemainsPtr(output), lstGet(this->headers, header_i), to_write);
            bufUsedInc(output, to_write);

            header_i++;
        }
    }

    const size_t align_size = MAXALIGN(this->got_len) - this->got_len;
    checkOutputSize(output, align_size);
    memset(bufRemainsPtr(output), 0, align_size);
    bufUsedInc(output, align_size);

    this->got_len = 0;
}

static bool
readBeginOfRecord(WalFilterState *const main_state)
{
    bool result = false;
    MEM_CONTEXT_TEMP_BEGIN();

    const String *walSegment = NULL;
    const TimeLineID timeLine = main_state->current_header->xlp_tli;
    uint64_t segno = main_state->current_header->xlp_pageaddr / XLOG_SEG_SIZE;
    const String *const path = strNewFmt("%08X%08X", timeLine, (uint32) (segno / XLogSegmentsPerXLogId));

    const StringList *const segmentList = storageListP(
        storageRepoIdx(main_state->archiveInfo->repoIdx),
        strNewFmt(STORAGE_REPO_ARCHIVE "/%s/%s", strZ(main_state->archiveInfo->archiveId), strZ(path)),
        .expression = strNewFmt("^[0-f]{24}-[0-f]{40}" COMPRESS_TYPE_REGEXP "{0,1}$"));

    if (strLstEmpty(segmentList))
    {
        // an exotic case where we couldn't even find the current file.
        THROW(FormatError, "no WAL files were found in the repository");
    }

    uint64_t segno_diff = UINT64_MAX;
    for (uint32_t i = 0; i < strLstSize(segmentList); i++)
    {
        const String *const file = strSubN(strLstGet(segmentList, i), 0, 24);
        TimeLineID tli;
        uint64_t fileSegNo = 0;
        XLogFromFileName(strZ(file), &tli, &fileSegNo);

        if (segno - fileSegNo < segno_diff && fileSegNo < segno)
        {
            segno_diff = segno - fileSegNo;
            walSegment = strLstGet(segmentList, i);
        }
    }

    // current file is oldest
    if (segno_diff == UINT64_MAX)
    {
        memContextSwitchBack();
        memContextDiscard();
        return false;
    }

    const StorageRead *const storageRead = storageNewReadP(
        storageRepoIdx(0),
        strNewFmt(STORAGE_REPO_ARCHIVE "/%s/%s", strZ(main_state->archiveInfo->archiveId), strZ(walSegment)));

    // If there is a cipher then add the decrypt filter
    if (main_state->archiveInfo->cipherType != cipherTypeNone)
    {
        ioFilterGroupAdd(
            ioReadFilterGroup(storageReadIo(storageRead)),
            cipherBlockNewP(cipherModeDecrypt, main_state->archiveInfo->cipherType, BUFSTR(main_state->archiveInfo->cipherPassArchive)));
    }

    // If file is compressed then add the decompression filter
    const CompressType compressType = compressTypeFromName(walSegment);

    if (compressType != compressTypeNone)
    {
        ioFilterGroupAdd(ioReadFilterGroup(storageReadIo(storageRead)), decompressFilterP(compressType));
    }

    OBJ_NEW_BEGIN(WalFilterState, .childQty = MEM_CONTEXT_QTY_MAX, .allocQty = MEM_CONTEXT_QTY_MAX)
    {
        memset(this, 0, sizeof(WalFilterState));
        this->is_begin = true;
        this->record = memNew(BLCKSZ);
        this->rec_buf_size = BLCKSZ;
        this->headers = lstNewP(SizeOfXLogLongPHD);

        this->walInterface = &interfaces[0];
    }
    OBJ_NEW_END();

    ioReadOpen(storageReadIo(storageRead));

    Buffer *const buffer = bufNew(XLOG_BLCKSZ);
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

                memcpy(main_state->record, this->record, this->got_len);
                main_state->got_len = this->got_len;
                main_state->current_step = this->current_step;
                main_state->tot_len = this->record->xl_tot_len;

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
    MEM_CONTEXT_TEMP_END();
    return result;
}

static bool
getEndOfRecord(WalFilterState *const this)
{
    MEM_CONTEXT_TEMP_BEGIN();

    const String *walSegment = NULL;
    const TimeLineID timeLine = this->current_header->xlp_tli;
    uint64_t segno = this->current_header->xlp_pageaddr / XLOG_SEG_SIZE;
    const String *const path = strNewFmt("%08X%08X", timeLine, (uint32) (segno / XLogSegmentsPerXLogId));

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
        const String *file = strSubN(strLstGet(segmentList, i), 0, 24);
        TimeLineID tli;
        uint64_t fileSegNo = 0;
        XLogFromFileName(strZ(file), &tli, &fileSegNo);

        if (fileSegNo - segno < segnoDiff && fileSegNo > segno)
        {
            segnoDiff = fileSegNo - segno;
            walSegment = strLstGet(segmentList, i);
        }
    }

    // current file is newest
    if (segnoDiff == UINT64_MAX)
    {
        memContextSwitchBack();
        memContextDiscard();
        return false;
    }

    const StorageRead *const storageRead = storageNewReadP(
        storageRepoIdx(0),
        strNewFmt(STORAGE_REPO_ARCHIVE "/%s/%s", strZ(this->archiveInfo->archiveId), strZ(walSegment)));

    // If there is a cipher then add the decrypt filter
    if (this->archiveInfo->cipherType != cipherTypeNone)
    {
        ioFilterGroupAdd(
            ioReadFilterGroup(storageReadIo(storageRead)),
            cipherBlockNewP(cipherModeDecrypt, this->archiveInfo->cipherType, BUFSTR(this->archiveInfo->cipherPassArchive)));
    }
    // If file is compressed then add the decompression filter
    const CompressType compressType = compressTypeFromName(walSegment);

    if (compressType != compressTypeNone)
    {
        ioFilterGroupAdd(ioReadFilterGroup(storageReadIo(storageRead)), decompressFilterP(compressType));
    }

    ioReadOpen(storageReadIo(storageRead));

    Buffer *const buffer = bufNew(XLOG_BLCKSZ);
    size_t size = ioRead(storageReadIo(storageRead), buffer);
    bufUsedSet(buffer, size);
    while (this->got_len != this->record->xl_tot_len)
    {
        if (!readRecord(this, buffer))
        {
            if (ioReadEof(storageReadIo(storageRead)))
            {
                THROW(FormatError, "Unexpected WAL end");
            }

            bufUsedZero(buffer);
            size = ioRead(storageReadIo(storageRead), buffer);
            bufUsedSet(buffer, size);
        }
    }

    MEM_CONTEXT_TEMP_END();
    return true;
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

    if (input == NULL)
    {
        // We have an incomplete record at the end
        if (this->current_step != 0)
        {
            const size_t size_on_page = this->got_len;

            // if xl_info and xl_rmid of the header is in current file then read end of record from next file if it exits
            if (this->got_len >= offsetof(XLogRecord, xl_rmid) + SIZE_OF_STRUCT_MEMBER(XLogRecord, xl_rmid) && getEndOfRecord(this))
            {
                filterRecord(this);
            }

            checkOutputSize(output, size_on_page);
            memcpy(bufRemainsPtr(output), this->record, size_on_page);
            bufUsedInc(output, size_on_page);
        }
        this->done = true;
        FUNCTION_LOG_RETURN_VOID();
        return;
    }

    if (this->is_begin)
    {
        this->is_begin = false;
        readPage(this, input, 0);

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
                    THROW(FormatError, "record is too big");
                }
                filterRecord(this);

                memmove(this->record, ((char *) this->record) + offset, this->record->xl_tot_len - offset);
                this->got_len -= offset;

                writeRecord(this, output);
                this->i++;
                this->same_input = true;
                lstClearFast(this->headers);
                FUNCTION_LOG_RETURN_VOID();
                return;
            }
            else
            {
                checkOutputSize(output, SizeOfXLogLongPHD);
                memcpy(bufRemainsPtr(output), this->current_header, SizeOfXLogLongPHD);
                bufUsedInc(output, SizeOfXLogLongPHD);
                lstClearFast(this->headers);

                // The header that needs to be modified is in another file or not exists. Just copy it as is.
                checkOutputSize(output, MAXALIGN(this->current_header->xlp_rem_len));
                memcpy(bufRemainsPtr(output), this->page + this->page_offset, MAXALIGN(this->current_header->xlp_rem_len));

                bufUsedInc(output, MAXALIGN(this->current_header->xlp_rem_len));
                this->page_offset += MAXALIGN(this->current_header->xlp_rem_len);
            }
        }
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

    if (!readRecord(this, input))
    {
        FUNCTION_LOG_RETURN_VOID();
        return;
    }

    filterRecord(this);

    writeRecord(this, output);

    this->i++;
    this->same_input = true;
    lstClearFast(this->headers);

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
walFilterNew(const unsigned int pgVersion, const StringId fork, const ArchiveGetFile *const archiveInfo)
{
    FUNCTION_LOG_BEGIN(logLevelTrace);
    FUNCTION_LOG_END();

    OBJ_NEW_BEGIN(WalFilterState, .childQty = MEM_CONTEXT_QTY_MAX, .allocQty = MEM_CONTEXT_QTY_MAX)
    {
        memset(this, 0, sizeof(WalFilterState));
        this->is_begin = true;
        this->record = memNew(BLCKSZ);
        this->rec_buf_size = BLCKSZ;
        this->headers = lstNewP(SizeOfXLogLongPHD);
        this->archiveInfo = archiveInfo;

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
