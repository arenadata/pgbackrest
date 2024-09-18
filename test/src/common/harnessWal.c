#include "build.auto.h"

#include "common/type/object.h"
#include "harnessWal.h"
#include "postgres/interface/crc32.h"
#include "string.h"

XLogRecord *
hrnGpdbCreateXRecord(uint8_t rmid, uint8_t info, uint32_t body_size, void *body)
{
    XLogRecord *record = memNew(SizeOfXLogRecord + body_size);
    *record = (XLogRecord){
        .xl_tot_len = (uint32_t) (SizeOfXLogRecord + body_size),
        .xl_xid = 0xADDE,
        .xl_len = body_size,
        .xl_info = info,
        .xl_rmid = (uint8_t) rmid,
        .xl_prev = 0xAABB
    };

    size_t align_size = MAXALIGN(sizeof(XLogRecord)) - sizeof(XLogRecord);
    memset((char *) record + sizeof(XLogRecord), 0, align_size);

    if (body == NULL)
    {
        memset(XLogRecGetData(record), 0xAB, body_size);
    }
    else
    {
        memcpy(XLogRecGetData(record), body, body_size);
    }

    uint32_t crc = crc32cInit();
    crc = crc32cComp(crc, (unsigned char *) XLogRecGetData(record), body_size);
    crc = crc32cComp(crc, (unsigned char *) record, offsetof(XLogRecord, xl_crc));
    crc = crc32cFinish(crc);
    record->xl_crc = crc;

    return record;
}

void
hrnGpdbWalInsertXRecord(
    Buffer *const walBuffer,
    XLogRecord *record,
    InsertXRecordParam param,
    InsertRecordFlags flags)
{
    if (param.magic == 0)
    {
        param.magic = 0xD07E;
    }

    if (param.walPageSize == 0)
    {
        param.walPageSize = 32768;
    }

    if (bufUsed(walBuffer) == 0)
    {
        // This is first record
        XLogLongPageHeaderData longHeader = {0};
        longHeader.std.xlp_magic = param.magic;
        longHeader.std.xlp_info = XLP_LONG_HEADER;
        longHeader.std.xlp_tli = 1;
        longHeader.std.xlp_pageaddr = param.segno * (64 * 1024 * 1024);
        longHeader.std.xlp_rem_len = param.begin_offset;
        longHeader.xlp_sysid = 10000000000000090400ULL;
        longHeader.xlp_seg_size = 64 * 1024 * 1024;
        longHeader.xlp_xlog_blcksz = param.walPageSize;

        if (flags & OVERWRITE)
        {
            longHeader.std.xlp_info |= XLP_FIRST_IS_OVERWRITE_CONTRECORD;
        }
        if (param.begin_offset)
        {
            longHeader.std.xlp_info |= XLP_FIRST_IS_CONTRECORD;
        }

        memcpy(bufRemainsPtr(walBuffer), &longHeader, sizeof(longHeader));
        bufUsedInc(walBuffer, sizeof(longHeader));
        size_t align_size = MAXALIGN(sizeof(longHeader)) - sizeof(longHeader);
        memset(bufRemainsPtr(walBuffer), 0, align_size);
        bufUsedInc(walBuffer, align_size);
    }

    if (bufUsed(walBuffer) % param.walPageSize == 0)
    {
        XLogPageHeaderData header = {0};
        header.xlp_magic = param.magic;
        header.xlp_tli = 1;
        header.xlp_pageaddr = bufUsed(walBuffer);
        header.xlp_rem_len = 0;

        if (flags & COND_FLAG)
        {
            header.xlp_info |= XLP_FIRST_IS_CONTRECORD;
        }
        else if (flags & OVERWRITE)
        {
            header.xlp_info = XLP_FIRST_IS_OVERWRITE_CONTRECORD;
        }
        else
        {
            header.xlp_info = 0;
        }

        memcpy(bufRemainsPtr(walBuffer), &header, sizeof(header));
        bufUsedInc(walBuffer, sizeof(header));
        memset(bufRemainsPtr(walBuffer), 0, XLogPageHeaderSize(&header) - sizeof(header));
        bufUsedInc(walBuffer, XLogPageHeaderSize(&header) - sizeof(header));
    }

    size_t space_on_page = param.walPageSize - bufUsed(walBuffer) % param.walPageSize;

    size_t tot_len;
    unsigned char *record_ptr;

    if (param.begin_offset == 0 || flags & OVERWRITE)
    {
        tot_len = record->xl_tot_len;
        record_ptr = (unsigned char *) record;
    }
    else
    {
        tot_len = param.begin_offset;
        record_ptr = ((unsigned char *) record) + (record->xl_tot_len - param.begin_offset);
    }

    if (space_on_page < tot_len)
    {
        // We need to split record into two or more pages
        size_t wrote = 0;
        while (wrote != tot_len)
        {
            space_on_page = param.walPageSize - bufUsed(walBuffer) % param.walPageSize;
            size_t to_write = Min(space_on_page, tot_len - wrote);
            memcpy(bufRemainsPtr(walBuffer), record_ptr + wrote, to_write);
            wrote += to_write;

            bufUsedInc(walBuffer, to_write);
            if (flags & INCOMPLETE_RECORD){
                return;
            }
            if (wrote == tot_len)
            {
                break;
            }

            ASSERT(bufUsed(walBuffer) % param.walPageSize == 0);
            // We should be on the beginning of the page. so write header
            XLogPageHeaderData header = {0};
            header.xlp_magic = param.magic;
            header.xlp_info = !(flags & NO_COND_FLAG) ? XLP_FIRST_IS_CONTRECORD : 0;
            header.xlp_tli = 1;
            header.xlp_pageaddr = param.segno * (64 * 1024 * 1024) + bufUsed(walBuffer);

            if (flags & ZERO_REM_LEN)
            {
                header.xlp_rem_len = 0;
            }
            else if (flags & WRONG_REM_LEN)
            {
                header.xlp_rem_len = 1;
            }
            else
            {
                header.xlp_rem_len = (uint32_t) (tot_len - wrote);
            }

            *((XLogPageHeaderData *) bufRemainsPtr(walBuffer)) = header;

            bufUsedInc(walBuffer, sizeof(header));

            size_t align_size = MAXALIGN(sizeof(header)) - sizeof(header);
            memset(bufRemainsPtr(walBuffer), 0, align_size);
            bufUsedInc(walBuffer, align_size);
        }
    }
    else
    {
        // Record should fit into current page
        memcpy(bufRemainsPtr(walBuffer), record_ptr, tot_len);
        bufUsedInc(walBuffer, tot_len);
    }
    size_t align_size = MAXALIGN(tot_len) - (tot_len);
    memset(bufRemainsPtr(walBuffer), 0, align_size);
    bufUsedInc(walBuffer, align_size);
}

void
hrnGpdbWalInsertXRecordSimple(Buffer *const walBuffer, XLogRecord *record)
{
    hrnGpdbWalInsertXRecordP(walBuffer, record, NO_FLAGS);
}
