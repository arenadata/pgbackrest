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
    InsertRecordFlags flags,
    uint16_t magic,
    uint32_t begin_offset)
{
    if (magic == 0)
    {
        magic = 0xD07E;
    }

    if (bufUsed(walBuffer) == 0)
    {
        // This is first record
        XLogLongPageHeaderData longHeader = {0};
        longHeader.std.xlp_magic = magic;
        longHeader.std.xlp_info = XLP_LONG_HEADER;
        longHeader.std.xlp_tli = 1;
        longHeader.std.xlp_pageaddr = 0;
        longHeader.std.xlp_rem_len = begin_offset;
        longHeader.xlp_sysid = 10000000000000090400ULL;
        longHeader.xlp_seg_size = 64 * 1024 * 1024;
        longHeader.xlp_xlog_blcksz = XLOG_BLCKSZ;

        if (flags & OVERWRITE)
        {
            longHeader.std.xlp_info |= XLP_FIRST_IS_OVERWRITE_CONTRECORD;
        }
        if (begin_offset)
        {
            longHeader.std.xlp_info |= XLP_FIRST_IS_CONTRECORD;
        }

        memcpy(bufRemainsPtr(walBuffer), &longHeader, sizeof(longHeader));
        bufUsedInc(walBuffer, sizeof(longHeader));

        if (begin_offset && !(flags & OVERWRITE))
        {
            begin_offset = (uint32_t) MAXALIGN(begin_offset);
            memset(bufRemainsPtr(walBuffer), 0, begin_offset);
            bufUsedInc(walBuffer, begin_offset);
        }

        size_t align_size = MAXALIGN(sizeof(longHeader)) - sizeof(longHeader);
        memset(bufRemainsPtr(walBuffer), 0, align_size);
        bufUsedInc(walBuffer, align_size);
    }

    if (bufUsed(walBuffer) % XLOG_BLCKSZ == 0)
    {
        XLogPageHeaderData header = {0};
        header.xlp_magic = magic;
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

    size_t space_on_page = XLOG_BLCKSZ - bufUsed(walBuffer) % XLOG_BLCKSZ;
    if (space_on_page < record->xl_tot_len)
    {
        // We need to split record into two or more pages
        size_t wrote = 0;
        while (wrote != record->xl_tot_len)
        {
            space_on_page = XLOG_BLCKSZ - bufUsed(walBuffer) % XLOG_BLCKSZ;
            size_t to_write = Min(space_on_page, record->xl_tot_len - wrote);
            memcpy(bufRemainsPtr(walBuffer), ((char *) record) + wrote, to_write);
            wrote += to_write;

            bufUsedInc(walBuffer, to_write);
            if (wrote == record->xl_tot_len || (flags & INCOMPLETE_RECORD))
            {
                break;
            }

            ASSERT(bufUsed(walBuffer) % XLOG_BLCKSZ == 0);
            // We should be on the beginning of the page. so write header
            XLogPageHeaderData header = {0};
            header.xlp_magic = magic;
            header.xlp_info = !(flags & NO_COND_FLAG) ? XLP_FIRST_IS_CONTRECORD : 0;
            header.xlp_tli = 1;
            header.xlp_pageaddr = bufUsed(walBuffer);

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
                header.xlp_rem_len = (uint32_t) (record->xl_tot_len - wrote);
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
        memcpy(bufRemainsPtr(walBuffer), record, record->xl_tot_len);
        bufUsedInc(walBuffer, record->xl_tot_len);
    }
    size_t align_size = MAXALIGN(record->xl_tot_len) - record->xl_tot_len;
    memset(bufRemainsPtr(walBuffer), 0, align_size);
    bufUsedInc(walBuffer, align_size);
}

void
hrnGpdbWalInsertXRecordSimple(Buffer *const walBuffer, XLogRecord *record)
{
    hrnGpdbWalInsertXRecord(walBuffer, record, NO_FLAGS, 0, 0);
}
