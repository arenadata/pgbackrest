#include "build.auto.h"

#include <stddef.h>
#include <string.h>
#include "common/error/error.h"
#include "postgres/interface/crc32.h"
#include "postgres_common.h"

FN_EXTERN void
ValidXLogRecordHeader(const XLogRecord *const record)
{
    /*
     * xl_len == 0 is bad data for everything except XLOG SWITCH, where it is
     * required.
     */
    if (record->xl_rmid == ResourceManager_XLOG && record->xl_info == XLOG_SWITCH)
    {
        if (record->xl_len != 0)
        {
            THROW_FMT(FormatError, "invalid xlog switch record");
        }
    }
    else if (record->xl_len == 0)
    {
        THROW_FMT(FormatError, "record with zero length");
    }
    if (record->xl_tot_len < SizeOfXLogRecord + record->xl_len ||
        record->xl_tot_len > SizeOfXLogRecord + record->xl_len +
        XLR_MAX_BKP_BLOCKS * (sizeof(BkpBlock) + BLCKSZ))
    {
        THROW_FMT(FormatError, "invalid record length");
    }
    if (record->xl_rmid > ResourceManager_Appendonly)
    {
        THROW_FMT(FormatError, "invalid resource manager ID %u", record->xl_rmid);
    }
}

FN_EXTERN void
ValidXLogRecord(const XLogRecord *const record)
{
    pg_crc32 crc;
    int i;
    uint32_t len = record->xl_len;
    BkpBlock bkpb;
    char *blk;
    size_t remaining = record->xl_tot_len;

    remaining -= SizeOfXLogRecord + len;
    crc = crc32cInit();
    crc = crc32cComp(crc, (unsigned char *) XLogRecGetData(record), len);

    /* Add in the backup blocks, if any */
    blk = (char *) XLogRecGetData(record) + len;
    for (i = 0; i < XLR_MAX_BKP_BLOCKS; i++)
    {
        size_t blen;

        if (!(record->xl_info & XLR_BKP_BLOCK(i)))
            continue;

        if (remaining < sizeof(BkpBlock))
        {
            THROW_FMT(FormatError, "invalid backup block size in record");
        }
        memcpy(&bkpb, blk, sizeof(BkpBlock));

        if (bkpb.hole_offset + bkpb.hole_length > BLCKSZ)
        {
            THROW_FMT(FormatError, "incorrect hole size in record");
        }
        blen = sizeof(BkpBlock) + BLCKSZ - bkpb.hole_length;

        if (remaining < blen)
        {
            THROW_FMT(FormatError, "invalid backup block size in record");
        }
        remaining -= blen;
        crc = crc32cComp(crc, (unsigned char *) blk, blen);
        blk += blen;
    }

    /* Check that xl_tot_len agrees with our calculation */
    if (remaining != 0)
    {
        THROW_FMT(FormatError, "incorrect total length in record");
    }

    /* Finally include the record header */
    crc = crc32cComp(crc, (unsigned char *) record, offsetof(XLogRecord, xl_crc));
    crc = crc32cFinish(crc);

    if (crc != record->xl_crc)
    {
        THROW_FMT(FormatError, "incorrect resource manager data checksum in record. expect: %u, but got: %u", record->xl_crc, crc);
    }
}
