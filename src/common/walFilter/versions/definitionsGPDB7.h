#ifndef PGBACKREST_DEFINITIONSGPDB7_H
#define PGBACKREST_DEFINITIONSGPDB7_H

#include "common/walFilter/postgresCommon.h"
#include "postgres/interface/crc32.h"

/*
 * Block IDs used to distinguish different kinds of record fragments. Block
 * references are numbered from 0 to XLR_MAX_BLOCK_ID. A rmgr is free to use
 * any ID number in that range (although you should stick to small numbers,
 * because the WAL machinery is optimized for that case). A couple of ID
 * numbers are reserved to denote the "main" data portion of the record.
 *
 * The maximum is currently set at 32, quite arbitrarily. Most records only
 * need a handful of block references, but there are a few exceptions that
 * need more.
 */
#define XLR_MAX_BLOCK_ID            32

#define XLR_BLOCK_ID_DATA_SHORT     255
#define XLR_BLOCK_ID_DATA_LONG      254
#define XLR_BLOCK_ID_ORIGIN         253

/*
 * The fork number fits in the lower 4 bits in the fork_flags field. The upper
 * bits are used for flags.
 */
#define BKPBLOCK_FORK_MASK  0x0F
#define BKPBLOCK_FLAG_MASK  0xF0
#define BKPBLOCK_HAS_IMAGE  0x10    /* block data is an XLogRecordBlockImage */
#define BKPBLOCK_HAS_DATA   0x20
#define BKPBLOCK_WILL_INIT  0x40    /* redo will re-init the page */
#define BKPBLOCK_SAME_REL   0x80    /* RelFileNode omitted, same as previous */

/* Information stored in bimg_info */
#define BKPIMAGE_HAS_HOLE       0x01    /* page image has "hole" */
#define BKPIMAGE_IS_COMPRESSED      0x02    /* page image is compressed */
#define BKPIMAGE_APPLY      0x04    /* page image should be restored during
                                     * replay */

/*
 * The overall layout of an XLOG record is:
 *      Fixed-size header (XLogRecord struct)
 *      XLogRecordBlockHeader struct
 *      XLogRecordBlockHeader struct
 *      ...
 *      XLogRecordDataHeader[Short|Long] struct
 *      block data
 *      block data
 *      ...
 *      main data
 *
 * There can be zero or more XLogRecordBlockHeaders, and 0 or more bytes of
 * rmgr-specific data not associated with a block.  XLogRecord structs
 * always start on MAXALIGN boundaries in the WAL files, but the rest of
 * the fields are not aligned.
 *
 * The XLogRecordBlockHeader, XLogRecordDataHeaderShort and
 * XLogRecordDataHeaderLong structs all begin with a single 'id' byte. It's
 * used to distinguish between block references, and the main data structs.
 */
typedef struct XLogRecordGPDB7
{
    uint32 xl_tot_len;          /* total len of entire record */
    TransactionId xl_xid;       /* xact id */
    XLogRecPtr xl_prev;         /* ptr to previous record in log */
    uint8 xl_info;              /* flag bits, see below */
    RmgrId xl_rmid;             /* resource manager for this record */
    /* 2 bytes of padding here, initialize to zero */
    uint32_t xl_crc;            /* CRC for this record */

    /* XLogRecordBlockHeaders and XLogRecordDataHeader follow, no padding */
} XLogRecordGPDB7;
_Static_assert(
    offsetof(XLogRecordGPDB7, xl_info) / 8 == offsetof(XLogRecordGPDB7, xl_rmid) / 8,
    "The xl_info and xl_rmid fields are in different 8 byte chunks.");

typedef struct XLogRecordDataHeaderShort
{
    uint8 id;                   /* XLR_BLOCK_ID_DATA_SHORT */
    uint8 data_length;          /* number of payload bytes */
} __attribute__((packed)) XLogRecordDataHeaderShort;

typedef struct XLogRecordDataHeaderLong
{
    uint8 id;                   /* XLR_BLOCK_ID_DATA_LONG */
    uint32 data_length;         /* number of payload bytes */
} __attribute__((packed)) XLogRecordDataHeaderLong;

FN_INLINE_ALWAYS pg_crc32
xLogRecordChecksumGPDB7(const XLogRecordGPDB7 *const record)
{
    uint32_t crc = crc32cInit();

    /* Calculate the CRC */
    crc = crc32cComp(crc, ((unsigned char *) record) + sizeof(XLogRecordGPDB7), record->xl_tot_len - sizeof(XLogRecordGPDB7));
    /* include the record header last */
    crc = crc32cComp(crc, (unsigned char *) record, offsetof(XLogRecordGPDB7, xl_crc));
    return crc32cFinish(crc);
}

#endif // PGBACKREST_DEFINITIONSGPDB7_H
