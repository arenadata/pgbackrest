#ifndef PGBACKREST_GREENPLUMCOMMON_H
#define PGBACKREST_GREENPLUMCOMMON_H

#include <stdint.h>
#include "postgres/interface/static.vendor.h"

// Common macros and constants for different versions of Greenplum

// types
typedef uint32_t TimeLineID;
typedef uint64_t XLogRecPtr;
typedef uint8_t RmgrId;
typedef uint32_t pg_crc32;
typedef uint32_t TransactionId;
typedef uint32_t BlockNumber;

// constants
#define XLOG_BLCKSZ 32768
/* This flag indicates a "long" page header */
#define XLP_LONG_HEADER             0x0002
#define MAXIMUM_ALIGNOF 8
#define BLCKSZ 32768
#define XLR_MAX_BKP_BLOCKS      4
#define XLP_FIRST_IS_CONTRECORD     0x0001
#define XLP_FIRST_IS_OVERWRITE_CONTRECORD 0x0008

#define RM_XLOG_ID                      0
#define XLOG_SWITCH                     0x40
#define XLOG_NOOP                       0x20
#define MAXPGPATH       1024
// macros
#define MAXALIGN(LEN)           TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
#define TYPEALIGN(ALIGNVAL,LEN)  \
    (((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))

#define SizeOfXLogShortPHD  MAXALIGN(sizeof(XLogPageHeaderData))
#define SizeOfXLogLongPHD   MAXALIGN(sizeof(XLogLongPageHeaderData))
#define XLogPageHeaderSize(hdr)     \
    (((hdr)->xlp_info & XLP_LONG_HEADER) ? SizeOfXLogLongPHD : SizeOfXLogShortPHD)
#define SizeOfXLogRecord    MAXALIGN(sizeof(XLogRecord))
#define XLogRecGetData(record)  ((char*) (record) + SizeOfXLogRecord)
#define XLR_BKP_BLOCK(iblk)     (0x08 >> (iblk))        /* iblk in 0..3 */
#define Min(x, y)       ((x) < (y) ? (x) : (y))

#define XLOG_SEG_SIZE (64 * 1024 * 1024)
#define UINT64CONST(x) (x##UL)
#define XLogSegmentsPerXLogId   (UINT64CONST(0x100000000) / XLOG_SEG_SIZE)
#define XLogFromFileName(fname, tli, logSegNo)  \
    do {                                                \
        uint32 log;                                     \
        uint32 seg;                                     \
        sscanf(fname, "%08X%08X%08X", tli, &log, &seg); \
        *logSegNo = (uint64) log * XLogSegmentsPerXLogId + seg; \
    } while (0)

#define XLogFilePath(path, tli, logSegNo)   \
    snprintf(path, MAXPGPATH, "%08X%08X%08X", tli,              \
             (uint32) ((logSegNo) / XLogSegmentsPerXLogId),             \
             (uint32) ((logSegNo) % XLogSegmentsPerXLogId))

// structs
typedef struct XLogPageHeaderData
{
    uint16_t xlp_magic;           /* magic value for correctness checks */
    uint16_t xlp_info;            /* flag bits, see below */
    TimeLineID xlp_tli;         /* TimeLineID of first record on page */
    XLogRecPtr xlp_pageaddr;    /* XLOG address of this page */

    /*
     * When there is not enough space on current page for whole record, we
     * continue on the next page.  xlp_rem_len is the number of bytes
     * remaining from a previous page.
     *
     * Note that xl_rem_len includes backup-block data; that is, it tracks
     * xl_tot_len not xl_len in the initial header.  Also note that the
     * continuation data isn't necessarily aligned.
     */
    uint32_t xlp_rem_len;         /* total len of remaining data for record */
} XLogPageHeaderData;

typedef struct XLogLongPageHeaderData
{
    XLogPageHeaderData std;     /* standard header fields */
    uint64_t xlp_sysid;           /* system identifier from pg_control */
    uint32_t xlp_seg_size;        /* just as a cross-check */
    uint32_t xlp_xlog_blcksz;         /* just as a cross-check */
} __attribute__ ((aligned (8))) XLogLongPageHeaderData;

typedef struct XLogRecord
{
    uint32_t xl_tot_len;          /* total len of entire record */
    TransactionId xl_xid;       /* xact id */
    uint32_t xl_len;              /* total len of rmgr data */
    uint8_t xl_info;            /* flag bits, see below */
    RmgrId xl_rmid;             /* resource manager for this record */
    /* 2 bytes of padding here, initialize to zero */
    XLogRecPtr xl_prev;         /* ptr to previous record in log */
    pg_crc32 xl_crc;            /* CRC for this record */

    /* If MAXALIGN==8, there are 4 wasted bytes here */

    /* ACTUAL LOG DATA FOLLOWS AT END OF STRUCT */
} XLogRecord;

#endif // PGBACKREST_GREENPLUMCOMMON_H
