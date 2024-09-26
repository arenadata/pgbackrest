#ifndef COMMON_WALFILTER_POSTGRESCOMMON_H
#define COMMON_WALFILTER_POSTGRESCOMMON_H

#include <stdint.h>
#include "postgres/interface/static.vendor.h"

// Common macros and constants for different versions of Postgres

// types
typedef uint32 TimeLineID;
typedef uint64 XLogRecPtr;
typedef uint8 RmgrId;
typedef uint32 pg_crc32;
typedef uint32 BlockNumber;

// constants
/* This flag indicates a "long" page header */
#define XLP_LONG_HEADER             0x0002
/* Define as the maximum alignment requirement of any C data type. */
#define MAXIMUM_ALIGNOF 8
/* When record crosses page boundary, set this flag in new page's header */
#define XLP_FIRST_IS_CONTRECORD     0x0001
/* Replaces a missing contrecord; see CreateOverwriteContrecordRecord */
#define XLP_FIRST_IS_OVERWRITE_CONTRECORD 0x0008

#define RM_XLOG_ID 0
#define XLOG_SWITCH 0x40
#define XLOG_NOOP   0x20

// macros

/* ----------------
 * Alignment macros: align a length or address appropriately for a given type.
 * The fooALIGN() macros round up to a multiple of the required alignment,
 * while the fooALIGN_DOWN() macros round down.  The latter are more useful
 * for problems like "how many X-sized structures will fit in a page?".
 *
 * NOTE: TYPEALIGN[_DOWN] will not work if ALIGNVAL is not a power of 2.
 * That case seems extremely unlikely to be needed in practice, however.
 *
 * NOTE: MAXIMUM_ALIGNOF, and hence MAXALIGN(), intentionally exclude any
 * larger-than-8-byte types the compiler might have.
 * ----------------
 */
#define TYPEALIGN(ALIGNVAL,LEN)  \
    (((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define MAXALIGN(LEN)           TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))

#define SizeOfXLogShortPHD  MAXALIGN(sizeof(XLogPageHeaderData))
#define SizeOfXLogLongPHD   MAXALIGN(sizeof(XLogLongPageHeaderData))
#define XLogPageHeaderSize(hdr)     \
    (((hdr)->xlp_info & XLP_LONG_HEADER) ? SizeOfXLogLongPHD : SizeOfXLogShortPHD)
#define SizeOfXLogRecord    MAXALIGN(sizeof(XLogRecord))
#define XLogRecGetData(record)  ((char*) (record) + SizeOfXLogRecord)
#define Min(x, y)       ((x) < (y) ? (x) : (y))

/*
 * The XLOG is split into WAL segments (physical files) of the size indicated
 * by XLOG_SEG_SIZE.
 */
#define XLogSegmentsPerXLogId(segSize)   (0x100000000ULL / segSize)
#define XLogFromFileName(fname, tli, logSegNo, segSize)  \
    do {                                                \
        uint32 log;                                     \
        uint32 seg;                                     \
        sscanf(fname, "%08X%08X%08X", tli, &log, &seg); \
        *logSegNo = (uint64) log * XLogSegmentsPerXLogId(segSize) + seg; \
    } while (0)

// structs
typedef struct XLogPageHeaderData
{
    uint16 xlp_magic;           /* magic value for correctness checks */
    uint16 xlp_info;            /* flag bits, see below */
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
    uint32 xlp_rem_len;         /* total len of remaining data for record */
} XLogPageHeaderData;

typedef struct XLogLongPageHeaderData
{
    XLogPageHeaderData std;     /* standard header fields */
    uint64 xlp_sysid;           /* system identifier from pg_control */
    uint32 xlp_seg_size;        /* just as a cross-check */
    uint32 xlp_xlog_blcksz;         /* just as a cross-check */
} XLogLongPageHeaderData;

typedef struct XLogRecord
{
    uint32 xl_tot_len;          /* total len of entire record */
    TransactionId xl_xid;       /* xact id */
    uint32 xl_len;              /* total len of rmgr data */
    uint8 xl_info;            /* flag bits, see below */
    RmgrId xl_rmid;             /* resource manager for this record */
    /* 2 bytes of padding here, initialize to zero */
    XLogRecPtr xl_prev;         /* ptr to previous record in log */
    pg_crc32 xl_crc;            /* CRC for this record */

    /* If MAXALIGN==8, there are 4 wasted bytes here */

    /* ACTUAL LOG DATA FOLLOWS AT END OF STRUCT */
} XLogRecord;
_Static_assert(
    MAXALIGN(offsetof(XLogRecord, xl_info)) == MAXALIGN(offsetof(XLogRecord, xl_rmid)),
    "The xl_info and xl_rmid fields are in different 8 byte chunks.");

#endif // COMMON_WALFILTER_POSTGRESCOMMON_H
