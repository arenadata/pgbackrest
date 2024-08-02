#ifndef PGBACKREST_POSTGRES_COMMON_H
#define PGBACKREST_POSTGRES_COMMON_H

#include <stdint.h>

// types
typedef uint32_t TimeLineID;
typedef uint64_t XLogRecPtr;
typedef uint8_t RmgrId;
typedef uint32_t pg_crc32;
typedef unsigned int Oid;
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
#define XLR_INFO_MASK           0x0F
#define XLOG_HEAP_OPMASK        0x70
#define GPDB6_XLOG_PAGE_MAGIC 0xD07E

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

// enums
// need to know size
typedef enum ForkNumber
{
    InvalidForkNumber = -1,
    MAIN_FORKNUM = 0,
    FSM_FORKNUM,
    VISIBILITYMAP_FORKNUM,

    /*
     * Init forks are used to create an initial state that can be used to
     * quickly revert an object back to its empty state. This is useful for
     * reverting unlogged tables and indexes back to their initial state during
     * recovery.
     */
    INIT_FORKNUM

    /*
     * NOTE: if you add a new fork, change MAX_FORKNUM and possibly
     * FORKNAMECHARS below, and update the forkNames array in
     * src/common/relpath.c
     */
} ForkNumber;

enum ResourceManager
{
    ResourceManager_XLOG,
    ResourceManager_Transaction,
    ResourceManager_Storage,
    ResourceManager_CLOG,
    ResourceManager_Database,
    ResourceManager_Tablespace,
    ResourceManager_MultiXact,
    ResourceManager_RelMap,
    ResourceManager_Standby,
    ResourceManager_Heap2,
    ResourceManager_Heap,
    ResourceManager_Btree,
    ResourceManager_Hash,
    ResourceManager_Gin,
    ResourceManager_Gist,
    ResourceManager_Sequence,
    ResourceManager_SPGist,
    ResourceManager_Bitmap,
    ResourceManager_DistributedLog,
    ResourceManager_Appendonly
};

// structs
typedef struct RelFileNode
{
    Oid spcNode;                /* tablespace */
    Oid dbNode;                 /* database */
    Oid relNode;                /* relation */
} RelFileNode;

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

typedef XLogPageHeaderData *XLogPageHeader;

typedef struct XLogLongPageHeaderData
{
    XLogPageHeaderData std;     /* standard header fields */
    uint64_t xlp_sysid;           /* system identifier from pg_control */
    uint32_t xlp_seg_size;        /* just as a cross-check */
    uint32_t xlp_xlog_blcksz;         /* just as a cross-check */
} XLogLongPageHeaderData;

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

typedef struct BkpBlock
{
    RelFileNode node;           /* relation containing block */
    ForkNumber fork;            /* fork within the relation */
    BlockNumber block;          /* block number */
    uint16_t hole_offset;         /* number of bytes before "hole" */
    uint16_t hole_length;         /* number of bytes in "hole" */

    /* ACTUAL BLOCK DATA FOLLOWS AT END OF STRUCT */
} BkpBlock;

// record types
// xlog
#define XLOG_CHECKPOINT_SHUTDOWN        0x00
#define XLOG_CHECKPOINT_ONLINE          0x10
#define XLOG_NOOP                       0x20
#define XLOG_NEXTOID                    0x30
#define XLOG_SWITCH                     0x40
#define XLOG_BACKUP_END                 0x50
#define XLOG_PARAMETER_CHANGE           0x60
#define XLOG_RESTORE_POINT              0x70
#define XLOG_FPW_CHANGE                 0x80
#define XLOG_END_OF_RECOVERY            0x90
#define XLOG_FPI                        0xA0
#define XLOG_NEXTRELFILENODE            0xB0
#define XLOG_OVERWRITE_CONTRECORD       0xC0

// storage
#define XLOG_SMGR_CREATE    0x10
#define XLOG_SMGR_TRUNCATE  0x20

// heap
#define XLOG_HEAP2_REWRITE      0x00
#define XLOG_HEAP2_CLEAN        0x10
#define XLOG_HEAP2_FREEZE_PAGE  0x20
#define XLOG_HEAP2_CLEANUP_INFO 0x30
#define XLOG_HEAP2_VISIBLE      0x40
#define XLOG_HEAP2_MULTI_INSERT 0x50
#define XLOG_HEAP2_LOCK_UPDATED 0x60
#define XLOG_HEAP2_NEW_CID      0x70
#define XLOG_HEAP_INSERT        0x00
#define XLOG_HEAP_DELETE        0x10
#define XLOG_HEAP_UPDATE        0x20
/* 0x030 is free, was XLOG_HEAP_MOVE */
#define XLOG_HEAP_HOT_UPDATE    0x40
#define XLOG_HEAP_NEWPAGE       0x50
#define XLOG_HEAP_LOCK          0x60
#define XLOG_HEAP_INPLACE       0x70
#define XLOG_HEAP_INIT_PAGE     0x80

// btree
#define XLOG_BTREE_INSERT_LEAF  0x00    /* add index tuple without split */
#define XLOG_BTREE_INSERT_UPPER 0x10    /* same, on a non-leaf page */
#define XLOG_BTREE_INSERT_META  0x20    /* same, plus update metapage */
#define XLOG_BTREE_SPLIT_L      0x30    /* add index tuple with split */
#define XLOG_BTREE_SPLIT_R      0x40    /* as above, new item on right */
#define XLOG_BTREE_SPLIT_L_ROOT 0x50    /* add tuple with split of root */
#define XLOG_BTREE_SPLIT_R_ROOT 0x60    /* as above, new item on right */
#define XLOG_BTREE_DELETE       0x70    /* delete leaf index tuples for a page */
#define XLOG_BTREE_UNLINK_PAGE  0x80    /* delete a half-dead page */
#define XLOG_BTREE_UNLINK_PAGE_META 0x90        /* same, and update metapage */
#define XLOG_BTREE_NEWROOT      0xA0    /* new root page */
#define XLOG_BTREE_MARK_PAGE_HALFDEAD 0xB0      /* mark a leaf as half-dead */
#define XLOG_BTREE_VACUUM       0xC0    /* delete entries on a page during
                                         * vacuum */
#define XLOG_BTREE_REUSE_PAGE   0xD0    /* old page is about to be reused from
                                         * FSM */

// gin
#define XLOG_GIN_CREATE_INDEX  0x00
#define XLOG_GIN_CREATE_PTREE  0x10
#define XLOG_GIN_INSERT  0x20
#define XLOG_GIN_SPLIT  0x30
#define XLOG_GIN_VACUUM_PAGE    0x40
#define XLOG_GIN_VACUUM_DATA_LEAF_PAGE  0x90
#define XLOG_GIN_DELETE_PAGE    0x50
#define XLOG_GIN_UPDATE_META_PAGE 0x60
#define XLOG_GIN_INSERT_LISTPAGE  0x70
#define XLOG_GIN_DELETE_LISTPAGE  0x80

// gist
#define XLOG_GIST_PAGE_UPDATE       0x00
#define XLOG_GIST_PAGE_SPLIT        0x30
#define XLOG_GIST_CREATE_INDEX      0x50

// sequence
#define XLOG_SEQ_LOG            0x00

// spgist
#define XLOG_SPGIST_CREATE_INDEX    0x00
#define XLOG_SPGIST_ADD_LEAF        0x10
#define XLOG_SPGIST_MOVE_LEAFS      0x20
#define XLOG_SPGIST_ADD_NODE        0x30
#define XLOG_SPGIST_SPLIT_TUPLE     0x40
#define XLOG_SPGIST_PICKSPLIT       0x50
#define XLOG_SPGIST_VACUUM_LEAF     0x60
#define XLOG_SPGIST_VACUUM_ROOT     0x70
#define XLOG_SPGIST_VACUUM_REDIRECT 0x80

// bitmap
#define XLOG_BITMAP_INSERT_LOVITEM  0x20 /* add a new entry into a LOV page */
#define XLOG_BITMAP_INSERT_META     0x30 /* update the metapage */
#define XLOG_BITMAP_INSERT_BITMAP_LASTWORDS 0x40 /* update the last 2 words
                                                    in a bitmap */
/* insert bitmap words into a bitmap page which is not the last one. */
#define XLOG_BITMAP_INSERT_WORDS        0x50
/* 0x60 is unused */
#define XLOG_BITMAP_UPDATEWORD          0x70
#define XLOG_BITMAP_UPDATEWORDS         0x80

// appendonly
#define XLOG_APPENDONLY_INSERT          0x00
#define XLOG_APPENDONLY_TRUNCATE        0x10

// prototypes
FN_EXTERN void ValidXLogRecordHeader(const XLogRecord *record);
FN_EXTERN void ValidXLogRecord(const XLogRecord *record);
#endif // PGBACKREST_POSTGRES_COMMON_H
