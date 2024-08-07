#include "build.auto.h"

#include <malloc.h>
#include <stdbool.h>
#include <stdio.h>
#include "common/log.h"
#include "recordProcessGPDB6.h"
#include "postgres/interface/crc32.h"

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

#define XLR_INFO_MASK           0x0F
#define XLOG_HEAP_OPMASK        0x70

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

typedef uint32_t CommandId;
typedef uint16_t OffsetNumber;

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

typedef struct BkpBlock
{
    RelFileNode node;           /* relation containing block */
    ForkNumber fork;            /* fork within the relation */
    BlockNumber block;          /* block number */
    uint16_t hole_offset;         /* number of bytes before "hole" */
    uint16_t hole_length;         /* number of bytes in "hole" */

    /* ACTUAL BLOCK DATA FOLLOWS AT END OF STRUCT */
} BkpBlock;

typedef struct xl_smgr_truncate
{
    BlockNumber blkno;
    RelFileNode rnode;
} xl_smgr_truncate;

typedef struct BlockIdData
{
    uint16_t bi_hi;
    uint16_t bi_lo;
} BlockIdData;

typedef struct ItemPointerData
{
    BlockIdData ip_blkid;
    OffsetNumber ip_posid;
}

#ifdef __arm__
__attribute__((packed))         /* Appropriate whack upside the head for ARM */
#endif
ItemPointerData;

typedef struct xl_heaptid
{
    RelFileNode node;
    ItemPointerData tid;        /* changed tuple id */
} xl_heaptid;

typedef struct xl_heap_new_cid
{
    /*
     * store toplevel xid so we don't have to merge cids from different
     * transactions
     */
    TransactionId top_xid;
    CommandId cmin;
    CommandId cmax;

    /*
     * don't really need the combocid since we have the actual values right in
     * this struct, but the padding makes it free and its useful for
     * debugging.
     */
    CommandId combocid;

    /*
     * Store the relfilenode/ctid pair to facilitate lookups.
     */
    xl_heaptid target;
} xl_heap_new_cid;

static
bool
getXlog(XLogRecord *record, RelFileNode *node)
{
    uint8_t info = (uint8_t) (record->xl_info & ~XLR_INFO_MASK);
    char *rec = XLogRecGetData(record);
    switch (info)
    {
        case XLOG_CHECKPOINT_SHUTDOWN:
        case XLOG_CHECKPOINT_ONLINE:
        case XLOG_NOOP:
        case XLOG_NEXTOID:
        case XLOG_NEXTRELFILENODE:
        case XLOG_RESTORE_POINT:
        case XLOG_BACKUP_END:
        case XLOG_PARAMETER_CHANGE:
        case XLOG_FPW_CHANGE:
        case XLOG_END_OF_RECOVERY:
        case XLOG_OVERWRITE_CONTRECORD:
        case XLOG_SWITCH:
//          ignore
            break;

        case XLOG_FPI:
        {
            RelFileNode *rec_node = (RelFileNode *) rec;
            *node = *rec_node;
            return 1;
        }

        default:
            THROW_FMT(FormatError, "XLOG UNKNOWN: %d", info);
    }
    return 0;
}

static
bool
getStorage(XLogRecord *record, RelFileNode *node)
{
    uint8_t info = (uint8_t) (record->xl_info & ~XLR_INFO_MASK);
    char *rec = XLogRecGetData(record);
    switch (info)
    {
        case XLOG_SMGR_CREATE:
        {
            RelFileNode *rec_node = (RelFileNode *) rec;
            *node = *rec_node;
            return 1;
        }

        case XLOG_SMGR_TRUNCATE:
        {
            xl_smgr_truncate *xlrec = (xl_smgr_truncate *) rec;

            *node = xlrec->rnode;
            return 1;
        }

        default:
            THROW_FMT(FormatError, "Storage UNKNOWN: %d", info);
    }
}

static
bool
getHeap2(XLogRecord *record, RelFileNode *node)
{
    uint8_t info = (uint8_t) (record->xl_info & ~XLR_INFO_MASK);
    char *rec = XLogRecGetData(record);
    info &= XLOG_HEAP_OPMASK;

    if (info == XLOG_HEAP2_NEW_CID){
        xl_heap_new_cid *xlrec = (xl_heap_new_cid *) rec;
        *node = xlrec->target.node;
        return 1;
    }
    else if (info == XLOG_HEAP2_REWRITE)
    {
        return 0;
    }

    // XLOG_HEAP2_CLEAN
    // XLOG_HEAP2_FREEZE_PAGE
    // XLOG_HEAP2_CLEANUP_INFO
    // XLOG_HEAP2_VISIBLE
    // XLOG_HEAP2_MULTI_INSERT
    // XLOG_HEAP2_LOCK_UPDATED
    RelFileNode *rec_node = (RelFileNode *) rec;
    *node = *rec_node;
    return 1;
}

static
bool
getHeap(XLogRecord *record, RelFileNode *node)
{
    char *rec = XLogRecGetData(record);

    // XLOG_HEAP_INSERT
    // XLOG_HEAP_DELETE
    // XLOG_HEAP_UPDATE
    // XLOG_HEAP_HOT_UPDATE
    // XLOG_HEAP_NEWPAGE
    // XLOG_HEAP_LOCK
    // XLOG_HEAP_INPLACE
    RelFileNode *rec_node = (RelFileNode *) rec;
    *node = *rec_node;
    return 1;
}

static
bool
getBtree(XLogRecord *record, RelFileNode *node)
{
    uint8_t info = (uint8_t) (record->xl_info & ~XLR_INFO_MASK);
    char *rec = XLogRecGetData(record);
    switch (info)
    {
        case XLOG_BTREE_INSERT_LEAF:
        case XLOG_BTREE_INSERT_UPPER:
        case XLOG_BTREE_INSERT_META:
        case XLOG_BTREE_SPLIT_L:
        case XLOG_BTREE_SPLIT_R:
        case XLOG_BTREE_SPLIT_L_ROOT:
        case XLOG_BTREE_SPLIT_R_ROOT:
        case XLOG_BTREE_VACUUM:
        case XLOG_BTREE_DELETE:
        case XLOG_BTREE_MARK_PAGE_HALFDEAD:
        case XLOG_BTREE_UNLINK_PAGE_META:
        case XLOG_BTREE_UNLINK_PAGE:
        case XLOG_BTREE_NEWROOT:
        case XLOG_BTREE_REUSE_PAGE:
        {
            RelFileNode *rec_node = (RelFileNode *) rec;
            *node = *rec_node;
            return 1;
        }

        default:
            THROW_FMT(FormatError, "Btree UNKNOWN: %d", info);
    }
}

static
bool
getGin(XLogRecord *record, RelFileNode *node)
{
    uint8_t info = (uint8_t) (record->xl_info & ~XLR_INFO_MASK);
    char *rec = XLogRecGetData(record);
    switch (info)
    {
        case XLOG_GIN_CREATE_INDEX:
        case XLOG_GIN_CREATE_PTREE:
        case XLOG_GIN_INSERT:
        case XLOG_GIN_SPLIT:
        case XLOG_GIN_VACUUM_PAGE:
        case XLOG_GIN_VACUUM_DATA_LEAF_PAGE:
        case XLOG_GIN_DELETE_PAGE:
        case XLOG_GIN_UPDATE_META_PAGE:
        case XLOG_GIN_INSERT_LISTPAGE:
        case XLOG_GIN_DELETE_LISTPAGE:
        {
            RelFileNode *rec_node = (RelFileNode *) rec;
            *node = *rec_node;
            return 1;
        }

        default:
            THROW_FMT(FormatError, "GIN UNKNOWN: %d", info);
    }
}

static
bool
getGist(XLogRecord *record, RelFileNode *node)
{
    uint8_t info = (uint8_t) (record->xl_info & ~XLR_INFO_MASK);
    char *rec = XLogRecGetData(record);

    switch (info)
    {
        case XLOG_GIST_PAGE_UPDATE:
        case XLOG_GIST_PAGE_SPLIT:
        case XLOG_GIST_CREATE_INDEX:
        {
            RelFileNode *rec_node = (RelFileNode *) rec;
            *node = *rec_node;
            return 1;
        }

        default:
            THROW_FMT(FormatError, "GIST UNKNOWN: %d", info);
    }
}

static
bool
getSeq(XLogRecord *record, RelFileNode *node)
{
    uint8_t info = (uint8_t) (record->xl_info & ~XLR_INFO_MASK);
    char *rec = XLogRecGetData(record);
    switch (info)
    {
        case XLOG_SEQ_LOG:
        {
            RelFileNode *rec_node = (RelFileNode *) rec;
            *node = *rec_node;
            return 1;
        }

        default:
            THROW_FMT(FormatError, "Sequence UNKNOWN: %d", info);
    }
}

static
bool
getSpgist(XLogRecord *record, RelFileNode *node)
{
    uint8_t info = (uint8_t) (record->xl_info & ~XLR_INFO_MASK);
    char *rec = XLogRecGetData(record);

    switch (info)
    {
        case XLOG_SPGIST_CREATE_INDEX:
        case XLOG_SPGIST_ADD_LEAF:
        case XLOG_SPGIST_MOVE_LEAFS:
        case XLOG_SPGIST_ADD_NODE:
        case XLOG_SPGIST_SPLIT_TUPLE:
        case XLOG_SPGIST_PICKSPLIT:
        case XLOG_SPGIST_VACUUM_LEAF:
        case XLOG_SPGIST_VACUUM_ROOT:
        case XLOG_SPGIST_VACUUM_REDIRECT:
        {
            RelFileNode *rec_node = (RelFileNode *) rec;
            *node = *rec_node;
            return 1;
        }

        default:
            THROW_FMT(FormatError, "SPGIST UNKNOWN: %d", info);
    }
}

static
bool
getBitmap(XLogRecord *record, RelFileNode *node)
{
    uint8_t info = (uint8_t) (record->xl_info & ~XLR_INFO_MASK);
    char *rec = XLogRecGetData(record);
    switch (info)
    {
        case XLOG_BITMAP_INSERT_LOVITEM:
        case XLOG_BITMAP_INSERT_META:
        case XLOG_BITMAP_INSERT_BITMAP_LASTWORDS:
        case XLOG_BITMAP_INSERT_WORDS:
        case XLOG_BITMAP_UPDATEWORD:
        case XLOG_BITMAP_UPDATEWORDS:
        {
            RelFileNode *rec_node = (RelFileNode *) rec;
            *node = *rec_node;
            return 1;
        }

        default:
            THROW_FMT(FormatError, "Bitmap UNKNOWN: %d", info);
    }
}

static
bool
getAppendonly(XLogRecord *record, RelFileNode *node)
{
    uint8_t info = (uint8_t) (record->xl_info & ~XLR_INFO_MASK);
    char *rec = XLogRecGetData(record);

    switch (info)
    {
        case XLOG_APPENDONLY_INSERT:
        case XLOG_APPENDONLY_TRUNCATE:
        {
            RelFileNode *rec_node = (RelFileNode *) rec;
            *node = *rec_node;
            return 1;
        }

        default:
            THROW_FMT(FormatError, "Appendonly UNKNOWN: %d", info);
    }
}

FN_EXTERN bool
getRelFileNodeGPDB6(XLogRecord *record, RelFileNode *node)
{
    switch (record->xl_rmid)
    {
        case ResourceManager_XLOG:
            return getXlog(record, node);

        case ResourceManager_Storage:
            return getStorage(record, node);

        case ResourceManager_Heap2:
            return getHeap2(record, node);

        case ResourceManager_Heap:
            return getHeap(record, node);

        case ResourceManager_Btree:
            return getBtree(record, node);

        case ResourceManager_Gin:
            return getGin(record, node);

        case ResourceManager_Gist:
            return getGist(record, node);

        case ResourceManager_Sequence:
            return getSeq(record, node);

        case ResourceManager_SPGist:
            return getSpgist(record, node);

        case ResourceManager_Bitmap:
            return getBitmap(record, node);

        case ResourceManager_Appendonly:
            return getAppendonly(record, node);

        case ResourceManager_Transaction:
        case ResourceManager_CLOG:
        case ResourceManager_Database:
        case ResourceManager_Tablespace:
        case ResourceManager_MultiXact:
        case ResourceManager_RelMap:
        case ResourceManager_Standby:
        case ResourceManager_DistributedLog:
            // skip
            break;

        case ResourceManager_Hash:
            THROW_FMT(FormatError, "Not supported in greenplum. shouldn't be here");

        default:
            THROW_FMT(FormatError, "Unknown resource manager");
    }
    return 0;
}

FN_EXTERN void
validXLogRecordHeaderGPDB6(const XLogRecord *const record)
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
validXLogRecordGPDB6(const XLogRecord *const record)
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