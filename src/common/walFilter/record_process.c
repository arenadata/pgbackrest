#include "build.auto.h"

#include <malloc.h>
#include <stdbool.h>
#include <stdio.h>
#include "common/log.h"
#include "record_process.h"

typedef uint32_t CommandId;
typedef uint16_t OffsetNumber;

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
get_xlog(XLogRecord *record, RelFileNode *node)
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
get_storage(XLogRecord *record, RelFileNode *node)
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
get_heap2(XLogRecord *record, RelFileNode *node)
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
get_heap(XLogRecord *record, RelFileNode *node)
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
get_btree(XLogRecord *record, RelFileNode *node)
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
get_gin(XLogRecord *record, RelFileNode *node)
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
get_gist(XLogRecord *record, RelFileNode *node)
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
get_seq(XLogRecord *record, RelFileNode *node)
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
get_spgist(XLogRecord *record, RelFileNode *node)
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
get_bitmap(XLogRecord *record, RelFileNode *node)
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
get_appendonly(XLogRecord *record, RelFileNode *node)
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
get_relfilenode(XLogRecord *record, RelFileNode *node)
{
    switch (record->xl_rmid)
    {
        case ResourceManager_XLOG:
            return get_xlog(record, node);

        case ResourceManager_Storage:
            return get_storage(record, node);

        case ResourceManager_Heap2:
            return get_heap2(record, node);

        case ResourceManager_Heap:
            return get_heap(record, node);

        case ResourceManager_Btree:
            return get_btree(record, node);

        case ResourceManager_Gin:
            return get_gin(record, node);

        case ResourceManager_Gist:
            return get_gist(record, node);

        case ResourceManager_Sequence:
            return get_seq(record, node);

        case ResourceManager_SPGist:
            return get_spgist(record, node);

        case ResourceManager_Bitmap:
            return get_bitmap(record, node);

        case ResourceManager_Appendonly:
            return get_appendonly(record, node);

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
