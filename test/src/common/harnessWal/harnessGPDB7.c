#include "build.auto.h"

#include "../harnessWal.h"
#include "common/type/object.h"
#include "common/walFilter/versions/definitionsGPDB7.h"
#include "string.h"

#define TRANSACTION_ID_PLACEHOLDER 0xADDE
#define PREV_RECPTR_PLACEHOLDER 0xAABB
#define RECORD_BODY_PLACEHOLDER 0XAB

#define WRITE_FIELD(type, data)                           \
    do {                                                  \
        recordSize += sizeof(type);                       \
        record = memResize(record, recordSize);           \
        *((type *) ((uint8_t *) record + offset)) = data; \
        offset += sizeof(type);                           \
    } while (0)

XLogRecordBase *
hrnGpdbCreateXRecord12GPDB(uint8_t rmid, uint8_t info, CreateXRecordParam param)
{
    size_t recordSize = sizeof(XLogRecordGPDB7);

    XLogRecordGPDB7 *record = memNew(recordSize);
    *record = (XLogRecordGPDB7){
        .xl_xid = TRANSACTION_ID_PLACEHOLDER,
        .xl_info = info,
        .xl_rmid = (uint8_t) rmid,
        .xl_prev = PREV_RECPTR_PLACEHOLDER
    };

    size_t offset = sizeof(XLogRecordGPDB7);

    if (param.has_origin)
    {
        // block id
        WRITE_FIELD(uint8_t, XLR_BLOCK_ID_ORIGIN);
        WRITE_FIELD(uint16_t, 0);
    }

    if (param.backupBlocks && !lstEmpty(param.backupBlocks))
    {
        for (unsigned int i = 0; i < lstSize(param.backupBlocks); i++)
        {
            BackupBlockInfoGPDB7 *block = lstGet(param.backupBlocks, i);
            WRITE_FIELD(uint8_t, block->block_id);
            WRITE_FIELD(uint8_t, block->fork_flags);
            WRITE_FIELD(uint16_t, block->data_length);

            if (block->fork_flags & BKPBLOCK_HAS_IMAGE)
            {
                WRITE_FIELD(uint16_t, block->bimg_len);
                WRITE_FIELD(uint16_t, block->hole_offset);
                WRITE_FIELD(uint8_t, block->bimg_info);

                if (block->bimg_info & BKPIMAGE_HAS_HOLE)
                {
                    WRITE_FIELD(uint16_t, block->hole_length);
                }
            }

            if (!(block->fork_flags & BKPBLOCK_SAME_REL))
            {
                WRITE_FIELD(RelFileNode, block->relFileNode);
            }
            WRITE_FIELD(BlockNumber, block->blockNumber);
        }
    }

    if (param.main_data_size != 0)
    {
        if (param.main_data_size <= UINT8_MAX)
        {
            uint8_t bodySizeSmall = (uint8_t) param.main_data_size;
            // block id
            WRITE_FIELD(uint8_t, XLR_BLOCK_ID_DATA_SHORT);
            WRITE_FIELD(uint8_t, bodySizeSmall);
        }
        else
        {
            // block id
            WRITE_FIELD(uint8_t, XLR_BLOCK_ID_DATA_LONG);
            WRITE_FIELD(uint32_t, param.main_data_size);
        }
    }

    if (param.backupBlocks && !lstEmpty(param.backupBlocks))
    {
        for (unsigned int i = 0; i < lstSize(param.backupBlocks); i++)
        {
            BackupBlockInfoGPDB7 *block = lstGet(param.backupBlocks, i);

            recordSize += block->bimg_len;
            record = memResize(record, recordSize);
            if (block->fork_flags & BKPBLOCK_HAS_IMAGE)
            {
                memset((uint8_t *) record + offset, RECORD_BODY_PLACEHOLDER, block->bimg_len);
                offset += block->bimg_len;
            }

            recordSize += block->data_length;
            record = memResize(record, recordSize);
            if (block->data)
            {
                memcpy((uint8_t *) record + offset, block->data, block->data_length);
            }
            else
            {
                memset((uint8_t *) record + offset, RECORD_BODY_PLACEHOLDER, block->data_length);
            }
            offset += block->data_length;
        }
    }

    recordSize += param.main_data_size;
    record = memResize(record, recordSize);
    if (param.main_data == NULL)
        memset((uint8_t *) record + offset, RECORD_BODY_PLACEHOLDER, param.main_data_size);
    else
        memcpy((uint8_t *) record + offset, param.main_data, param.main_data_size);

    ASSERT(recordSize <= UINT32_MAX);
    record->xl_tot_len = (uint32_t) recordSize;
    if (param.xl_crc == 0)
        record->xl_crc = xLogRecordChecksumGPDB7(record);
    else
        record->xl_crc = param.xl_crc;

    return (XLogRecordBase *) record;
}
