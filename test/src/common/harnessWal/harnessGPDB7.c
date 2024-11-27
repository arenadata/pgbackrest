#include "build.auto.h"

#include "../harnessWal.h"
#include "common/type/object.h"
#include "common/walFilter/versions/definitionsGPDB7.h"
#include "string.h"

#define TRANSACTION_ID_PLACEHOLDER 0xADDE
#define PREV_RECPTR_PLACEHOLDER 0xAABB
#define RECORD_BODY_PLACEHOLDER 0XAB

#define WRITE_FIELD(ptr, type, data) \
    do {                             \
        *((type *) ptr) = data;      \
        ptr += sizeof(type);         \
    } while (0)

XLogRecordBase *
hrnGpdbCreateXRecord12GPDB(uint8_t rmid, uint8_t info, CreateXRecordParam param)
{
    uint32_t bodySize = 0;
    if (param.main_data_size != 0){
        bodySize += (uint32_t) sizeof(uint8_t);
        bodySize += (uint32_t) (param.main_data_size <= UINT8_MAX ? sizeof(uint8_t) : sizeof(uint32_t));
        bodySize += param.main_data_size;
    }

    if (param.has_origin){
        bodySize += (uint32_t) sizeof(uint8_t);
        bodySize += (uint32_t) sizeof(uint16_t);
    }

    if (param.backupBlocks && !lstEmpty(param.backupBlocks))
    {
        for (unsigned int i = 0; i < lstSize(param.backupBlocks); ++i)
        {
            bodySize += (uint32_t) sizeof(uint8_t);
            BackupBlockInfoGPDB7 *block = lstGet(param.backupBlocks, i);
            bodySize += (uint32_t) sizeof(block->fork_flags);
            bodySize += (uint32_t) sizeof(block->data_length);

            if (block->fork_flags & BKPBLOCK_HAS_IMAGE)
            {
                bodySize += (uint32_t) sizeof(block->bimg_len);
                bodySize += (uint32_t) sizeof(block->hole_offset);
                bodySize += (uint32_t) sizeof(block->bimg_info);

                if (block->bimg_info & BKPIMAGE_HAS_HOLE)
                {
                    bodySize += (uint32_t) sizeof(block->hole_length);
                }
                bodySize += block->bimg_len;
            }

            if (!(block->fork_flags & BKPBLOCK_SAME_REL))
                bodySize += (uint32_t) sizeof(block->relFileNode);
            bodySize += (uint32_t) sizeof(block->blockNumber);
            bodySize += block->data_length;
        }
    }

    XLogRecordGPDB7 *record = memNew(SizeOfXLogRecordGPDB7 + bodySize);
    *record = (XLogRecordGPDB7){
        .xl_tot_len = (uint32_t) (SizeOfXLogRecordGPDB7 + bodySize),
        .xl_xid = TRANSACTION_ID_PLACEHOLDER,
        .xl_info = info,
        .xl_rmid = (uint8_t) rmid,
        .xl_prev = PREV_RECPTR_PLACEHOLDER
    };

    char *body_ptr = (char *) record + sizeof(XLogRecordGPDB7);

    if (param.has_origin){
        *((uint8_t *) body_ptr) = XLR_BLOCK_ID_ORIGIN;
        body_ptr += (uint32_t) sizeof(uint8_t);
        *((uint16_t *) body_ptr) = 0;
        body_ptr += (uint32_t) sizeof(uint16_t);
    }

    if (param.backupBlocks && !lstEmpty(param.backupBlocks))
    {
        for (unsigned int i = 0; i < lstSize(param.backupBlocks); ++i)
        {
            BackupBlockInfoGPDB7 *block = lstGet(param.backupBlocks, i);
            WRITE_FIELD(body_ptr, uint8_t, block->block_id);
            WRITE_FIELD(body_ptr, uint8_t, block->fork_flags);
            WRITE_FIELD(body_ptr, uint16_t, block->data_length);

            if (block->fork_flags & BKPBLOCK_HAS_IMAGE)
            {
                WRITE_FIELD(body_ptr, uint16_t, block->bimg_len);
                WRITE_FIELD(body_ptr, uint16_t, block->hole_offset);
                WRITE_FIELD(body_ptr, uint8_t, block->bimg_info);

                if (block->bimg_info & BKPIMAGE_HAS_HOLE)
                {
                    WRITE_FIELD(body_ptr, uint16_t, block->hole_length);
                }
            }

            if (!(block->fork_flags & BKPBLOCK_SAME_REL))
            {
                WRITE_FIELD(body_ptr, RelFileNode, block->relFileNode);
            }
            WRITE_FIELD(body_ptr, BlockNumber, block->blockNumber);
        }
    }

    if (param.main_data_size != 0)
    {
        if (param.main_data_size <= UINT8_MAX)
        {
            *((uint8_t *) body_ptr) = XLR_BLOCK_ID_DATA_SHORT;
            body_ptr += sizeof(uint8_t);
            uint8_t bodySizeSmall = (uint8_t) param.main_data_size;
            *((uint8_t *) body_ptr) = bodySizeSmall;
            body_ptr += sizeof(bodySizeSmall);
        }
        else
        {
            *((uint8_t *) body_ptr) = XLR_BLOCK_ID_DATA_LONG;
            body_ptr += sizeof(uint8_t);
            *((uint32_t *) body_ptr) = param.main_data_size;
            body_ptr += sizeof(param.main_data_size);
        }
    }

    if (param.backupBlocks && !lstEmpty(param.backupBlocks))
    {
        for (unsigned int i = 0; i < lstSize(param.backupBlocks); ++i)
        {
            BackupBlockInfoGPDB7 *block = lstGet(param.backupBlocks, i);

            if (block->fork_flags & BKPBLOCK_HAS_IMAGE)
            {
                memset(body_ptr, RECORD_BODY_PLACEHOLDER, block->bimg_len);
                body_ptr += block->bimg_len;
            }

            if (block->data)
            {
                memcpy(body_ptr, block->data,block->data_length);
            }
            else
            {
                memset(body_ptr, RECORD_BODY_PLACEHOLDER,block->data_length);
            }
            body_ptr += block->data_length;
        }
    }

    if (param.main_data == NULL)
        memset(body_ptr, RECORD_BODY_PLACEHOLDER, param.main_data_size);
    else
        memcpy(body_ptr, param.main_data, param.main_data_size);

    if (param.xl_crc == 0)
        record->xl_crc = xLogRecordChecksumGPDB7(record);
    else
        record->xl_crc = param.xl_crc;

    return (XLogRecordBase *) record;
}
