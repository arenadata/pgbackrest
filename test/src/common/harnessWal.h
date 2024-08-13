#ifndef PGBACKREST_HARNESSWAL_H
#define PGBACKREST_HARNESSWAL_H
#include "common/type/buffer.h"
#include "common/walFilter/greenplumCommon.h"

typedef enum InsertRecordFlags
{
    NO_FLAGS = 0,
    INCOMPLETE_RECORD = 1 << 0,
    OVERWRITE = 1 << 1,
    COND_FLAG = 1 << 2,
    NO_COND_FLAG = 1 << 3,
    ZERO_REM_LEN = 1 << 4,
    WRONG_REM_LEN = 1 << 5
}InsertRecordFlags;

XLogRecord *hrnGpdbCreateXRecord(uint8_t rmid, uint8_t info, uint32_t body_size, void *body);
void hrnGpdbWalInsertXRecord(
    Buffer *const walBuffer,
    XLogRecord *record,
    InsertRecordFlags flags,
    uint16_t magic,
    uint32_t begin_offset);
void hrnGpdbWalInsertXRecordSimple(Buffer *const walBuffer, XLogRecord *record);
#endif // PGBACKREST_HARNESSWAL_H
