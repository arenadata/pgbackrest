#ifndef PGBACKREST_HARNESSWAL_H
#define PGBACKREST_HARNESSWAL_H
#include "common/type/buffer.h"
#include "common/type/param.h"
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

typedef struct InsertXRecordParam
{
    VAR_PARAM_HEADER;
    uint16_t magic;
    uint32_t begin_offset;
    uint64_t segno;
}InsertXRecordParam;

XLogRecord *hrnGpdbCreateXRecord(uint8_t rmid, uint8_t info, uint32_t body_size, void *body);

#define hrnGpdbWalInsertXRecordP(wal, record, flags, ...) \
    hrnGpdbWalInsertXRecord(wal, record, (InsertXRecordParam){VAR_PARAM_INIT, __VA_ARGS__}, flags)

void hrnGpdbWalInsertXRecord(
    Buffer *const walBuffer,
    XLogRecord *record,
    InsertXRecordParam param,
    InsertRecordFlags flags);
void hrnGpdbWalInsertXRecordSimple(Buffer *const walBuffer, XLogRecord *record);
#endif // PGBACKREST_HARNESSWAL_H
