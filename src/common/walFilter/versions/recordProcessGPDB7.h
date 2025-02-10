#ifndef PGBACKREST_RECORDPROCRESSGPDB7_H
#define PGBACKREST_RECORDPROCRESSGPDB7_H

#include "build.auto.h"
#include "common/walFilter/postgresCommon.h"

#define GPDB7_XLOG_PAGE_MAGIC 0xD101

FN_EXTERN void validXLogRecordHeaderGPDB7(const XLogRecordBase *record, PgPageSize heapPageSize);
FN_EXTERN void validXLogRecordGPDB7(const XLogRecordBase *recordBase, PgPageSize heapPageSize);
FN_EXTERN bool xLogRecordIsWalSwitchGPDB7(const XLogRecordBase *record);
FN_EXTERN void filterRecordGPDB7(XLogRecordBase *recordBase, PgPageSize pageSize);
FN_EXTERN WalInterface getWalInterfaceGPDB7(void);
#endif // PGBACKREST_RECORDPROCRESSGPDB7_H
