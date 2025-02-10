#ifndef COMMON_WALFILTER_VERSIONS_RECORDPROCESSGPDB6_H
#define COMMON_WALFILTER_VERSIONS_RECORDPROCESSGPDB6_H

#include "common/walFilter/postgresCommon.h"

#define GPDB6_XLOG_PAGE_MAGIC 0xD07E

FN_EXTERN void validXLogRecordHeaderGPDB6(const XLogRecordBase *record, PgPageSize heapPageSize);
FN_EXTERN void validXLogRecordGPDB6(const XLogRecordBase *record, PgPageSize heapPageSize);
FN_EXTERN bool xLogRecordIsWalSwitchGPDB6(const XLogRecordBase *record);
FN_EXTERN void filterRecordGPDB6(XLogRecordBase *recordBase, PgPageSize pageSize);
FN_EXTERN WalInterface getWalInterfaceGPDB6(void);
#endif // COMMON_WALFILTER_VERSIONS_RECORDPROCESSGPDB6_H
