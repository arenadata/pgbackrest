#ifndef PGBACKREST_WALFILTER_H
#define PGBACKREST_WALFILTER_H

#include "command/archive/get/file.h"
#include "common/io/filter/filter.h"
#include "common/type/json.h"
#include "postgres/interface/static.vendor.h"

#define WAL_FILTER_TYPE                                   STRID5("wal-fltr", 0x95186db0370)

FN_EXTERN IoFilter *walFilterNew(unsigned int pgVersion, StringId fork, PgPageSize pageSize, const ArchiveGetFile *archiveInfo);

#endif // PGBACKREST_WALFILTER_H
