#ifndef COMMON_WALFILTER_WALFILTER_H
#define COMMON_WALFILTER_WALFILTER_H

#include "command/archive/get/file.h"
#include "postgres/interface.h"

#define WAL_FILTER_TYPE                                   STRID5("wal-fltr", 0x95186db0370)

FN_EXTERN IoFilter *walFilterNew(StringId fork, PgControl pgControl, const ArchiveGetFile *archiveInfo);

#endif // COMMON_WALFILTER_WALFILTER_H
