#ifndef PGBACKREST_WALFILTER_H
#define PGBACKREST_WALFILTER_H

#include "common/io/filter/filter.h"
#include "common/type/json.h"

#define WAL_FILTER_TYPE                                   STRID5("wal-fltr", 0x95186db0370)

FN_EXTERN IoFilter *walFilterNew(JsonRead *json);

#endif // PGBACKREST_WALFILTER_H
