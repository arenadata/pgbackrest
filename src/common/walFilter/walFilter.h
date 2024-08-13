#ifndef PGBACKREST_WALFILTER_H
#define PGBACKREST_WALFILTER_H

#include "common/io/filter/filter.h"
#include "common/type/json.h"
#include "postgres/interface/static.vendor.h"

#define WAL_FILTER_TYPE                                   STRID5("wal-fltr", 0x95186db0370)

FN_EXTERN IoFilter *walFilterNew(unsigned int pgVersion, StringId fork, RelFileNode *filter_list, size_t filter_list_len);

FN_EXTERN void buildFilterList(JsonRead *json, RelFileNode **filter_list, size_t *filter_list_len);

#endif // PGBACKREST_WALFILTER_H
