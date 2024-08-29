#ifndef PGBACKREST_PARTIALRESTORE_H
#define PGBACKREST_PARTIALRESTORE_H

#include "common/type/json.h"
#include "postgres/interface/static.vendor.h"

FN_EXTERN void buildFilterList(JsonRead *json, RelFileNode **filter_list, size_t *filter_list_len);

#endif // PGBACKREST_PARTIALRESTORE_H
