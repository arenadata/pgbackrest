#ifndef PGBACKREST_PARTIALRESTORE_H
#define PGBACKREST_PARTIALRESTORE_H

#include "common/type/json.h"
#include "postgres/interface/static.vendor.h"

FN_EXTERN List *buildFilterList(JsonRead *json);

#endif // PGBACKREST_PARTIALRESTORE_H
