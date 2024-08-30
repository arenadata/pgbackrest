#ifndef PGBACKREST_PARTIALRESTORE_H
#define PGBACKREST_PARTIALRESTORE_H

#include "common/type/json.h"
#include "common/type/list.h"
#include "postgres/interface/static.vendor.h"

typedef struct DataBase
{
    Oid dbOid;
    List *tables;
}DataBase;

FN_EXTERN List *buildFilterList(JsonRead *json);

#endif // PGBACKREST_PARTIALRESTORE_H
