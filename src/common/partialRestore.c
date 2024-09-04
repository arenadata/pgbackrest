#include "build.auto.h"
#include "partialRestore.h"
#include "postgres/interface/static.vendor.h"

static int
tableComparator(const Table *const a, const Table *const b)
{
    if (a->spcNode != b->spcNode)
        return a->spcNode > b->spcNode ? 1 : -1;

    if (a->relNode == b->relNode)
        return 0;

    return a->relNode > b->relNode ? 1 : -1;
}

FN_EXTERN __attribute__((unused)) List *
buildFilterList(JsonRead *const json)
{
    List *result = lstNewP(sizeof(DataBase), .comparator = lstComparatorUInt);

    jsonReadArrayBegin(json);
    while (jsonReadTypeNextIgnoreComma(json) != jsonTypeArrayEnd)
    {
        jsonReadObjectBegin(json);
        jsonReadSkip(jsonReadKeyRequireZ(json, "dbName"));

        DataBase dataBase = {
            .dbOid = jsonReadUInt(jsonReadKeyRequireZ(json, "dbOid")),
            .tables = lstNewP(sizeof(Table), .comparator = (ListComparator *) tableComparator)
        };

        jsonReadKeyRequireZ(json, "tables");
        jsonReadArrayBegin(json);
        while (jsonReadTypeNextIgnoreComma(json) != jsonTypeArrayEnd)
        {
            jsonReadObjectBegin(json);
            jsonReadSkip(jsonReadKeyRequireZ(json, "tablefqn"));
            jsonReadSkip(jsonReadKeyRequireZ(json, "tableOid"));

            Table table = {
                .spcNode = jsonReadUInt(jsonReadKeyRequireZ(json, "tablespace")),
                .relNode = jsonReadUInt(jsonReadKeyRequireZ(json, "relfilenode"))
            };
            jsonReadObjectEnd(json);

            lstAdd(dataBase.tables, &table);
        }
        jsonReadArrayEnd(json);
        jsonReadObjectEnd(json);

        lstSort(dataBase.tables, sortOrderAsc);
        lstAdd(result, &dataBase);
    }
    jsonReadArrayEnd(json);

    lstSort(result, sortOrderAsc);

    return result;
}
