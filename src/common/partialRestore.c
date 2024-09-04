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
        // Read database info
        jsonReadSkip(jsonReadKeyRequireZ(json, "dbName"));
        Oid dbOid = jsonReadUInt(jsonReadKeyRequireZ(json, "dbOid"));

        List *tableList = lstNewP(sizeof(Table), .comparator = (ListComparator *) tableComparator);

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

            lstAdd(tableList, &table);

            jsonReadObjectEnd(json);
        }

        lstSort(tableList, sortOrderAsc);

        DataBase dataBase = {
            .dbOid = dbOid,
            .tables = tableList
        };

        lstAdd(result, &dataBase);

        jsonReadArrayEnd(json);

        jsonReadObjectEnd(json);
    }
    jsonReadArrayEnd(json);

    lstSort(result, sortOrderAsc);

    return result;
}
