#include "build.auto.h"
#include "partialRestore.h"
#include "postgres/interface/static.vendor.h"

static int
relFileNodeComparator(const RelFileNode *const a, const RelFileNode *const b)
{
    if (a->dbNode != b->dbNode)
        return a->dbNode > b->dbNode ? 1 : -1;

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
        RelFileNode node = {
            .dbNode = jsonReadUInt(jsonReadKeyRequireZ(json, "dbOid"))
        };

        List *tableList = lstNewP(sizeof(RelFileNode), .comparator = (ListComparator *) relFileNodeComparator);

        jsonReadKeyRequireZ(json, "tables");

        jsonReadArrayBegin(json);
        while (jsonReadTypeNextIgnoreComma(json) != jsonTypeArrayEnd)
        {
            jsonReadObjectBegin(json);
            jsonReadSkip(jsonReadKeyRequireZ(json, "tablefqn"));
            jsonReadSkip(jsonReadKeyRequireZ(json, "tableOid"));

            node.spcNode = jsonReadUInt(jsonReadKeyRequireZ(json, "tablespace"));
            node.relNode = jsonReadUInt(jsonReadKeyRequireZ(json, "relfilenode"));

            lstAdd(tableList, &node);

            jsonReadObjectEnd(json);
        }

        lstSort(tableList, sortOrderAsc);

        DataBase dataBase = {
            .dbOid = node.dbNode,
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
