#include "build.auto.h"
#include "partialRestore.h"
#include "postgres/interface/static.vendor.h"

static int
relFileNodeComparator(const void *item1, const void *item2)
{
    RelFileNode *a = (RelFileNode *) item1;
    RelFileNode *b = (RelFileNode *) item2;

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
    List *result = lstNewP(sizeof(RelFileNode), .comparator = relFileNodeComparator);

    jsonReadArrayBegin(json);

    while (jsonReadTypeNextIgnoreComma(json) != jsonTypeArrayEnd)
    {
        jsonReadObjectBegin(json);
        // Read database info
        jsonReadSkip(jsonReadKeyRequireZ(json, "dbName"));
        RelFileNode node = {
            .dbNode = jsonReadUInt(jsonReadKeyRequireZ(json, "dbOid"))
        };

        jsonReadKeyRequireZ(json, "tables");
        size_t tableCount = 0;
        jsonReadArrayBegin(json);
        while (jsonReadTypeNextIgnoreComma(json) != jsonTypeArrayEnd)
        {
            tableCount++;
            jsonReadObjectBegin(json);
            jsonReadSkip(jsonReadKeyRequireZ(json, "tablefqn"));
            jsonReadSkip(jsonReadKeyRequireZ(json, "tableOid"));

            node.spcNode = jsonReadUInt(jsonReadKeyRequireZ(json, "tablespace"));
            node.relNode = jsonReadUInt(jsonReadKeyRequireZ(json, "relfilenode"));

            lstAdd(result, &node);

            jsonReadObjectEnd(json);
        }

        if (tableCount == 0)
            lstAdd(result, &node);

        jsonReadArrayEnd(json);

        jsonReadObjectEnd(json);
    }
    jsonReadArrayEnd(json);

    lstSort(result, sortOrderAsc);

    return result;
}
