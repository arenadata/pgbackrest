#include "build.auto.h"
#include "partialRestore.h"

FN_EXTERN __attribute__((unused)) int
relFileNodeComparator(const void *item1, const void *item2)
{
    RelFileNode *a = (RelFileNode *) item1;
    RelFileNode *b = (RelFileNode *) item2;

    if (a->dbNode != b->dbNode)
    {
        return (int) (a->dbNode - b->dbNode);
    }
    if (a->spcNode != b->spcNode)
    {
        return (int) (a->spcNode - b->spcNode);
    }
    return (int) (a->relNode - b->relNode);
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
        while (jsonReadTypeNextIgnoreComma(json) != jsonTypeObjectEnd)
        {
            jsonReadSkip(jsonReadKeyRequireZ(json, "dbName"));
            Oid dbOid = jsonReadUInt(jsonReadKeyRequireZ(json, "dbOid"));
            jsonReadKeyRequireZ(json, "tables");
            size_t tableCount = 0;
            jsonReadArrayBegin(json);
            while (jsonReadTypeNextIgnoreComma(json) != jsonTypeArrayEnd)
            {
                tableCount++;
                jsonReadObjectBegin(json);
                jsonReadSkip(jsonReadKeyRequireZ(json, "tablefqn"));
                jsonReadSkip(jsonReadKeyRequireZ(json, "tableOid"));
                Oid tablespace = jsonReadUInt(jsonReadKeyRequireZ(json, "tablespace"));
                Oid relfilenode = jsonReadUInt(jsonReadKeyRequireZ(json, "relfilenode"));

                RelFileNode node = {
                    .spcNode = tablespace,
                    .dbNode = dbOid,
                    .relNode = relfilenode
                };
                lstAdd(result, &node);

                jsonReadObjectEnd(json);
            }

            if (tableCount == 0)
            {
                RelFileNode node = {.dbNode = dbOid};
                lstAdd(result, &node);
            }

            jsonReadArrayEnd(json);
        }
        jsonReadObjectEnd(json);
    }
    jsonReadArrayEnd(json);

    lstSort(result, sortOrderAsc);

    return result;
}
