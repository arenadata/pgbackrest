#include "build.auto.h"
#include "partialRestore.h"

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
    // read database array
    while (jsonReadTypeNextIgnoreComma(json) != jsonTypeArrayEnd)
    {
        jsonReadObjectBegin(json);
        DataBase dataBase = {0};
        // read database object
        while (jsonReadTypeNextIgnoreComma(json) != jsonTypeObjectEnd)
        {
            const String *const dbKey = jsonReadKey(json);
            if (strEqZ(dbKey, "dbOid"))
            {
                dataBase.dbOid = jsonReadUInt(json);
            }
            else if (strEqZ(dbKey, "tables"))
            {
                jsonReadArrayBegin(json);
                dataBase.tables = lstNewP(sizeof(Table), .comparator = (ListComparator *) tableComparator);
                // read table array
                while (jsonReadTypeNextIgnoreComma(json) != jsonTypeArrayEnd)
                {
                    jsonReadObjectBegin(json);
                    Table table = {0};
                    // read table object
                    while (jsonReadTypeNextIgnoreComma(json) != jsonTypeObjectEnd)
                    {
                        const String *const tableKey = jsonReadKey(json);
                        if (strEqZ(tableKey, "tablespace"))
                        {
                            table.spcNode = jsonReadUInt(json);
                        }
                        else if (strEqZ(tableKey, "relfilenode"))
                        {
                            table.relNode = jsonReadUInt(json);
                        }
                        else
                        {
                            jsonReadSkip(json);
                        }
                    }
                    jsonReadObjectEnd(json);

                    if (table.spcNode == 0)
                    {
                        THROW(FormatError, "tablespace field of table is missing");
                    }
                    else if (table.relNode == 0)
                    {
                        THROW(FormatError, "relfilenode field of table is missing");
                    }

                    lstAdd(dataBase.tables, &table);
                }
                jsonReadArrayEnd(json);
            }
            else
            {
                jsonReadSkip(json);
            }
        }
        jsonReadObjectEnd(json);

        if (dataBase.dbOid == 0)
        {
            THROW(FormatError, "dbOid field of table is missing");
        }
        else if (dataBase.tables == NULL)
        {
            THROW(FormatError, "tables field of table is missing");
        }

        lstSort(dataBase.tables, sortOrderAsc);
        lstAdd(result, &dataBase);
    }
    jsonReadArrayEnd(json);

    lstSort(result, sortOrderAsc);

    return result;
}
