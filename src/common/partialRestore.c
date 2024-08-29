#include "build.auto.h"
#include "partialRestore.h"

static RelFileNode *
appendRelFileNode(RelFileNode *array, size_t *len, RelFileNode node)
{
    if (array)
    {
        array = memResize(array, sizeof(RelFileNode) * (++(*len)));
    }
    else
    {
        (*len)++;
        array = memNew(sizeof(node));
    }

    array[*len - 1] = node;

    return array;
}

FN_EXTERN __attribute__((unused)) void
buildFilterList(JsonRead *json, RelFileNode **filter_list, size_t *filter_list_len)
{
    size_t result_len = 0;
    RelFileNode *filter_list_result = NULL;

    jsonReadArrayBegin(json);

    Oid dbOid = 0;
    // Read array of databases
    while (jsonReadTypeNextIgnoreComma(json) != jsonTypeArrayEnd)
    {
        jsonReadObjectBegin(json);

        // Read database info
        while (jsonReadTypeNextIgnoreComma(json) != jsonTypeObjectEnd)
        {
            String *key1 = jsonReadKey(json);
            if (strEqZ(key1, "dbOid"))
            {
                dbOid = jsonReadUInt(json);
            }
            else if (strEqZ(key1, "tables"))
            {
                jsonReadArrayBegin(json);

                size_t table_count = 0;
                // Read tables
                while (jsonReadTypeNextIgnoreComma(json) != jsonTypeArrayEnd)
                {
                    RelFileNode node = {.dbNode = dbOid};
                    jsonReadObjectBegin(json);
                    table_count++;
                    // Read table info
                    while (jsonReadTypeNextIgnoreComma(json) != jsonTypeObjectEnd)
                    {
                        String *key2 = jsonReadKey(json);
                        if (strEqZ(key2, "relfilenode"))
                        {
                            node.relNode = jsonReadUInt(json);
                        }
                        else if (strEqZ(key2, "tablespace"))
                        {
                            node.spcNode = jsonReadUInt(json);
                        }
                        else
                        {
                            jsonReadSkip(json);
                        }
                    }
                    filter_list_result = appendRelFileNode(filter_list_result, &result_len, node);

                    jsonReadObjectEnd(json);
                }

                // If the database does not have any tables specified, then add RelFileNode where spcNode and dbNode are 0.
                if (table_count == 0)
                {
                    RelFileNode node = {
                        .dbNode = dbOid
                    };
                    filter_list_result = appendRelFileNode(filter_list_result, &result_len, node);
                }

                jsonReadArrayEnd(json);
            }
            else
            {
                jsonReadSkip(json);
            }
        }

        jsonReadObjectEnd(json);
    }

    jsonReadArrayEnd(json);

    *filter_list = filter_list_result;
    *filter_list_len = result_len;
}
