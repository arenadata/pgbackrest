/***********************************************************************************************************************************
Test Run
***********************************************************************************************************************************/
#include "common/type/json.h"
#include "postgres/interface/static.vendor.h"

static void
testRun(void)
{
    if (testBegin("parse json"))
    {
        const String *jsonstr = STRDEF("[\n"
                                       "  {\n"
                                       "    \"dbName\": \"db1\",\n"
                                       "    \"dbOid\": 20000,\n"
                                       "    \"tables\": [\n"
                                       "      {\n"
                                       "        \"tablefqn\": \"t1\",\n"
                                       "        \"tableOid\": 16384,\n"
                                       "        \"tablespace\": 1600,\n"
                                       "        \"relfilenode\": 16384\n"
                                       "      },\n"
                                       "      {\n"
                                       "        \"tablefqn\": \"t2\",\n"
                                       "        \"tableOid\": 16387,\n"
                                       "        \"tablespace\": 1601,\n"
                                       "        \"relfilenode\": 16385\n"
                                       "      }\n"
                                       "    ]\n"
                                       "  },\n"
                                       "  {\n"
                                       "    \"dbName\": \"db2\",\n"
                                       "    \"dbOid\": 20001,\n"
                                       "    \"tables\": [\n"
                                       "      {\n"
                                       "        \"tablefqn\": \"t3\",\n"
                                       "        \"tableOid\": 16390,\n"
                                       "        \"tablespace\": 1700,\n"
                                       "        \"relfilenode\": 16386\n"
                                       "      }\n"
                                       "    ]\n"
                                       "  },\n"
                                       "  {\n"
                                       "    \"dbName\": \"db3\",\n"
                                       "    \"dbOid\": 20002,\n"
                                       "    \"tables\": []\n"
                                       "  }\n"
                                       "]");
        JsonRead *json = jsonReadNew(jsonstr);
        RelFileNode *filter_list = NULL;
        size_t filter_list_len = 0;
        buildFilterList(json, &filter_list, &filter_list_len);

        RelFileNode filter_list_expect[] = {
            {
                1600,
                20000,
                16384
            },
            {
                1601,
                20000,
                16385
            },
            {
                1700,
                20001,
                16386
            },
            {
                0,
                20002,
                0
            }
        };
        TEST_RESULT_UINT(filter_list_len, 4, "filter array size");
        TEST_RESULT_BOOL(memcmp(filter_list_expect, filter_list, sizeof(RelFileNode) * 3) == 0, true,  "filter array content");
    }
}
