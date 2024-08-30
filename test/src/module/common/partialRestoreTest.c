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
                                       "        \"tablefqn\": \"public.t1\",\n"
                                       "        \"tableOid\": 16384,\n"
                                       "        \"tablespace\": 1600,\n"
                                       "        \"relfilenode\": 16384\n"
                                       "      },\n"
                                       "      {\n"
                                       "        \"tablefqn\": \"public.t2\",\n"
                                       "        \"tableOid\": 16387,\n"
                                       "        \"tablespace\": 1601,\n"
                                       "        \"relfilenode\": 16385\n"
                                       "      },\n"
                                       "      {\n"
                                       "        \"tablefqn\": \"public.t3\",\n"
                                       "        \"tableOid\": 16388,\n"
                                       "        \"tablespace\": 1600,\n"
                                       "        \"relfilenode\": 16386\n"
                                       "      }\n"
                                       "    ]\n"
                                       "  },\n"
                                       "  {\n"
                                       "    \"dbName\": \"db2\",\n"
                                       "    \"dbOid\": 20001,\n"
                                       "    \"tables\": [\n"
                                       "      {\n"
                                       "        \"tablefqn\": \"public.t3\",\n"
                                       "        \"tableOid\": 16390,\n"
                                       "        \"tablespace\": 1700,\n"
                                       "        \"relfilenode\": 16386\n"
                                       "      },\n"
                                       "      {\n"
                                       "        \"tablefqn\": \"public.t3\",\n"
                                       "        \"tableOid\": 16390,\n"
                                       "        \"tablespace\": 1700,\n"
                                       "        \"relfilenode\": 11000\n"
                                       "      },\n"
                                       "      {\n"
                                       "        \"tablefqn\": \"public.t4\",\n"
                                       "        \"tableOid\": 16390,\n"
                                       "        \"tablespace\": 1701,\n"
                                       "        \"relfilenode\": 10000\n"
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
        List *filterList = buildFilterList(json);
        TEST_RESULT_UINT(lstSize(filterList), 7, "tables count");
        TEST_RESULT_BOOL(memcmp(
                             &((RelFileNode){.spcNode = 1600, .dbNode = 20000, .relNode = 16384}),
                             lstGet(filterList, 0), sizeof(RelFileNode)) == 0, true, "Check the 1st element");
        TEST_RESULT_BOOL(memcmp(
                             &((RelFileNode){.spcNode = 1600, .dbNode = 20000, .relNode = 16386}),
                             lstGet(filterList, 1), sizeof(RelFileNode)) == 0, true, "Check the 2nd element");
        TEST_RESULT_BOOL(memcmp(
                             &((RelFileNode){.spcNode = 1601, .dbNode = 20000, .relNode = 16385}),
                             lstGet(filterList, 2), sizeof(RelFileNode)) == 0, true, "Check the 3rd element");
        TEST_RESULT_BOOL(memcmp(
                             &((RelFileNode){.spcNode = 1700, .dbNode = 20001, .relNode = 11000}),
                             lstGet(filterList, 3), sizeof(RelFileNode)) == 0, true, "Check the 4th element");
        TEST_RESULT_BOOL(memcmp(
                             &((RelFileNode){.spcNode = 1700, .dbNode = 20001, .relNode = 16386}),
                             lstGet(filterList, 4), sizeof(RelFileNode)) == 0, true, "Check the 5th element");
        TEST_RESULT_BOOL(memcmp(
                             &((RelFileNode){.spcNode = 1701, .dbNode = 20001, .relNode = 10000}),
                             lstGet(filterList, 5), sizeof(RelFileNode)) == 0, true, "Check the 6th element");
        TEST_RESULT_BOOL(memcmp(
                             &((RelFileNode){.spcNode = 0, .dbNode = 20002, .relNode = 0}),
                             lstGet(filterList, 6), sizeof(RelFileNode)) == 0, true, "Check the 7th element");
        // This is necessary to cover the comparator
        RelFileNode *found = lstFind(filterList, &((RelFileNode){.spcNode = 0, .dbNode = 20002, .relNode = 0}));
        TEST_RESULT_BOOL(memcmp(
                             &((RelFileNode){.spcNode = 0, .dbNode = 20002, .relNode = 0}), found,
                             sizeof(RelFileNode)) == 0, true, "test find");
    }
}
