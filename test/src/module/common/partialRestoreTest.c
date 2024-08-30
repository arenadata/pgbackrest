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
        const String *jsonstr = STRDEF("["
                                       "  {"
                                       "    \"dbName\": \"db1\","
                                       "    \"dbOid\": 20000,"
                                       "    \"tables\": ["
                                       "      {"
                                       "        \"tablefqn\": \"public.t1\","
                                       "        \"tableOid\": 16384,"
                                       "        \"tablespace\": 1600,"
                                       "        \"relfilenode\": 16384"
                                       "      },"
                                       "      {"
                                       "        \"tablefqn\": \"public.t2\","
                                       "        \"tableOid\": 16387,"
                                       "        \"tablespace\": 1601,"
                                       "        \"relfilenode\": 16385"
                                       "      },"
                                       "      {"
                                       "        \"tablefqn\": \"public.t3\","
                                       "        \"tableOid\": 16388,"
                                       "        \"tablespace\": 1600,"
                                       "        \"relfilenode\": 16386"
                                       "      }"
                                       "    ]"
                                       "  },"
                                       "  {"
                                       "    \"dbName\": \"db2\","
                                       "    \"dbOid\": 20001,"
                                       "    \"tables\": ["
                                       "      {"
                                       "        \"tablefqn\": \"public.t3\","
                                       "        \"tableOid\": 16390,"
                                       "        \"tablespace\": 1700,"
                                       "        \"relfilenode\": 16386"
                                       "      },"
                                       "      {"
                                       "        \"tablefqn\": \"public.t3\","
                                       "        \"tableOid\": 16390,"
                                       "        \"tablespace\": 1700,"
                                       "        \"relfilenode\": 11000"
                                       "      },"
                                       "      {"
                                       "        \"tablefqn\": \"public.t4\","
                                       "        \"tableOid\": 16390,"
                                       "        \"tablespace\": 1701,"
                                       "        \"relfilenode\": 10000"
                                       "      }"
                                       "    ]"
                                       "  },"
                                       "  {"
                                       "    \"dbName\": \"db3\","
                                       "    \"dbOid\": 20002,"
                                       "    \"tables\": []"
                                       "  }"
                                       "]");
        JsonRead *json = jsonReadNew(jsonstr);
        List *filterList = buildFilterList(json);
        TEST_RESULT_UINT(lstSize(filterList), 7, "tables count");
        TEST_RESULT_INT(
            memcmp(
                &((RelFileNode){.spcNode = 1600, .dbNode = 20000, .relNode = 16384}), lstGet(filterList, 0), sizeof(RelFileNode)),
            0, "Check the 1st element");
        TEST_RESULT_INT(
            memcmp(
                &((RelFileNode){.spcNode = 1600, .dbNode = 20000, .relNode = 16386}), lstGet(filterList, 1), sizeof(RelFileNode)),
            0, "Check the 2nd element");
        TEST_RESULT_INT(
            memcmp(
                &((RelFileNode){.spcNode = 1601, .dbNode = 20000, .relNode = 16385}), lstGet(filterList, 2), sizeof(RelFileNode)),
            0, "Check the 3rd element");
        TEST_RESULT_INT(
            memcmp(
                &((RelFileNode){.spcNode = 1700, .dbNode = 20001, .relNode = 11000}), lstGet(filterList, 3), sizeof(RelFileNode)),
            0, "Check the 4th element");
        TEST_RESULT_INT(
            memcmp(
                &((RelFileNode){.spcNode = 1700, .dbNode = 20001, .relNode = 16386}), lstGet(filterList, 4), sizeof(RelFileNode)),
            0, "Check the 5th element");
        TEST_RESULT_INT(
            memcmp(
                &((RelFileNode){.spcNode = 1701, .dbNode = 20001, .relNode = 10000}), lstGet(filterList, 5), sizeof(RelFileNode)),
            0, "Check the 6th element");
        TEST_RESULT_INT(
            memcmp(
                &((RelFileNode){.spcNode = 0, .dbNode = 20002, .relNode = 0}), lstGet(filterList, 6), sizeof(RelFileNode)),
            0, "Check the 7th element");
        // This is necessary to cover the comparator
        RelFileNode *found = lstFind(filterList, &((RelFileNode){.spcNode = 0, .dbNode = 20002, .relNode = 0}));
        TEST_RESULT_INT(
            memcmp(&((RelFileNode){.spcNode = 0, .dbNode = 20002, .relNode = 0}), found, sizeof(RelFileNode)), 0, "test find");
    }
}
