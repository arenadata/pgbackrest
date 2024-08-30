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
                                       "    \"dbName\": \"db3\","
                                       "    \"dbOid\": 20002,"
                                       "    \"tables\": []"
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
                                       "  }"
                                       "]");
        JsonRead *json = jsonReadNew(jsonstr);
        List *filterList = NULL;
        TEST_ASSIGN(filterList, buildFilterList(json), "create filter list");
        TEST_RESULT_UINT(lstSize(filterList), 3, "database count");

        DataBase *db1 = NULL;
        DataBase *db2 = NULL;
        DataBase *db3 = NULL;

        TEST_ASSIGN(db1, (DataBase *) lstGet(filterList, 0), "find 1st database");
        TEST_ASSIGN(db2, (DataBase *) lstGet(filterList, 1), "find 2nd database");
        TEST_ASSIGN(db3, (DataBase *) lstGet(filterList, 2), "find 3rd database");

        TEST_RESULT_INT(
            memcmp(
                &((RelFileNode){.spcNode = 1600, .dbNode = 20000, .relNode = 16384}), lstGet(db1->tables, 0), sizeof(RelFileNode)),
            0, "Check the 1st element");
        TEST_RESULT_INT(
            memcmp(
                &((RelFileNode){.spcNode = 1600, .dbNode = 20000, .relNode = 16386}), lstGet(db1->tables, 1), sizeof(RelFileNode)),
            0, "Check the 2nd element");
        TEST_RESULT_INT(
            memcmp(
                &((RelFileNode){.spcNode = 1601, .dbNode = 20000, .relNode = 16385}), lstGet(db1->tables, 2), sizeof(RelFileNode)),
            0, "Check the 3rd element");
        TEST_RESULT_INT(
            memcmp(
                &((RelFileNode){.spcNode = 1700, .dbNode = 20001, .relNode = 11000}), lstGet(db2->tables, 0), sizeof(RelFileNode)),
            0, "Check the 4th element");
        TEST_RESULT_INT(
            memcmp(
                &((RelFileNode){.spcNode = 1700, .dbNode = 20001, .relNode = 16386}), lstGet(db2->tables, 1), sizeof(RelFileNode)),
            0, "Check the 5th element");
        TEST_RESULT_INT(
            memcmp(
                &((RelFileNode){.spcNode = 1701, .dbNode = 20001, .relNode = 10000}), lstGet(db2->tables, 2), sizeof(RelFileNode)),
            0, "Check the 6th element");
        TEST_RESULT_BOOL(lstEmpty(db3->tables), true, "no tables in 3rd database");

        // This is necessary to cover the comparator
        RelFileNode *found = lstFind(db1->tables, &((RelFileNode){.spcNode = 1600, .dbNode = 20000, .relNode = 16384}));
        TEST_RESULT_INT(
            memcmp(
                &((RelFileNode){.spcNode = 1600, .dbNode = 20000, .relNode = 16384}), found, sizeof(RelFileNode)), 0, "test find");
        found = lstFind(db1->tables, &((RelFileNode){.spcNode = 1600, .dbNode = 0, .relNode = 16384}));
        TEST_RESULT_PTR(found, NULL, "wrong database");
        found = lstFind(db1->tables, &((RelFileNode){.spcNode = 1600, .dbNode = 40000, .relNode = 16384}));
        TEST_RESULT_PTR(found, NULL, "wrong database");
    }
}
