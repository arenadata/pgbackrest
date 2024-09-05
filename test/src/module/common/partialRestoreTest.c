/***********************************************************************************************************************************
Test Run
***********************************************************************************************************************************/
#include "common/type/json.h"
#include "postgres/interface/static.vendor.h"

static void
testRun(void)
{
    if (testBegin("parse valid json"))
    {
        const String *jsonstr = STRDEF("["
                                       "  {"
                                       "    \"dbOid\": 20002,"
                                       "    \"tables\": [],"
                                       "    \"dbName\": \"db3\""
                                       "  },"
                                       "  {"
                                       "    \"dbName\": \"db2\","
                                       "    \"dbOid\": 20001,"
                                       "    \"tables\": ["
                                       "      {"
                                       "        \"tablespace\": 1700,"
                                       "        \"relfilenode\": 16386"
                                       "      },"
                                       "      {"
                                       "        \"relfilenode\": 11000,"
                                       "        \"tablefqn\": \"public.t3\","
                                       "        \"tablespace\": 1700"
                                       "      },"
                                       "      {"
                                       "        \"relfilenode\": 10000,"
                                       "        \"tablespace\": 1701"
                                       "      }"
                                       "    ]"
                                       "  },"
                                       "  {"
                                       "    \"dbOid\": 20000,"
                                       "    \"tables\": ["
                                       "      {"
                                       "        \"tablespace\": 1600,"
                                       "        \"relfilenode\": 16384"
                                       "      },"
                                       "      {"
                                       "        \"tablespace\": 1601,"
                                       "        \"relfilenode\": 16385"
                                       "      },"
                                       "      {"
                                       "        \"tablespace\": 1600,"
                                       "        \"relfilenode\": 16386"
                                       "      }"
                                       "    ]"
                                       "  }"
                                       "]");
        JsonRead *json = jsonReadNew(jsonstr);
        List *filterList = buildFilterList(json);
        TEST_RESULT_UINT(lstSize(filterList), 3, "database count");

        DataBase *db1 = lstGet(filterList, 0);
        DataBase *db2 = lstGet(filterList, 1);
        DataBase *db3 = lstGet(filterList, 2);

        TEST_RESULT_UINT(db1->dbOid, 20000, "dbOid of 1st database");
        TEST_RESULT_UINT(db2->dbOid, 20001, "dbOid of 2nd database");
        TEST_RESULT_UINT(db3->dbOid, 20002, "dbOid of 3rd database");

        TEST_RESULT_UINT(lstSize(db1->tables), 3, "dbOid of 1st database");
        TEST_RESULT_UINT(lstSize(db2->tables), 3, "dbOid of 2nd database");
        TEST_RESULT_UINT(lstSize(db3->tables), 0, "dbOid of 3rd database");

        TEST_RESULT_INT(
            memcmp(&((Table){.spcNode = 1600, .relNode = 16384}), lstGet(db1->tables, 0), sizeof(Table)), 0, "1st table of 1st DB");
        TEST_RESULT_INT(
            memcmp(&((Table){.spcNode = 1600, .relNode = 16386}), lstGet(db1->tables, 1), sizeof(Table)), 0, "2nd table of 1st DB");
        TEST_RESULT_INT(
            memcmp(&((Table){.spcNode = 1601, .relNode = 16385}), lstGet(db1->tables, 2), sizeof(Table)), 0, "3rd table of 1st DB");
        TEST_RESULT_INT(
            memcmp(&((Table){.spcNode = 1700, .relNode = 11000}), lstGet(db2->tables, 0), sizeof(Table)), 0, "1st table of 2nd DB");
        TEST_RESULT_INT(
            memcmp(&((Table){.spcNode = 1700, .relNode = 16386}), lstGet(db2->tables, 1), sizeof(Table)), 0, "2nd table of 2nd DB");
        TEST_RESULT_INT(
            memcmp(&((Table){.spcNode = 1701, .relNode = 10000}), lstGet(db2->tables, 2), sizeof(Table)), 0, "3th table of 2nd DB");

        Table *found = lstFind(db1->tables, &((Table){.spcNode = 1600, .relNode = 16384}));
        TEST_RESULT_INT(memcmp(&((Table){.spcNode = 1600, .relNode = 16384}), found, sizeof(Table)), 0, "test find");
    }

    if (testBegin("parse invalid json"))
    {
        TEST_TITLE("missing dbOid");
        JsonRead *json = jsonReadNew(STRDEF("[{}]"));
        TEST_ERROR(buildFilterList(json), FormatError, "dbOid field of table is missing");

        TEST_TITLE("missing tables");
        json = jsonReadNew(STRDEF("[{\"dbOid\": 10}]"));
        TEST_ERROR(buildFilterList(json), FormatError, "tables field of table is missing");

        TEST_TITLE("missing tablespace");
        json = jsonReadNew(STRDEF("[{\"dbOid\": 10, \"tables\": [{}]}]"));
        TEST_ERROR(buildFilterList(json), FormatError, "tablespace field of table is missing");

        TEST_TITLE("relfilenode tablespace");
        json = jsonReadNew(STRDEF("[{\"dbOid\": 10, \"tables\": [{\"tablespace\": 11}]}]"));
        TEST_ERROR(buildFilterList(json), FormatError, "relfilenode field of table is missing");
    }
}
