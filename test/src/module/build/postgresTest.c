/***********************************************************************************************************************************
Test Build PostgreSQL Interface
***********************************************************************************************************************************/
#include "common/harnessStorage.h"

/***********************************************************************************************************************************
Test Run
***********************************************************************************************************************************/
static void
testRun(void)
{
    FUNCTION_HARNESS_VOID();

    // Create default storage object for testing
    Storage *storageTest = storagePosixNewP(TEST_PATH_STR, .write = true);

    // *****************************************************************************************************************************
    if (testBegin("bldPgParse() and bldPgRender()"))
    {
        // -------------------------------------------------------------------------------------------------------------------------
        TEST_TITLE("parse errors");

        HRN_STORAGE_PUT_Z(
            storageTest, "src/postgres/interface/version.vendor.h",
            "#define CATALOG_VERSION_NO\t1\n"
            "#define PG_CONTROL_VERSION  2\n"
            "\n"
            "typedef int64_t int64;\n"
            "\n"
            "typedef struct struct_type\n"
            "{\n"
            "    int field1;\n"
            "    int field2;\n"
            "} struct_type;\n"
            "\n"
            "typedef enum\n"
            "{\n"
            "    enum1 = 0,\n"
            "    enum2,\n"
            "} enum_type;\n");

        HRN_STORAGE_PUT_Z(
            storageTest, "src/postgres/interface/version.intern.h",
            "#define  PG_INTERFACE_CONTROL_IS(version)\n");

        TEST_ERROR(
            bldPgParse(storageTest), FormatError,
            "unable to find define -- are there extra spaces on '#define  PG_INTERFACE_CONTROL_IS(version)'");

        HRN_STORAGE_PUT_Z(
            storageTest, "src/postgres/interface/version.intern.h",
            "#define PG_INTERFACE_CONTROL_IS(version)\n"
            "#define PG_INTERFACE_CONTROL(version)\n");

        HRN_STORAGE_PUT_Z(
            storageTest, "src/build/postgres/postgres.yaml",
            "bogus: value\n");

        HRN_STORAGE_PUT_Z(
            storageTest, "src/build/postgres/gpdb.yaml",
            "version:\n"
            "  - 6:\n"
            "     pg_version: 9.4\n");

        TEST_ERROR(bldPgParse(storageTest), FormatError, "unknown postgres definition 'bogus'");

        HRN_STORAGE_PUT_Z(
            storageTest, "src/build/postgres/postgres.yaml",
            "version:\n"
            "  - 11:\n"
            "      bogus: value");

        TEST_ERROR(bldPgParse(storageTest), FormatError, "unknown postgres definition 'bogus'");

        HRN_STORAGE_PUT_Z(
            storageTest, "src/build/postgres/postgres.yaml",
            "version:\n"
            "  - 9.0\n");

        HRN_STORAGE_PUT_Z(
            storageTest, "src/build/postgres/gpdb.yaml",
            "bogus: value\n");

        TEST_ERROR(bldPgParse(storageTest), FormatError, "unknown GPDB definition 'bogus'");

        HRN_STORAGE_PUT_Z(
            storageTest, "src/build/postgres/postgres.yaml",
            "version:\n"
            "  - 9.0\n");

        HRN_STORAGE_PUT_Z(
            storageTest, "src/build/postgres/gpdb.yaml",
            "bogus: value\n");

        TEST_ERROR(bldPgParse(storageTest), FormatError, "unknown GPDB definition 'bogus'");

        HRN_STORAGE_PUT_Z(
            storageTest, "src/build/postgres/postgres.yaml",
            "version:\n"
            "  - 9.0\n");

        HRN_STORAGE_PUT_Z(
            storageTest, "src/build/postgres/gpdb.yaml",
            "version:\n"
            "  - 6:\n"
            "     bogus: 9.4\n");

        TEST_ERROR(bldPgParse(storageTest), FormatError, "unknown GPDB definition 'bogus'");

        HRN_STORAGE_PUT_Z(
            storageTest, "src/build/postgres/postgres.yaml",
            "version:\n"
            "  - 9.0\n");

        HRN_STORAGE_PUT_Z(
            storageTest, "src/build/postgres/gpdb.yaml",
            "version:\n"
            "  - 6\n");

        TEST_ERROR(bldPgParse(storageTest), FormatError, "invalid GPDB version '6'");
        // -------------------------------------------------------------------------------------------------------------------------
        TEST_TITLE("parse and render postgres");

        HRN_STORAGE_PUT_Z(
            storageTest, "src/build/postgres/postgres.yaml",
            "version:\n"
            "  - 9.0\n"
            "  - 11:\n"
            "      release: false\n");

        HRN_STORAGE_PUT_Z(
            storageTest, "src/build/postgres/gpdb.yaml",
            "version:\n"
            "  - 6:\n"
            "     pg_version: 9.4\n"
            "  - 7:\n"
            "     pg_version: 12\n");
        TEST_RESULT_VOID(bldPgRender(storageTest, bldPgParse(storageTest)), "parse and render");

        // -------------------------------------------------------------------------------------------------------------------------
        TEST_TITLE("check interface.auto.c.inc");

        TEST_STORAGE_GET(
            storageTest,
            "postgres/interface.auto.c.inc",
            COMMENT_BLOCK_BEGIN "\n"
            "PostgreSQL Interface\n"
            "\n"
            "Automatically generated by 'make build-postgres' -- do not modify directly.\n"
            COMMENT_BLOCK_END "\n"
            "\n"
            COMMENT_BLOCK_BEGIN "\n"
            "PostgreSQL 11 interface\n"
            COMMENT_BLOCK_END "\n"
            "#define PG_VERSION                                                  PG_VERSION_11\n"
            "\n"
            "#define enum1                                                       enum1_11\n"
            "#define enum2                                                       enum2_11\n"
            "#define enum_type                                                   enum_type_11\n"
            "#define int64                                                       int64_11\n"
            "#define struct_type                                                 struct_type_11\n"
            "\n"
            "#define CATALOG_VERSION_NO_MAX\n"
            "\n"
            "#include \"postgres/interface/version.intern.h\"\n"
            "\n"
            "PG_INTERFACE_CONTROL_IS(11);\n"
            "PG_INTERFACE_CONTROL(11);\n"
            "\n"
            "#undef enum1\n"
            "#undef enum2\n"
            "#undef enum_type\n"
            "#undef int64\n"
            "#undef struct_type\n"
            "\n"
            "#undef CATALOG_VERSION_NO\n"
            "#undef CATALOG_VERSION_NO_MAX\n"
            "#undef PG_CONTROL_VERSION\n"
            "#undef PG_VERSION\n"
            "\n"
            "#undef PG_INTERFACE_CONTROL_IS\n"
            "#undef PG_INTERFACE_CONTROL\n"
            "\n"
            COMMENT_BLOCK_BEGIN "\n"
            "PostgreSQL 9.0 interface\n"
            COMMENT_BLOCK_END "\n"
            "#define PG_VERSION                                                  PG_VERSION_90\n"
            "\n"
            "#define enum1                                                       enum1_90\n"
            "#define enum2                                                       enum2_90\n"
            "#define enum_type                                                   enum_type_90\n"
            "#define int64                                                       int64_90\n"
            "#define struct_type                                                 struct_type_90\n"
            "\n"
            "#include \"postgres/interface/version.intern.h\"\n"
            "\n"
            "PG_INTERFACE_CONTROL_IS(90);\n"
            "PG_INTERFACE_CONTROL(90);\n"
            "\n"
            "#undef enum1\n"
            "#undef enum2\n"
            "#undef enum_type\n"
            "#undef int64\n"
            "#undef struct_type\n"
            "\n"
            "#undef CATALOG_VERSION_NO\n"
            "#undef CATALOG_VERSION_NO_MAX\n"
            "#undef PG_CONTROL_VERSION\n"
            "#undef PG_VERSION\n"
            "\n"
            "#undef PG_INTERFACE_CONTROL_IS\n"
            "#undef PG_INTERFACE_CONTROL\n"
            "\n"
            COMMENT_BLOCK_BEGIN "\n"
            "PostgreSQL interface struct\n"
            COMMENT_BLOCK_END "\n"
            "static const PgInterface pgInterface[] =\n"
            "{\n"
            "    {\n"
            "        .version = PG_VERSION_11,\n"
            "\n"
            "        .controlIs = pgInterfaceControlIs11,\n"
            "        .control = pgInterfaceControl11,\n"
            "    },\n"
            "    {\n"
            "        .version = PG_VERSION_90,\n"
            "\n"
            "        .controlIs = pgInterfaceControlIs90,\n"
            "        .control = pgInterfaceControl90,\n"
            "    },\n"
            "};\n"
            "\n"
            COMMENT_BLOCK_BEGIN "\n"
            "GPDB 7 interface\n"
            COMMENT_BLOCK_END "\n"
            "#define PG_VERSION                                                  PG_VERSION_12\n"
            "#define GPDB_VERSION                                                GPDB_VERSION_7\n"
            "\n"
            "#define enum1                                                       enum1_GPDB7\n"
            "#define enum2                                                       enum2_GPDB7\n"
            "#define enum_type                                                   enum_type_GPDB7\n"
            "#define int64                                                       int64_GPDB7\n"
            "#define struct_type                                                 struct_type_GPDB7\n"
            "\n"
            "#include \"postgres/interface/version.intern.h\"\n"
            "\n"
            "PG_INTERFACE_CONTROL_IS(GPDB7);\n"
            "PG_INTERFACE_CONTROL(GPDB7);\n"
            "\n"
            "#undef enum1\n"
            "#undef enum2\n"
            "#undef enum_type\n"
            "#undef int64\n"
            "#undef struct_type\n"
            "\n"
            "#undef CATALOG_VERSION_NO\n"
            "#undef CATALOG_VERSION_NO_MAX\n"
            "#undef PG_CONTROL_VERSION\n"
            "#undef PG_VERSION\n"
            "#undef GPDB_VERSION\n"
            "\n"
            "#undef PG_INTERFACE_CONTROL_IS\n"
            "#undef PG_INTERFACE_CONTROL\n"
            "\n"
            COMMENT_BLOCK_BEGIN "\n"
            "GPDB 6 interface\n"
            COMMENT_BLOCK_END "\n"
            "#define PG_VERSION                                                  PG_VERSION_94\n"
            "#define GPDB_VERSION                                                GPDB_VERSION_6\n"
            "\n"
            "#define enum1                                                       enum1_GPDB6\n"
            "#define enum2                                                       enum2_GPDB6\n"
            "#define enum_type                                                   enum_type_GPDB6\n"
            "#define int64                                                       int64_GPDB6\n"
            "#define struct_type                                                 struct_type_GPDB6\n"
            "\n"
            "#include \"postgres/interface/version.intern.h\"\n"
            "\n"
            "PG_INTERFACE_CONTROL_IS(GPDB6);\n"
            "PG_INTERFACE_CONTROL(GPDB6);\n"
            "\n"
            "#undef enum1\n"
            "#undef enum2\n"
            "#undef enum_type\n"
            "#undef int64\n"
            "#undef struct_type\n"
            "\n"
            "#undef CATALOG_VERSION_NO\n"
            "#undef CATALOG_VERSION_NO_MAX\n"
            "#undef PG_CONTROL_VERSION\n"
            "#undef PG_VERSION\n"
            "#undef GPDB_VERSION\n"
            "\n"
            "#undef PG_INTERFACE_CONTROL_IS\n"
            "#undef PG_INTERFACE_CONTROL\n"
            "\n"
            COMMENT_BLOCK_BEGIN "\n"
            "GPDB interface struct\n"
            COMMENT_BLOCK_END "\n"
            "static const PgInterface gpdbInterface[] =\n"
            "{\n"
            "    {\n"
            "        .version = GPDB_VERSION_7,\n"
            "\n"
            "        .controlIs = pgInterfaceControlIsGPDB7,\n"
            "        .control = pgInterfaceControlGPDB7,\n"
            "    },\n"
            "    {\n"
            "        .version = GPDB_VERSION_6,\n"
            "\n"
            "        .controlIs = pgInterfaceControlIsGPDB6,\n"
            "        .control = pgInterfaceControlGPDB6,\n"
            "    },\n"
            "};\n");
    }

    FUNCTION_HARNESS_RETURN_VOID();
}
