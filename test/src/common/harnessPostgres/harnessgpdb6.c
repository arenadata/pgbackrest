/***********************************************************************************************************************************
Harness for PostgreSQL Interface (see PG_VERSION for version)
***********************************************************************************************************************************/
#include "build.auto.h"

#define PG_VERSION                                                  PG_VERSION_94
#define GPDB_VERSION                                                GPDB_VERSION_6

#include "common/harnessPostgres/harnessVersion.intern.h"

HRN_PG_INTERFACE(GPDB6);
