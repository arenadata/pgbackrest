#ifndef PGBACKREST_TEST_RECORD_PROCESS_H
#define PGBACKREST_TEST_RECORD_PROCESS_H

#include "postgres_common.h"

FN_EXTERN bool get_relfilenode(XLogRecord *record, RelFileNode *node);
#endif // PGBACKREST_TEST_RECORD_PROCESS_H
