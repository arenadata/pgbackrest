#ifndef PGBACKREST_RECORDPROCRESSGPDB7_H
#define PGBACKREST_RECORDPROCRESSGPDB7_H

#include "build.auto.h"
#include "common/walFilter/postgresCommon.h"

FN_EXTERN WalInterface getWalInterfaceGPDB7(PgPageSize heapPageSize);

#endif // PGBACKREST_RECORDPROCRESSGPDB7_H
