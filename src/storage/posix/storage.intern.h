/***********************************************************************************************************************************
Posix Storage Internal
***********************************************************************************************************************************/
#ifndef STORAGE_POSIX_STORAGE_INTERN_H
#define STORAGE_POSIX_STORAGE_INTERN_H

#include <dirent.h>

#include "storage/posix/storage.h"

/***********************************************************************************************************************************
Object type
***********************************************************************************************************************************/
typedef struct StoragePosix StoragePosix;

/***********************************************************************************************************************************
Constructors
***********************************************************************************************************************************/
FN_EXTERN Storage *storagePosixNewInternal(
    StringId type, const String *path, mode_t modeFile, mode_t modePath, bool write,
    StoragePathExpressionCallback pathExpressionFunction, bool pathSync);

/***********************************************************************************************************************************
Thin opendir/readdir/closedir wrappers exposed so tests can shim directory iteration
***********************************************************************************************************************************/
FN_EXTERN DIR *storagePosixOpendir(const char *path);
FN_EXTERN struct dirent *storagePosixReaddir(DIR *dir);
FN_EXTERN int storagePosixClosedir(DIR *dir);

/***********************************************************************************************************************************
Macros for function logging
***********************************************************************************************************************************/
#define FUNCTION_LOG_STORAGE_POSIX_TYPE                                                                                            \
    StoragePosix *
#define FUNCTION_LOG_STORAGE_POSIX_FORMAT(value, buffer, bufferSize)                                                               \
    objNameToLog(value, "StoragePosix *", buffer, bufferSize)

#endif
