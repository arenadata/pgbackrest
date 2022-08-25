/***********************************************************************************************************************************
Common Functions and Definitions for Backup and Expire Commands
***********************************************************************************************************************************/
#ifndef COMMAND_BACKUP_COMMON_H
#define COMMAND_BACKUP_COMMON_H

#include <stdbool.h>
#include <time.h>

#include "common/type/string.h"
#include "info/infoBackup.h"

/***********************************************************************************************************************************
Backup constants
***********************************************************************************************************************************/
#define BACKUP_PATH_HISTORY                                         "backup.history"

// Date and time must be in the %Y%m%d-%H%M%S format, for example 20220901-193409
#define DATE_TIME_REGEX                                             "[0-9]{8}\\-[0-9]{6}"
#define DATE_TIME_LEN                                               (8 + 1 + 6)

/***********************************************************************************************************************************
Functions
***********************************************************************************************************************************/
// Format a backup label from a type and timestamp with an optional prior label
String *backupLabelFormat(BackupType type, const String *backupLabelPrior, time_t timestamp);

// Returns an anchored regex string for filtering backups based on the type (at least one type is required to be true)
typedef struct BackupRegExpParam
{
    bool full;
    bool differential;
    bool incremental;
    bool noAnchorEnd;
} BackupRegExpParam;

#define backupRegExpP(...)                                                                                                         \
    backupRegExp((BackupRegExpParam){__VA_ARGS__})

String *backupRegExp(BackupRegExpParam param);

// Create a symlink to the specified backup (if symlinks are supported)
void backupLinkLatest(const String *backupLabel, unsigned int repoIdx);

#endif
