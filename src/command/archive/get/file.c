/***********************************************************************************************************************************
Archive Get File
***********************************************************************************************************************************/
#include "build.auto.h"

#include <fcntl.h>
#include <unistd.h>
#include "command/archive/common.h"
#include "command/archive/get/file.h"
#include "command/control/common.h"
#include "common/compress/helper.h"
#include "common/crypto/cipherBlock.h"
#include "common/debug.h"
#include "common/io/bufferWrite.h"
#include "common/io/fdRead.h"
#include "common/io/filter/group.h"
#include "common/io/io.h"
#include "common/log.h"
#include "common/walFilter/walFilter.h"
#include "config/config.h"
#include "info/infoArchive.h"
#include "postgres/interface.h"
#include "storage/helper.h"

/**********************************************************************************************************************************/
FN_EXTERN ArchiveGetFileResult
archiveGetFile(
    const Storage *const storage, const String *const request, const List *const actualList, const String *const walDestination)
{
    FUNCTION_LOG_BEGIN(logLevelDebug);
        FUNCTION_LOG_PARAM(STORAGE, storage);
        FUNCTION_LOG_PARAM(STRING, request);
        FUNCTION_LOG_PARAM(LIST, actualList);
        FUNCTION_LOG_PARAM(STRING, walDestination);
    FUNCTION_LOG_END();

    FUNCTION_AUDIT_STRUCT();

    ASSERT(request != NULL);
    ASSERT(actualList != NULL && !lstEmpty(actualList));
    ASSERT(walDestination != NULL);

    ArchiveGetFileResult result = {.warnList = strLstNew()};

    // Check all files in the actual list and return as soon as one is copied
    bool copied = false;

    RelFileNode *filter_list = NULL;
    size_t filter_list_len = 0;
    unsigned int pgVersion = 0;
    if(cfgOptionTest(cfgOptFilter))
    {
        const String *filter_path = cfgOptionStrNull(cfgOptFilter);
        if (strZ(filter_path)[0] != '/')
        {
            THROW(AssertError, "The path to the filter is not absolute");
        }

        const PgControl pgControl = pgControlFromFile(storagePg(), cfgOptionStrNull(cfgOptPgVersionForce));
        pgVersion = pgControl.version;
        MEM_CONTEXT_TEMP_BEGIN()
        {
            const Storage *local_storage = storageLocal();
            StorageRead *storageRead = storageNewReadP(local_storage, filter_path);
            Buffer *jsonFile = storageGetP(storageRead);
            JsonRead *jsonRead = jsonReadNew(strNewBuf(jsonFile));

            MemContext *save_memory_context = memContextCurrent();
            memContextSwitchBack();
            build_filter_list(jsonRead, &filter_list, &filter_list_len);
            memContextSwitch(save_memory_context);
        }
        MEM_CONTEXT_TEMP_END();
    }
    for (unsigned int actualIdx = 0; actualIdx < lstSize(actualList); actualIdx++)
    {
        const ArchiveGetFile *const actual = lstGet(actualList, actualIdx);

        // Is the file compressible during the copy?
        bool compressible = true;

        TRY_BEGIN()
        {
            MEM_CONTEXT_TEMP_BEGIN()
            {
                StorageWrite *const destination = storageNewWriteP(
                    storage, walDestination, .noCreatePath = true, .noSyncFile = true, .noSyncPath = true, .noAtomic = true);

                // If there is a cipher then add the decrypt filter
                if (actual->cipherType != cipherTypeNone)
                {
                    ioFilterGroupAdd(
                        ioWriteFilterGroup(storageWriteIo(destination)),
                        cipherBlockNewP(cipherModeDecrypt, actual->cipherType, BUFSTR(actual->cipherPassArchive)));
                    compressible = false;
                }

                // If file is compressed then add the decompression filter
                CompressType compressType = compressTypeFromName(actual->file);

                if (compressType != compressTypeNone)
                {
                    ioFilterGroupAdd(ioWriteFilterGroup(storageWriteIo(destination)), decompressFilterP(compressType));
                    compressible = false;
                }

                if (walIsSegment(request) && cfgOptionTest(cfgOptFilter))
                {
                    ioFilterGroupAdd(ioWriteFilterGroup(storageWriteIo(destination)),
                                     walFilterNew(pgVersion, cfgOptionStrId(cfgOptFork), filter_list, filter_list_len));
                }
                // Copy the file
                storageCopyP(
                    storageNewReadP(
                        storageRepoIdx(actual->repoIdx), strNewFmt(STORAGE_REPO_ARCHIVE "/%s", strZ(actual->file)),
                        .compressible = compressible),
                    destination);
            }
            MEM_CONTEXT_TEMP_END();

            // File was successfully copied
            result.actualIdx = actualIdx;
            copied = true;
        }
        // Log errors as warnings and continue
        CATCH_ANY()
        {
            strLstAddFmt(
                result.warnList, "%s: %s [%s] %s", cfgOptionGroupName(cfgOptGrpRepo, actual->repoIdx), strZ(actual->file),
                errorTypeName(errorType()), errorMessage());
        }
        TRY_END();

        // Stop on success
        if (copied)
            break;
    }

    // If no file was successfully copied then error
    if (!copied)
    {
        ASSERT(!strLstEmpty(result.warnList));
        THROW_FMT(FileReadError, "unable to get %s:\n%s", strZ(request), strZ(strLstJoin(result.warnList, "\n")));
    }

    FUNCTION_LOG_RETURN_STRUCT(result);
}
