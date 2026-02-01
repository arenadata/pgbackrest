/***********************************************************************************************************************************
Repository Put Command
***********************************************************************************************************************************/
#include "build.auto.h"

#include <unistd.h>

#include "command/repo/common.h"
#include "common/crypto/cipherBlock.h"
#include "common/debug.h"
#include "common/io/fdRead.h"
#include "common/io/io.h"
#include "common/log.h"
#include "common/compress/helper.h"
#include "common/memContext.h"
#include "config/config.h"
#include "storage/helper.h"

static String *
composeDestinationPath(const String *source)
{
    FUNCTION_LOG_BEGIN(logLevelDebug);
        FUNCTION_LOG_PARAM(STRING, source);
    FUNCTION_LOG_END();

    FUNCTION_LOG_RETURN(STRING, strNewFmt("%s", strZ(source)));
}

/***********************************************************************************************************************************
Write source IO to destination file
***********************************************************************************************************************************/
static void
storagePushProcess(const String *file, CompressType compressType, int compressLevel)
{
    FUNCTION_LOG_BEGIN(logLevelDebug);
        FUNCTION_LOG_PARAM(STRING, file);
        FUNCTION_LOG_PARAM(ENUM, compressType);
        FUNCTION_LOG_PARAM(INT, compressLevel);
    FUNCTION_LOG_END();

    // Ensure that the file exists and readable

    // Normalize source file path
    // Get current working dir
    char currentWorkDir[1024];
    THROW_ON_SYS_ERROR(getcwd(currentWorkDir, sizeof(currentWorkDir)) == NULL, FormatError, "unable to get cwd");

    // TODO: Use realpath() to normalize on posix. 

    String *sourcePath = strPathAbsolute(file, strNewZ(currentWorkDir));

    // Repository Path Formation

    String *destPath = composeDestinationPath(file);

    // Is path valid for repo?
    destPath = repoPathIsValid(destPath);

    MEM_CONTEXT_TEMP_BEGIN()
    {
        StorageWrite *const destination = storageNewWriteP(storageRepoWrite(), destPath);

        IoRead *const source = storageReadIo(storageNewReadP(storageLocal(), sourcePath));

        // Compression

        // See archive/push/push.c for compress example

        // Upload to Repository

        // Update manifest

        // Add encryption if needed
        if (!cfgOptionBool(cfgOptRaw))
        {
            const CipherType repoCipherType = cfgOptionStrId(cfgOptRepoCipherType);

            if (repoCipherType != cipherTypeNone)
            {
                // Check for a passphrase parameter
                const String *cipherPass = cfgOptionStrNull(cfgOptCipherPass);

                // If not passed as a parameter use the repo passphrase
                if (cipherPass == NULL)
                    cipherPass = cfgOptionStr(cfgOptRepoCipherPass);

                // Add encryption filter
                cipherBlockFilterGroupAdd(
                    ioWriteFilterGroup(storageWriteIo(destination)), repoCipherType, cipherModeEncrypt, cipherPass);
            }
        }

        // Open source and destination
        ioReadOpen(source);
        ioWriteOpen(storageWriteIo(destination));

        // Copy data from source to destination
        ioCopyP(source, storageWriteIo(destination));

        // Close the source and destination
        ioReadClose(source);
        ioWriteClose(storageWriteIo(destination));
    }
    MEM_CONTEXT_TEMP_END();

    FUNCTION_LOG_RETURN_VOID();
}

/**********************************************************************************************************************************/
FN_EXTERN void
cmdStoragePush(void)
{
    FUNCTION_LOG_VOID(logLevelDebug);

    MEM_CONTEXT_TEMP_BEGIN()
    {
        const StringList *params = cfgCommandParam();

        if (strLstSize(params) != 1)
            THROW(ParamInvalidError, "file parameter is required");

        String *filename = strLstGet(cfgCommandParam(), 0);

        LOG_INFO_FMT(
            "push file %s to the archive.",
                strZ(filename));

        storagePushProcess(filename, compressTypeEnum(cfgOptionStrId(cfgOptCompressType)),
                    cfgOptionInt(cfgOptCompressLevel));
    }
    MEM_CONTEXT_TEMP_END();

    FUNCTION_LOG_RETURN_VOID();
}
