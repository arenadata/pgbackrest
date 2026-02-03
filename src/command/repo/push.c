/***********************************************************************************************************************************
Repository Put Command
***********************************************************************************************************************************/
#include "build.auto.h"

#include <unistd.h>

#include "command/repo/common.h"
#include "common/crypto/cipherBlock.h"
#include "common/debug.h"
#include "common/io/fdRead.h"
#include "common/io/filter/size.h"
#include "common/io/io.h"
#include "common/log.h"
#include "common/compress/helper.h"
#include "common/memContext.h"
#include "common/type/string.h"
#include "config/config.h"
#include "storage/helper.h"
#include "info/manifest.h"


static String *
composeDestinationPath(const String *stanza, const String *backupLabel, const String *fileName)
{
    FUNCTION_LOG_BEGIN(logLevelDebug);
        FUNCTION_LOG_PARAM(STRING, stanza);
        FUNCTION_LOG_PARAM(STRING, backupLabel);
        FUNCTION_LOG_PARAM(STRING, fileName);
    FUNCTION_LOG_END();

    String *const result = strNewFmt("%s/%s/%s", strZ(stanza), strZ(backupLabel), strZ(fileName));

    FUNCTION_LOG_RETURN(STRING, result);
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
    MEM_CONTEXT_TEMP_BEGIN()
    {

        // Normalize source file path
        // Get current working dir
        char currentWorkDir[1024];
        THROW_ON_SYS_ERROR(getcwd(currentWorkDir, sizeof(currentWorkDir)) == NULL, FormatError, "unable to get cwd");

        // TODO: Use realpath() to normalize on posix. 

        String *sourcePath = strPathAbsolute(file, strNewZ(currentWorkDir));

        // Repository Path Formation

        const String *stanza = cfgOptionStr(cfgOptStanza); 
        const String *backupLabel = cfgOptionStr(cfgOptSet);

        String *destFilename = strFileName(file);
        String *destPath = composeDestinationPath(stanza, backupLabel, destFilename);

        // Is path valid for repo?
        destPath = repoPathIsValid(destPath);

        bool repoChecksum = false;
        const Storage *storage = storageRepoWrite();
        const StorageWrite *const destination = storageNewWriteP(storage, destPath);

        IoFilterGroup *const filterGroup = ioWriteFilterGroup(storageWriteIo(destination));

        CipherType cipherType = cfgOptionStrId(cfgOptRepoCipherType);
        const String *cipherPass = cfgOptionStrNull(cfgOptRepoCipherPass);

        const String *manifestFileName = strNewFmt(STORAGE_REPO_BACKUP "/%s/" BACKUP_MANIFEST_FILE, strZ(backupLabel));
        Manifest *manifest = manifestLoadFile(
                            storage, manifestFileName,
                            cipherType, cipherPass);

        // Add SHA1 filter
        ioFilterGroupAdd(filterGroup, cryptoHashNew(hashTypeSha1));

        // Add compression
        if (compressType != compressTypeNone)
        {
            ioFilterGroupAdd(
                ioWriteFilterGroup(storageWriteIo(destination)), 
                compressFilterP(compressType, cfgOptionInt(cfgOptCompressLevel)));

            repoChecksum = true;
        }

        // Add encryption filter if required
        if (manifestCipherSubPass(manifest) != NULL)
        {
            const CipherType repoCipherType = cfgOptionStrId(cfgOptRepoCipherType);

            if (repoCipherType != cipherTypeNone) 
            {
                // Check for a passphrase parameter
                const String *cipherPass = cfgOptionStrNull(cfgOptCipherPass);

                // If not passed as a parameter use the repo passphrase
                if (cipherPass == NULL)
                    cipherPass = cfgOptionStr(cfgOptRepoCipherPass);

                ioFilterGroupAdd(
                    ioWriteFilterGroup(storageWriteIo(destination)),
                    cipherBlockNewP(
                        cipherModeEncrypt, repoCipherType, BUFSTR(manifestCipherSubPass(manifest))
                    )
                );
                repoChecksum = true;
            }            
        }

        // Add size filter last to calculate repo size
        ioFilterGroupAdd(filterGroup, ioSizeNew());

        IoRead *const source = storageReadIo(storageNewReadP(storageLocal(), sourcePath));

        // Open source and destination
        ioReadOpen(source);
        ioWriteOpen(storageWriteIo(destination));

        // Copy data from source to destination
        ioCopyP(source, storageWriteIo(destination));

        // Close the source and destination
        ioReadClose(source);
        ioWriteClose(storageWriteIo(destination));

        // Use base path to set ownership and mode
        const ManifestPath *const basePath = manifestPathFind(manifest, MANIFEST_TARGET_PGDATA_STR);

        // Add to manifest
        uint64_t size = pckReadU64P(ioFilterGroupResultP(filterGroup, SIZE_FILTER_TYPE));
        ManifestFile customFile =
        {
            .name = destFilename,
            .mode = basePath->mode & (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH),
            .user = basePath->user,
            .group = basePath->group,
            .size = size,
            .sizeOriginal = size,
            .sizeRepo = size,
            .timestamp = time(NULL),
            .checksumSha1 = bufPtr(pckReadBinP(ioFilterGroupResultP(filterGroup, CRYPTO_HASH_FILTER_TYPE, .idx = 0))),
        };

        if (repoChecksum)
        {
            PackRead * packRead = ioFilterGroupResultP(filterGroup, CRYPTO_HASH_FILTER_TYPE, .idx = 1);
            ASSERT(packRead != NULL);
            customFile.checksumRepoSha1 = bufPtr(pckReadBinP(packRead));
        }

        manifestCustomFileAdd(manifest, &customFile);

        // Save manifest
        IoWrite *const manifestWrite = storageWriteIo(
                storageNewWriteP(
                    storageRepoWrite(),
                    manifestFileName           
                    ));

        manifestSave(manifest, manifestWrite);
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

        CompressType compressType = compressTypeNone;
        if (cfgOptionValid(cfgOptCompress))
        {
            compressType = compressTypeEnum(cfgOptionStrId(cfgOptCompressType));            
        }
        storagePushProcess(filename, compressType, 
            cfgOptionInt(cfgOptCompressLevel));
    }
    MEM_CONTEXT_TEMP_END();

    FUNCTION_LOG_RETURN_VOID();
}
