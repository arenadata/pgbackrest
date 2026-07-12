/***********************************************************************************************************************************
Repository Get Command
***********************************************************************************************************************************/
#include "build.auto.h"

#include <unistd.h>

#include "command/repo/common.h"
#include "common/crypto/cipherBlock.h"
#include "common/debug.h"
#include "common/io/fdWrite.h"
#include "common/io/io.h"
#include "common/log.h"
#include "common/memContext.h"
#include "config/config.h"
#include "storage/helper.h"

#include "info/infoArchive.h"
#include "info/infoBackup.h"

/***********************************************************************************************************************************
Write source file to destination IO
***********************************************************************************************************************************/

static bool isStoragePathArchive(const StringList *const filePathSplitLst)
{
    return strEq(strLstGet(filePathSplitLst, 0), STORAGE_REPO_ARCHIVE_STR) ||
                            strEq(strLstGet(filePathSplitLst, 0), STORAGE_PATH_ARCHIVE_STR);
}

static bool isStoragePathBackup(const StringList *const filePathSplitLst)
{
    return strEq(strLstGet(filePathSplitLst, 0), STORAGE_REPO_BACKUP_STR) ||
                            strEq(strLstGet(filePathSplitLst, 0), STORAGE_PATH_BACKUP_STR);
}

static int
storageGetProcess(IoWrite *const destination)
{
    FUNCTION_LOG_BEGIN(logLevelDebug);
        FUNCTION_LOG_PARAM(IO_READ, destination);
    FUNCTION_LOG_END();

    // Get source file
    if (strLstSize(cfgCommandParam()) != 1)
        THROW(ParamRequiredError, "source file required");

    const String *file = strLstGet(cfgCommandParam(), 0);
    CompressType compressType = compressTypeNone;
    bool readCompressTypeFromManifest = cfgOptionBool(cfgOptDecompress) && cfgOptionSource(cfgOptCompressType) != cfgSourceParam;
    if (readCompressTypeFromManifest)
    {
        // Stanza and set are mandatory in with decompress
        if (cfgOptionStrNull(cfgOptStanza) == NULL)
        {
            THROW(ParamRequiredError, "stanza required");
        }

        if (cfgOptionStrNull(cfgOptSet) == NULL)
        {
            THROW(ParamRequiredError, "set required");
        }
    }

    // Assume the file is missing
    int result = 1;

    MEM_CONTEXT_TEMP_BEGIN()
    {
        // Is path valid for repo?
        file = repoPathIsValid(file);

        // Create new file read
        IoRead *const source = storageReadIo(
            storageNewReadP(storageRepo(), file, .ignoreMissing = cfgOptionBool(cfgOptIgnoreMissing)));

        // Add decryption if needed
        if (!cfgOptionBool(cfgOptRaw))
        {
            const CipherType repoCipherType = cfgOptionStrId(cfgOptRepoCipherType);

            if (repoCipherType != cipherTypeNone)
            {
                // Check for a passphrase parameter
                const String *cipherPass = cfgOptionStrNull(cfgOptCipherPass);
                const String *archiveCipherPass = NULL; // pass which archive is encrypted
                const String *backupCipherPass = NULL;  // pass which backup is encrypted
                const String *fileCipherPass = cipherPass;    // pass which will be used to decrypt the file

                // If not passed as a parameter then determine the passphrase using the following pattern:
                //
                // REPO / (repo passphrase)
                //      / archive / (repo passphrase)
                //      / archive / stanza / (archive passphrase)
                //      / backup  / (repo passphrase)
                //      / backup  / stanza / (backup passphrase)
                //      / backup  / stanza / set / (manifest passphrase)
                //      / backup  / stanza / backup.history / (backup passphrase)
                //
                // Nothing should be stored at the top level of the repo except the backup/archive paths. The backup/archive paths
                // should contain only stanza paths.
                // -----------------------------------------------------------------------------------------------------------------
                if (cipherPass == NULL)
                {
                    const StringList *const filePathSplitLst = strLstNewSplit(file, FSLASH_STR);

                    // At a minimum the path must contain archive/backup, a stanza, and a file
                    if (strLstSize(filePathSplitLst) > 2)
                    {
                        bool usedTemplateInFile = strEq(strLstGet(filePathSplitLst, 0), STORAGE_REPO_ARCHIVE_STR) ||
                             strEq(strLstGet(filePathSplitLst, 0), STORAGE_REPO_BACKUP_STR);

                        const String *const stanza = usedTemplateInFile ? cfgOptionStr(cfgOptStanza) : strLstGet(filePathSplitLst, 1);
                        const char *backupSet = strZ(strLstGet(filePathSplitLst, usedTemplateInFile ? 1 : 2));

                        // If stanza option is specified then it must match the given file path
                        if (cfgOptionStrNull(cfgOptStanza) != NULL && !strEq(stanza, cfgOptionStr(cfgOptStanza)))
                        {
                            THROW_FMT(
                                OptionInvalidValueError, "stanza name '%s' given in option doesn't match the given path",
                                strZ(cfgOptionDisplay(cfgOptStanza)));
                        }

                        // Archive path
                        if (isStoragePathArchive(filePathSplitLst))
                        {
                            archiveCipherPass = cfgOptionStr(cfgOptRepoCipherPass);

                            // Find the archive passphrase
                            if (!strEndsWithZ(file, INFO_ARCHIVE_FILE) && !strEndsWithZ(file, INFO_ARCHIVE_FILE INFO_COPY_EXT))
                            {
                                const InfoArchive *const info = infoArchiveLoadFile(
                                    storageRepo(), strNewFmt(STORAGE_PATH_ARCHIVE "/%s/%s", strZ(stanza), INFO_ARCHIVE_FILE),
                                    repoCipherType, cipherPass);                                
                                archiveCipherPass = infoArchiveCipherPass(info);
                            }
                            fileCipherPass = archiveCipherPass;
                        }

                        // Backup path
                        if (readCompressTypeFromManifest || isStoragePathBackup(filePathSplitLst))
                        {
                            backupCipherPass = cfgOptionStr(cfgOptRepoCipherPass);

                            if (!strEndsWithZ(file, INFO_BACKUP_FILE) && !strEndsWithZ(file, INFO_BACKUP_FILE INFO_COPY_EXT))
                            {
                                // Find the backup passphrase
                                const InfoBackup *const info = infoBackupLoadFile(
                                    storageRepo(), strNewFmt(STORAGE_PATH_BACKUP "/%s/%s", strZ(stanza), INFO_BACKUP_FILE),
                                    repoCipherType, cipherPass);
                                backupCipherPass = infoBackupCipherPass(info);

                                // Find the manifest passphrase
                                if (!strEq(strLstGet(filePathSplitLst, 2), STRDEF(BACKUP_PATH_HISTORY)) &&
                                    !strEndsWithZ(file, BACKUP_MANIFEST_FILE) &&
                                    !strEndsWithZ(file, BACKUP_MANIFEST_FILE INFO_COPY_EXT))
                                {
                                    if (fileCipherPass != NULL)
                                    {
                                        // Processing archive, but backup manifest will be used for decompress
                                        // Use provided set to read the manifest
                                        backupSet = cfgOptionStrNull(cfgOptSet);
                                    }
                                    
                                    const Manifest *const manifest = manifestLoadFile(
                                        storageRepo(),
                                        strNewFmt(
                                            STORAGE_PATH_BACKUP "/%s/%s/%s", strZ(stanza), backupSet,
                                            BACKUP_MANIFEST_FILE),
                                        repoCipherType, backupCipherPass);

                                    // We are processing backup, file encoded with pass in manifest
                                    if (fileCipherPass == NULL)
                                        fileCipherPass = manifestCipherSubPass(manifest);

                                    compressType = manifestData(manifest)->backupOptionCompressType;
                                }
                            }
                        }
                    }
                }

                // Error when unable to determine cipher passphrase
                if (cipherPass == NULL)
                    THROW_FMT(OptionInvalidValueError, "unable to determine cipher passphrase for '%s'", strZ(file));

                // Add encryption filter
                cipherBlockFilterGroupAdd(ioReadFilterGroup(source), repoCipherType, cipherModeDecrypt, cipherPass);
            }

            if (cfgOptionBool(cfgOptDecompress))
            {
                // Process constants
                file = storagePathP(storageRepo(), file);

                if (cfgOptionSource(cfgOptCompressType) == cfgSourceParam)
                {
                    // Compression type was specified on command line, overwrite what we found above
                    compressType = compressTypeEnum(cfgOptionStrId(cfgOptCompressType));
                }

                // Add decompression if needed
                if (compressType != compressTypeNone)
                {
                    // Add decompression filter
                    ioFilterGroupAdd(ioReadFilterGroup(source), decompressFilterP(compressType));
                }
            }
        }

        // Open source
        if (ioReadOpen(source))
        {
            // Open the destination file now that we know the source exists and is readable
            ioWriteOpen(destination);

            // Copy data from source to destination
            ioCopyP(source, destination);

            // Close the source and destination
            ioReadClose(source);
            ioWriteClose(destination);

            // Source file exists
            result = 0;
        }
    }
    MEM_CONTEXT_TEMP_END();

    FUNCTION_LOG_RETURN(INT, result);
}

/**********************************************************************************************************************************/
FN_EXTERN int
cmdStorageGet(void)
{
    FUNCTION_LOG_VOID(logLevelDebug);

    // Assume the file is missing
    int result = 1;

    MEM_CONTEXT_TEMP_BEGIN()
    {
        TRY_BEGIN()
        {
            result = storageGetProcess(ioFdWriteNew(STRDEF("stdout"), STDOUT_FILENO, cfgOptionUInt64(cfgOptIoTimeout)));
        }
        // Ignore write errors because it's possible (even likely) that this output is being piped to something like head which will
        // exit when it gets what it needs and leave us writing to a broken pipe. It would be better to just ignore the broken pipe
        // error but currently we don't store system error codes.
        CATCH(FileWriteError)
        {
        }
        TRY_END();
    }
    MEM_CONTEXT_TEMP_END();

    FUNCTION_LOG_RETURN(INT, result);
}
