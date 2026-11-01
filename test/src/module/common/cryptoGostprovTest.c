/***********************************************************************************************************************************
Test Block Cipher
***********************************************************************************************************************************/
#include "common/io/bufferRead.h"
#include "common/io/filter/filter.h"
#include "common/io/io.h"
#include "common/type/json.h"

/***********************************************************************************************************************************
Data for testing
***********************************************************************************************************************************/
#define TEST_CIPHER                                                 "kuznyechik-cbc"
#define TEST_PASS                                                   "areallybadpassphrase"
#define TEST_PLAINTEXT                                              "plaintext"
#define TEST_BUFFER_SIZE                                            256

/***********************************************************************************************************************************
Test Run
***********************************************************************************************************************************/
static void
testRun(void)
{
    FUNCTION_HARNESS_VOID();

    setenv("OPENSSL_CONF", "/openssl-gost-provider-only.cnf", 1);

    const Buffer *testPass = BUFSTRDEF(TEST_PASS);
    const Buffer *testPlainText = BUFSTRDEF(TEST_PLAINTEXT);

    // *****************************************************************************************************************************
    if (testBegin("Common"))
    {
        TEST_RESULT_VOID(cryptoInit(), "initialize crypto");
        TEST_RESULT_VOID(cryptoInit(), "initialize crypto again");

        // -------------------------------------------------------------------------------------------------------------------------
        cryptoInit();

        TEST_RESULT_VOID(cryptoError(false, "no error here"), "no error");

        EVP_MD_CTX *context = EVP_MD_CTX_create();
        TEST_ERROR(
            cryptoError(EVP_DigestInit_ex(context, NULL, NULL) != 1, "unable to initialize hash context"), CryptoError,
            "unable to initialize hash context: "
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
            "[50331787]"
#else
            "[101187723]"
#endif
            " no digest set");
        EVP_MD_CTX_destroy(context);

        TEST_ERROR(cryptoError(true, "no error"), CryptoError, "no error: [0] no details available");

        // No cryptoRandomBytes test because gost doesn't have function for rand and this check gives buffer with all zeros
    }

    // *****************************************************************************************************************************
    if (testBegin("CipherBlock"))
    {
        // Cipher and digest errors
        // -------------------------------------------------------------------------------------------------------------------------
        TEST_ERROR(
            cipherBlockNewP(cipherModeEncrypt, cipherTypeNone, BUFSTRZ(TEST_PASS)), AssertError, "invalid cipher type 'none'");
        TEST_ERROR(
            cipherBlockNewP(cipherModeEncrypt, cipherTypeAes256Cbc, BUFSTRZ(TEST_PASS)), AssertError, "unable to load cipher 'aes-256-cbc'");
        TEST_ERROR(
            cipherBlockNewP(cipherModeEncrypt, cipherTypeKuznyechikCbc, testPass, .digest = STRDEF(BOGUS_STR)), AssertError,
            "unable to load digest 'BOGUS'");

        // Initialization of object
        // -------------------------------------------------------------------------------------------------------------------------
        IoFilter *cipherFilter = cipherBlockNewP(cipherModeEncrypt, cipherTypeKuznyechikCbc, testPass);
        CipherBlock *cipherBlock = (CipherBlock *)ioFilterDriver(cipherFilter);
        TEST_RESULT_INT(cipherBlock->mode, cipherModeEncrypt, "mode is valid");
        TEST_RESULT_UINT(bufSize(cipherBlock->pass), strlen(TEST_PASS), "passphrase size is valid");
        TEST_RESULT_BOOL(memcmp(bufPtrConst(cipherBlock->pass), TEST_PASS, strlen(TEST_PASS)) == 0, true, "passphrase is valid");
        TEST_RESULT_BOOL(cipherBlock->saltDone, false, "salt done is false");
        TEST_RESULT_BOOL(cipherBlock->processDone, false, "process done is false");
        TEST_RESULT_UINT(cipherBlock->headerSize, 0, "header size is 0");
        TEST_RESULT_PTR_NE(cipherBlock->cipher, NULL, "cipher is set");
        TEST_RESULT_PTR_NE(cipherBlock->digest, NULL, "digest is set");
        TEST_RESULT_PTR(cipherBlock->cipherContext, NULL, "cipher context is not set");

        ioFilterFree(cipherFilter);

        // Encrypt
        // -------------------------------------------------------------------------------------------------------------------------
        Buffer *encryptBuffer = bufNew(TEST_BUFFER_SIZE);

        IoFilter *blockEncryptFilter = cipherBlockNewP(cipherModeEncrypt, cipherTypeKuznyechikCbc, testPass);
        blockEncryptFilter = cipherBlockNewPack(ioFilterParamList(blockEncryptFilter));
        CipherBlock *blockEncrypt = (CipherBlock *)ioFilterDriver(blockEncryptFilter);

        TEST_RESULT_UINT(
            cipherBlockProcessSize(blockEncrypt, strlen(TEST_PLAINTEXT)),
            strlen(TEST_PLAINTEXT) + EVP_MAX_BLOCK_LENGTH + CIPHER_BLOCK_MAGIC_SIZE + PKCS5_SALT_LEN, "check process size");

        bufLimitSet(encryptBuffer, CIPHER_BLOCK_MAGIC_SIZE);
        ioFilterProcessInOut(blockEncryptFilter, testPlainText, encryptBuffer);
        TEST_RESULT_UINT(bufUsed(encryptBuffer), CIPHER_BLOCK_MAGIC_SIZE, "cipher size is magic size");
        TEST_RESULT_BOOL(ioFilterInputSame(blockEncryptFilter), true, "filter needs same input");

        bufLimitSet(encryptBuffer, CIPHER_BLOCK_MAGIC_SIZE + PKCS5_SALT_LEN);
        ioFilterProcessInOut(blockEncryptFilter, testPlainText, encryptBuffer);
        TEST_RESULT_BOOL(ioFilterInputSame(blockEncryptFilter), false, "filter does not need same input");

        TEST_RESULT_BOOL(blockEncrypt->saltDone, true, "salt done is true");
        TEST_RESULT_BOOL(blockEncrypt->processDone, true, "process done is true");
        TEST_RESULT_UINT(blockEncrypt->headerSize, 0, "header size is 0");
        TEST_RESULT_UINT(bufUsed(encryptBuffer), CIPHER_BLOCK_HEADER_SIZE, "cipher size is header len");

        TEST_RESULT_UINT(
            cipherBlockProcessSize(blockEncrypt, strlen(TEST_PLAINTEXT)),
            strlen(TEST_PLAINTEXT) + EVP_MAX_BLOCK_LENGTH, "check process size");

        bufLimitSet(
            encryptBuffer, CIPHER_BLOCK_MAGIC_SIZE + PKCS5_SALT_LEN + (size_t)EVP_CIPHER_block_size(blockEncrypt->cipher) / 2);
        ioFilterProcessInOut(blockEncryptFilter, testPlainText, encryptBuffer);
        bufLimitSet(
            encryptBuffer, CIPHER_BLOCK_MAGIC_SIZE + PKCS5_SALT_LEN + (size_t)EVP_CIPHER_block_size(blockEncrypt->cipher));
        ioFilterProcessInOut(blockEncryptFilter, testPlainText, encryptBuffer);
        bufLimitClear(encryptBuffer);

        TEST_RESULT_UINT(
            bufUsed(encryptBuffer), CIPHER_BLOCK_HEADER_SIZE + (size_t)EVP_CIPHER_block_size(blockEncrypt->cipher),
            "cipher size increases by one block");
        TEST_RESULT_BOOL(ioFilterDone(blockEncryptFilter), false, "filter is not done");

        ioFilterProcessInOut(blockEncryptFilter, NULL, encryptBuffer);
        TEST_RESULT_UINT(
            bufUsed(encryptBuffer), CIPHER_BLOCK_HEADER_SIZE + (size_t)(EVP_CIPHER_block_size(blockEncrypt->cipher) * 2),
            "cipher size increases by one block on flush");
        TEST_RESULT_BOOL(ioFilterDone(blockEncryptFilter), true, "filter is done");

        ioFilterFree(blockEncryptFilter);

        // Decrypt in one pass
        // -------------------------------------------------------------------------------------------------------------------------
        Buffer *decryptBuffer = bufNew(TEST_BUFFER_SIZE);

        IoFilter *blockDecryptFilter = cipherBlockNewP(cipherModeDecrypt, cipherTypeKuznyechikCbc, testPass, .digest = STRDEF("sha1"));
        blockDecryptFilter = cipherBlockNewPack(ioFilterParamList(blockDecryptFilter));
        CipherBlock *blockDecrypt = (CipherBlock *)ioFilterDriver(blockDecryptFilter);

        TEST_RESULT_UINT(
            cipherBlockProcessSize(blockDecrypt, bufUsed(encryptBuffer)), bufUsed(encryptBuffer) + EVP_MAX_BLOCK_LENGTH,
            "check process size");

        ioFilterProcessInOut(blockDecryptFilter, encryptBuffer, decryptBuffer);
        TEST_RESULT_UINT_INT(bufUsed(decryptBuffer), EVP_CIPHER_block_size(blockDecrypt->cipher), "decrypt size is one block");

        ioFilterProcessInOut(blockDecryptFilter, NULL, decryptBuffer);
        TEST_RESULT_UINT(bufUsed(decryptBuffer), strlen(TEST_PLAINTEXT) * 2, "check final decrypt size");

        TEST_RESULT_STR_Z(strNewBuf(decryptBuffer), TEST_PLAINTEXT TEST_PLAINTEXT, "check final decrypt buffer");

        ioFilterFree(blockDecryptFilter);
    }

    unsetenv("OPENSSL_CONF");

    FUNCTION_HARNESS_RETURN_VOID();
}
