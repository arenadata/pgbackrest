/***********************************************************************************************************************************
Crypto Common
***********************************************************************************************************************************/
#include "build.auto.h"

#include <openssl/conf.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>

#include "common/crypto/common.h"
#include "common/debug.h"
#include "common/log.h"

/***********************************************************************************************************************************
Flag to indicate if OpenSSL has already been initialized
***********************************************************************************************************************************/
static bool cryptoInitDone = false;

/**********************************************************************************************************************************/
FN_EXTERN void
cryptoError(const bool error, const char *const description)
{
    FUNCTION_TEST_BEGIN();
        FUNCTION_TEST_PARAM(BOOL, error);
        FUNCTION_TEST_PARAM(STRINGZ, description);
    FUNCTION_TEST_END();

    if (error)
        cryptoErrorCode(ERR_get_error(), description);

    FUNCTION_TEST_RETURN_VOID();
}

FN_EXTERN void
cryptoErrorCode(const unsigned long code, const char *const description)
{
    FUNCTION_TEST_BEGIN();
        FUNCTION_TEST_PARAM(UINT64, code);
        FUNCTION_TEST_PARAM(STRINGZ, description);
    FUNCTION_TEST_END();

    const char *const errorMessage = ERR_reason_error_string(code);
    THROW_FMT(CryptoError, "%s: [%lu] %s", description, code, errorMessage == NULL ? "no details available" : errorMessage);

    FUNCTION_TEST_NO_RETURN();
}

/**********************************************************************************************************************************/
FN_EXTERN void
cryptoInit(void)
{
    FUNCTION_LOG_VOID(logLevelTrace);

    if (!cryptoInitDone)
    {
        // Load crypto strings and algorithms
        ERR_load_crypto_strings();
        OpenSSL_add_all_algorithms();

        // Initialization
        OPENSSL_init_ssl(OPENSSL_INIT_LOAD_CONFIG, NULL);

        // Mark crypto as initialized
        cryptoInitDone = true;
    }

    FUNCTION_LOG_RETURN_VOID();
}

/**********************************************************************************************************************************/
FN_EXTERN void
cryptoRandomBytes(unsigned char *const buffer, const size_t size)
{
    FUNCTION_LOG_BEGIN(logLevelTrace);
        FUNCTION_LOG_PARAM_P(UCHARDATA, buffer);
        FUNCTION_LOG_PARAM(SIZE, size);
    FUNCTION_LOG_END();

    ASSERT(buffer != NULL);
    ASSERT(size > 0);

    // A failure must not go unnoticed since the caller would be left with bytes that are not random
    cryptoError(RAND_bytes(buffer, (int)size) != 1, "unable to generate random bytes");

    FUNCTION_LOG_RETURN_VOID();
}

/***********************************************************************************************************************************
Decoding table
***********************************************************************************************************************************/
static const struct CipherTypeLocal
{
    CipherType type;                                                // Cipher type
    const char *name;                                               // OpenSSL cipher name
} cipherTypeLocal[] =
{
    {.type = cipherTypeNone,          .name = "none"},
    {.type = cipherTypeAes128Cbc,      .name = "aes-128-cbc"},
    {.type = cipherTypeAes192Cbc,      .name = "aes-192-cbc"},
    {.type = cipherTypeAes256Cbc,      .name = "aes-256-cbc"},
    {.type = cipherTypeKuznyechikCbc,  .name = "kuznyechik-cbc"},
    {.type = cipherTypeGrasshopperCbc, .name = "grasshopper-cbc"},
    {.type = cipherTypeMagmaCbc,       .name = "magma-cbc"},
    {.type = cipherTypeAria128Cbc,     .name = "aria-128-cbc"},
    {.type = cipherTypeAria192Cbc,     .name = "aria-192-cbc"},
    {.type = cipherTypeAria256Cbc,     .name = "aria-256-cbc"},
    {.type = cipherTypeSM4Cbc,         .name = "sm4-cbc"},
};

/***********************************************************************************************************************************/
FN_EXTERN String *
cipherConvertFromIdToStr(const CipherType cipherType)
{
    FUNCTION_TEST_BEGIN();
        FUNCTION_TEST_PARAM(STRING_ID, cipherType);
    FUNCTION_TEST_END();

    unsigned int typeIdx = 0;

    for (; typeIdx < LENGTH_OF(cipherTypeLocal); typeIdx++)
    {
        if (cipherTypeLocal[typeIdx].type == cipherType)
            break;
    }

    if (typeIdx == LENGTH_OF(cipherTypeLocal))
        THROW_FMT(AssertError, "invalid cipher type '%s'", strZ(strIdToStr(cipherType)));

    String *result = strNewZ(cipherTypeLocal[typeIdx].name);

    FUNCTION_TEST_RETURN(STRING, result);
}
