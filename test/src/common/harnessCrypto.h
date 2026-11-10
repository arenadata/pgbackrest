/***********************************************************************************************************************************
Harness for testing different ciphers
***********************************************************************************************************************************/
#ifndef TEST_COMMON_HARNESS_CRYPTO_H
#define TEST_COMMON_HARNESS_CRYPTO_H

#include <openssl/opensslv.h>

#include "common/crypto/common.h"
#include "common/macro.h"

typedef struct HrnCipherTest
{
    CipherType type;
    const char *name;
} HrnCipherTest;

/* Canonical names avoid alias-normalization warnings in command tests. */
static const HrnCipherTest hrnCipherList[] =
{
    {.type = cipherTypeAes256Cbc, .name = "aes-256-cbc"},
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
    {.type = cipherTypeKuznyechikCbc, .name = "kuznyechik-cbc"},
#else
    {.type = cipherTypeGrasshopperCbc, .name = "grasshopper-cbc"},
#endif
    {.type = cipherTypeMagmaCbc, .name = "magma-cbc"},
};

#define HRN_CIPHER_BEGIN(cipherParam)                               \
    do                                                                   \
    {                                                                    \
        for (unsigned int hrnCipherIdx = 0;                          \
             hrnCipherIdx < LENGTH_OF(hrnCipherList);            \
             hrnCipherIdx++)                                         \
        {                                                                \
            const HrnCipherTest cipherParam =                            \
                hrnCipherList[hrnCipherIdx];

#define HRN_CIPHER_END()                                            \
        }                                                                \
    }                                                                    \
    while (0)

#endif
