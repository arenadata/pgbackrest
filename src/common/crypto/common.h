/***********************************************************************************************************************************
Crypto Common
***********************************************************************************************************************************/
#ifndef COMMON_CRYPTO_COMMON_H
#define COMMON_CRYPTO_COMMON_H

#include <common/type/stringId.h>

/***********************************************************************************************************************************
Cipher modes
***********************************************************************************************************************************/
typedef enum
{
    cipherModeEncrypt = STRID5("encrypt", 0x521990dc50),
    cipherModeDecrypt = STRID5("decrypt", 0x521990ca40),
} CipherMode;

/***********************************************************************************************************************************
Cipher types
***********************************************************************************************************************************/
typedef enum
{
    cipherTypeNone = STRID5("none", 0x2b9ee0),
    cipherTypeAes128Cbc = STRID5("aes-128-cbc", 0x836e479d6d31415),
    cipherTypeAes192Cbc = STRID5("aes-192-cbc", 0x836de95d6d31415),
    cipherTypeAes256Cbc = STRID5("aes-256-cbc", 0xc43dfbbcdcca10),
    cipherTypeAria128Cbc = STRID6("aria-128-cbc", 0xdb91e75b0494815),
    cipherTypeAria192Cbc = STRID6("aria-192-cbc", 0xdb7a575b0494815),
    cipherTypeAria256Cbc = STRID5("aria-256-cbc", 0x1887bf779b0a6410),
    cipherTypeSM4Cbc = STRID6("sm4-cbc", 0x30836e03531),
    cipherTypeKuznyechikCbc = STRID5("kuznyechik-cbc", 0x1ed6940cb976aab4),
    cipherTypeMagmaCbc = STRID5("magma-cbc", 0x310f6169c2d0),
} CipherType;

/***********************************************************************************************************************************
Hash types
***********************************************************************************************************************************/
typedef enum
{
    hashTypeMd5 = STRID5("md5", 0x748d0),
    hashTypeSha1 = STRID6("sha1", 0x7412131),
    hashTypeSha256 = STRID5("sha256", 0x3dde05130),
} HashType;

/***********************************************************************************************************************************
Functions
***********************************************************************************************************************************/
// Initialize crypto
FN_EXTERN void cryptoInit(void);

// Throw crypto errors
FN_EXTERN void cryptoError(bool error, const char *description);
FN_EXTERN FN_NO_RETURN void cryptoErrorCode(unsigned long code, const char *description);

// Generate random bytes
FN_EXTERN void cryptoRandomBytes(unsigned char *buffer, size_t size);

#endif
