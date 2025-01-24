/***********************************************************************************************************************************
String Handler Extensions
***********************************************************************************************************************************/
// Include core module
#include "common/type/string.c"

/**********************************************************************************************************************************/
String *
strUpper(String *const this)
{
    FUNCTION_TEST_BEGIN();
        FUNCTION_TEST_PARAM(STRING, this);
    FUNCTION_TEST_END();

    ASSERT(this != NULL);

    for (size_t idx = 0; idx < strSize(this); idx++)
        this->pub.buffer[idx] = (char)toupper(this->pub.buffer[idx]);

    FUNCTION_TEST_RETURN(STRING, this);
}

/**********************************************************************************************************************************/
String *
strReplace(String *const this, const String *const replace, const String *const with)
{
    FUNCTION_TEST_BEGIN();
        FUNCTION_TEST_PARAM(STRING, this);
        FUNCTION_TEST_PARAM(STRING, replace);
        FUNCTION_TEST_PARAM(STRING, with);
    FUNCTION_TEST_END();

    MEM_CONTEXT_TEMP_BEGIN()
    {
        // Does replace exist?
        const char *found = strstr(this->pub.buffer, strZ(replace));

        while (found != NULL)
        {
            // Offset into string
            const size_t offset = (size_t)(found - this->pub.buffer);

            // Calculate new size and resize
            const size_t size = strSize(this) - strSize(replace) + strSize(with);
            strResize(this, size);

            // Replace
            memmove(
                this->pub.buffer + offset + strSize(with), this->pub.buffer + offset + strSize(replace),
                strSize(this) - offset - strSize(replace));
            memcpy(this->pub.buffer + offset, strZ(with), strSize(with));

            // Set new size and zero-terminate
            this->pub.size = (unsigned int)size;
            this->pub.buffer[size] = '\0';

            // Does replace exist again?
            found = strstr(this->pub.buffer + offset + strSize(with), strZ(replace));
        }
    }
    MEM_CONTEXT_TEMP_END();

    FUNCTION_TEST_RETURN(STRING, this);
}

/**********************************************************************************************************************************/
String *
strEscape(String *const this)
{
    FUNCTION_TEST_BEGIN();
        FUNCTION_TEST_PARAM(STRING, this);
    FUNCTION_TEST_END();

    String *ret = strNew();

    // Track portion of string with no escapes
    const char *valuePtr = strZ(this);
    const char *noEscape = NULL;
    size_t noEscapeSize = 0;

    for (unsigned int valueIdx = 0; valueIdx < strSize(this); valueIdx++)
    {
        switch (*valuePtr)
        {
            case '"':
            case '\\':
            case '\n':
            case '\r':
            case '\t':
            case '\b':
            case '\f':
            {
                // Copy portion of string without escapes
                if (noEscapeSize > 0)
                {
                    strCatZN(ret, noEscape, noEscapeSize);
                    noEscapeSize = 0;
                }

                switch (*valuePtr)
                {
                    case '"':
                        strCatZ(ret, "\\\"");
                        break;

                    case '\\':
                        strCatZ(ret, "\\\\");
                        break;

                    case '\n':
                        strCatZ(ret, "\\n");
                        break;

                    case '\r':
                        strCatZ(ret, "\\r");
                        break;

                    case '\t':
                        strCatZ(ret, "\\t");
                        break;

                    case '\b':
                        strCatZ(ret, "\\b");
                        break;

                    case '\f':
                        strCatZ(ret, "\\f");
                        break;
                }

                break;
            }

            default:
            {
                // If escape string is zero size then start it
                if (noEscapeSize == 0)
                    noEscape = valuePtr;

                noEscapeSize++;
                break;
            }
        }

        valuePtr++;
    }

    // Copy portion of string without escapes
    if (noEscapeSize > 0)
        strCatZN(ret, noEscape, noEscapeSize);

    FUNCTION_TEST_RETURN(STRING, ret);
}
