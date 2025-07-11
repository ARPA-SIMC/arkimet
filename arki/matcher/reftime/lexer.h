#ifndef ARKI_MATCHER_REFTIME_LEXER_H
#define ARKI_MATCHER_REFTIME_LEXER_H

#include <arki/core/fuzzytime.h>
#include <cstring>
#include <ctime>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace arki {
namespace matcher {
namespace reftime {
namespace lexer {

struct LexInterval
{
    int val;
    int idx;

    void add_to(int* vals) const { vals[idx] += val; }
};

struct Parser
{
    const char* orig_buf;
    const char* buf;
    unsigned len;

    Parser(const char* sbuf, unsigned slen)
        : orig_buf(sbuf), buf(sbuf), len(slen)
    {
    }

    bool string_in(std::initializer_list<const char*> values) const;

    void error(const std::string& msg)
    {
        std::string lead(orig_buf, buf - orig_buf);
        std::string trail(buf, len);
        throw std::invalid_argument("cannot parse reftime match expression \"" +
                                    lead + "[HERE]" + trail + "\": " + msg);
    }

    void eatNonSpaces()
    {
        while (len && !isspace(*buf))
        {
            ++buf;
            --len;
        }
    }

    void eatSpaces()
    {
        while (len && isspace(*buf))
        {
            ++buf;
            --len;
        }
    }

    char itype();

    void eatInsensitive(const char* s)
    {
        if (*s == 0)
            return;
        if (!len || ::tolower(*buf) != s[0])
            error(std::string("expecting ") + s);
        ++buf;
        --len;
        eatInsensitive(s + 1);
    }
};

/**
 * Parser for Easter times
 */
arki::core::FuzzyTime* parse_easter(const char* buf, unsigned len);

/**
 * Simple recursive descent parser for reftimes
 */
struct DTParser : public Parser
{
    using Parser::Parser;

    /// Parse a number
    int num()
    {
        std::string val;
        for (; len && isdigit(*buf); ++buf, --len)
            val += *buf;
        if (val.empty())
            error("number expected");
        return strtoul(val.c_str(), 0, 10);
    }
    bool eat(char c)
    {
        if (!len || *buf != c)
            return false;
        ++buf;
        --len;
        return true;
        // error(string("expected '") + c + "'");
    }
    bool eatOneOf(const char* chars)
    {
        if (!len || strchr(chars, *buf) == NULL)
            return false;
        ++buf;
        --len;
        return true;
    }
    void end()
    {
        if (len)
            error("trailing characters found");
    }
    std::unique_ptr<arki::core::FuzzyTime> getDate()
    {
        std::unique_ptr<arki::core::FuzzyTime> res(new arki::core::FuzzyTime);

        // Year
        res->ye = num();
        // Month
        if (!eat('-'))
            goto done;
        res->mo = num();
        // Day
        if (!eat('-'))
            goto done;
        res->da = num();
        // Eat optional 'T' at the end of the date
        if (len && (*buf == 'T' || *buf == 't'))
        {
            ++buf;
            --len;
        }
        // Eat optional spaces
        eatSpaces();
        // Hour
        if (!len)
            goto done;
        res->ho = num();
        // Minute
        if (!eat(':'))
            goto done;
        res->mi = num();
        // Second
        if (!eat(':'))
            goto done;
        res->se = num();

    done:
        try
        {
            res->validate();
        }
        catch (std::invalid_argument& e)
        {
            error(e.what());
        }

        return res;
    }

    void check_minmax(int value, int min, int max, const char* what)
    {
        if (value < min || value > max)
            error(std::string(what) + " must be between " +
                  std::to_string(min) + " and " + std::to_string(max));
    }

    void getTime(int* res)
    {
        // Hour
        res[0] = num();
        check_minmax(res[0], 0, 24, "hour");
        // Minute
        if (!eat(':'))
            return;
        res[1] = num();
        check_minmax(res[1], 0, 59, "minute");
        // Second
        if (!eat(':'))
            return;
        res[2] = num();
        check_minmax(res[2], 0, 60, "second");

        if (res[0] == 24)
        {
            if (res[1] != -1 && res[1] != 0)
                error("on hour 24, minute must be zero");
            if (res[2] != -1 && res[2] != 0)
                error("on hour 24, second must be zero");
        }
    }
};

arki::core::FuzzyTime* parse_datetime(const char* buf, unsigned len);
void parse_time(const char* buf, unsigned len, int* res);

struct ISParser : public Parser
{
    struct LexInterval& res;

    ISParser(const char* sbuf, unsigned slen, struct LexInterval& res)
        : Parser(sbuf, slen), res(res)
    {
    }

    unsigned long int num()
    {
        std::string val;
        for (; len && isdigit(*buf); ++buf, --len)
            val += *buf;
        if (val.empty())
            error("number expected");
        return strtoul(val.c_str(), 0, 10);
    }
};

/// Parser for time intervals
struct IParser : public ISParser
{
    IParser(const char* sbuf, unsigned slen, struct LexInterval& res);

    void itype();
};

/// Parser for time steps
struct SParser : public ISParser
{
    SParser(const char* sbuf, unsigned slen, struct LexInterval& res);

    void itype();
};

} // namespace lexer
} // namespace reftime
} // namespace matcher
} // namespace arki

#endif
