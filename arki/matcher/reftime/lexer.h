#ifndef ARKI_MATCHER_REFTIME_LEXER_H
#define ARKI_MATCHER_REFTIME_LEXER_H

#include <arki/types.h>
#include <arki/core/fuzzytime.h>
#include <string>
#include <ctime>
#include <cstring>
#include <vector>

namespace arki {
namespace matcher {
namespace reftime {
namespace lexer {

struct LexInterval
{
    int val;
    int idx;

    void add_to(int* vals) const
    {
        vals[idx] += val;
    }
};


struct Parser
{
    std::string str;
    std::string::const_iterator cur;

    Parser(const char* buf, unsigned len) : str(buf, len), cur(str.begin()) {}

    void error(const std::string& msg)
    {
        std::string lead = str.substr(0, cur - str.begin());
        std::string trail = str.substr(cur - str.begin());
        throw std::invalid_argument("cannot parse reftime match expression \"" + lead + "[HERE]" + trail + "\": " + msg);
    }

    void eatNonSpaces()
    {
        while (!isspace(*cur))
            ++cur;
    }

    void eatSpaces()
    {
        while (isspace(*cur))
            ++cur;
    }

    char itype();

    void eatInsensitive(const char* s)
    {
        if (*s == 0) return;
        if (cur == str.end() || ::tolower(*cur) != s[0])
            error(std::string("expecting ") + s);
        ++cur;
        eatInsensitive(s+1);
    }
};

/**
 * Parser for Easter times
 */
arki::core::FuzzyTime* parse_easter(const std::string& str);

/**
 * Simple recursive descent parser for reftimes
 */
struct DTParser : public Parser
{
    DTParser(const char* buf, unsigned len) : Parser(buf, len) {}

    /// Parse a number
    int num()
    {
        std::string val;
        for ( ; cur != str.end() && isdigit(*cur); ++cur)
            val += *cur;
        if (val.empty())
            error("number expected");
        return strtoul(val.c_str(), 0, 10);
    }
    bool eat(char c)
    {
        if (cur == str.end() || *cur != c)
            return false;
        ++cur;
        return true;
        //error(string("expected '") + c + "'");
    }
    bool eatOneOf(const char* chars)
    {
        if (cur == str.end() || strchr(chars, *cur) == NULL)
            return false;
        ++cur;
        return true;
    }
    void end()
    {
        if (cur != str.end())
            error("trailing characters found");
    }
    std::unique_ptr<arki::core::FuzzyTime> getDate()
    {
        std::unique_ptr<arki::core::FuzzyTime> res(new arki::core::FuzzyTime);

        // Year
        res->ye = num();
        // Month
        if (!eat('-')) return res;
        res->mo = num();
        // Day
        if (!eat('-')) return res;
        res->da = num();
        // Eat optional 'T' at the end of the date
        if (cur != str.end() && (*cur == 'T' || *cur == 't')) ++cur;
        // Eat optional spaces
        eatSpaces();
        // Hour
        if (cur == str.end()) return res;
        res->ho = num();
        // Minute
        if (!eat(':')) return res;
        res->mi = num();
        // Second
        if (!eat(':')) return res;
        res->se = num();

        return res;
    }

    void getTime(int* res)
    {
        // Hour
        res[0] = num();
        // Minute
        if (!eat(':')) return;
        res[1] = num();
        // Second
        if (!eat(':')) return;
        res[2] = num();
    }
};

arki::core::FuzzyTime* parse_datetime(const char* buf, unsigned len);
void parse_time(const char* buf, unsigned len, int* res);


struct ISParser : public Parser
{
    struct LexInterval& res;

    ISParser(const char* buf, unsigned len, struct LexInterval& res) : Parser(buf, len), res(res)
    {
    }

    unsigned long int num()
    {
        std::string val;
        for ( ; cur != str.end() && isdigit(*cur); ++cur)
            val += *cur;
        if (val.empty())
            error("number expected");
        return strtoul(val.c_str(), 0, 10);
    }
};


/// Parser for time intervals
struct IParser : public ISParser
{
    IParser(const char* buf, unsigned len, struct LexInterval& res);

    void itype();
};

/// Parser for time steps
struct SParser : public ISParser
{
    SParser(const char* buf, unsigned len, struct LexInterval& res);

    void itype();
};


}
}
}
}

#endif
