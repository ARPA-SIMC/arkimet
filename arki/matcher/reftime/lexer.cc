#include "lexer.h"

namespace arki {
namespace matcher {
namespace reftime {
namespace lexer {

bool Parser::string_in(std::initializer_list<const char*> values) const
{
    for (const char* val: values)
        if (strncmp(buf, val, len) == 0)
            return true;
    return false;
}

char Parser::itype()
{
    switch (*buf)
    {
        case 'h': {
            if (string_in({"h", "hour", "hours"}))
                return 'h';
            error("expected h, hour or hours");
            break;
        }
        case 'm': {
            if (string_in({"m", "min", "minute", "minutes"}))
                return 'm';
            if (string_in({"month", "months"}))
                return 'M';
            error("expected m, min, minute, minutes, month or months");
            break;
        }
        case 's': {
            if (string_in({"s", "sec", "second", "seconds"}))
                return 's';
            error("expected s, sec, second or seconds");
            break;
        }
        case 'w': {
            if (string_in({"w", "week", "weeks"}))
                return 'w';
            error("expected w, week or weeks");
            break;
        }
        case 'd': {
            if (string_in({"d", "day", "days"}))
                return 'd';
            error("expected d, day or days");
            break;
        }
        case 'y': {
            if (string_in({"y", "year", "years"}))
                return 'y';
            error("expected y, year or years");
            break;
        }
    }
    error("expected a time name like hour, minute, second, day, week, month or year");
    // This is just to appease the compiler warnings
    return 0;
}

arki::core::FuzzyTime* parse_easter(const char* buf, unsigned len)
{
    if (len < 4)
        throw std::invalid_argument("cannot parse reftime match expression \"" + std::string(buf, len) + "\": expecting at least 4 characters");
    std::unique_ptr<arki::core::FuzzyTime> res(new arki::core::FuzzyTime);
    // Parse the year directly
    res->set_easter(strtoul(buf + len - 4, nullptr, 10));
    return res.release();
}

arki::core::FuzzyTime* parse_datetime(const char* buf, unsigned len)
{
    DTParser parser(buf, len);
    auto res = parser.getDate();
    parser.end();
    return res.release();
}

void parse_time(const char* buf, unsigned len, int* res)
{
    DTParser parser(buf, len);
    for (int i = 0; i < 3; ++i)
        res[i] = -1;
    parser.getTime(res);
    parser.eatOneOf("zZ");
    parser.end();
};


IParser::IParser(const char* sbuf, unsigned slen, struct LexInterval& res)
    : ISParser(sbuf, slen, res)
{
    if (!len)
        error("number or 'a' expected");
    if (*buf == 'a' || *buf == 'A')
    {
        res.val = 1;
        eatNonSpaces();
    } else {
        res.val = num();
    }
    eatSpaces();
    itype();
}

void IParser::itype()
{
    switch (*buf)
    {
        case 'h': {
            if (string_in({"h", "hour", "hours"}))
            {
                res.idx = 3;
                return;
            }
            error("expected h, hour or hours");
            break;
        }
        case 'm': {
            if (string_in({"m", "min", "minute", "minutes"}))
            {
                res.idx = 4;
                return;
            }
            if (string_in({"month", "months"}))
            {
                res.idx = 1;
                return;
            }
            error("expected m, min, minute, minutes, month or months");
            break;
        }
        case 's': {
            if (string_in({"s", "sec", "second", "seconds"}))
            {
                res.idx = 5;
                return;
            }
            error("expected s, sec, second or seconds");
            break;
        }
        case 'w': {
            if (string_in({"w", "week", "weeks"}))
            {
                res.val *= 7;
                res.idx = 2;
                return;
            }
            error("expected w, week or weeks");
            break;
        }
        case 'd': {
            if (string_in({"d", "day", "days"}))
            {
                res.idx = 2;
                return;
            }
            error("expected d, day or days");
            break;
        }
        case 'y': {
            if (string_in({"y", "year", "years"}))
            {
                res.idx = 0;
                return;
            }
            error("expected y, year or years");
            break;
        }
    }
    error("expected a time name like hour, minute, second, day, week, month or year");
}


SParser::SParser(const char* sbuf, unsigned slen, struct LexInterval& res) : ISParser(sbuf, slen, res)
{
    if (!len)
        error("expecting time step");
    if (*buf == '%')
    {
        ++buf;
        --len;
    }
    else
        eatInsensitive("every");
    eatSpaces();
    res.val = num();
    eatSpaces();
    itype();
}

void SParser::itype()
{
    switch (*buf)
    {
        case 'h': {
            if (string_in({"h", "hour", "hours"}))
            {
                res.idx = 3;
                return;
            }
            error("expected h, hour or hours");
            break;
        }
        case 'm': {
            if (string_in({"m", "min", "minute", "minutes"}))
            {
                res.idx = 4;
                return;
            }
            error("expected m, min, minute, minutes");
            break;
        }
        case 's': {
            if (string_in({"s", "sec", "second", "seconds"}))
            {
                res.idx = 5;
                return;
            }
            error("expected s, sec, second or seconds");
            break;
        }
    }
    error("expected a time name like hours, minutes or seconds");
}

}
}
}
}
