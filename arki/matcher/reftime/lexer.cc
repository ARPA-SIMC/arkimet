#include "lexer.h"

namespace arki {
namespace matcher {
namespace reftime {
namespace lexer {

char Parser::itype()
{
    switch (*cur)
    {
        case 'h': {
            std::string name(cur, str.end());
            if (name == "h" || name == "hour" || name == "hours")
                return 'h';
            error("expected h, hour or hours");
            break;
        }
        case 'm': {
            std::string name(cur, str.end());
            if (name == "m" || name == "min" || name == "minute" || name == "minutes")
                return 'm';
            if (name == "month" || name == "months")
                return 'M';
            error("expected m, min, minute, minutes, month or months");
            break;
        }
        case 's': {
            std::string name(cur, str.end());
            if (name == "s" || name == "sec" || name == "second" || name == "seconds")
                return 's';
            error("expected s, sec, second or seconds");
            break;
        }
        case 'w': {
            std::string name(cur, str.end());
            if (name == "w" || name == "week" || name == "weeks")
                return 'w';
            error("expected w, week or weeks");
            break;
        }
        case 'd': {
            std::string name(cur, str.end());
            if (name == "d" || name == "day" || name == "days")
                return 'd';
            error("expected d, day or days");
            break;
        }
        case 'y': {
            std::string name(cur, str.end());
            if (name == "y" || name == "year" || name == "years")
                return 'y';
            error("expected y, year or years");
            break;
        }
    }
    error("expected a time name like hour, minute, second, day, week, month or year");
    // This is just to appease the compiler warnings
    return 0;
}

arki::core::FuzzyTime* parse_easter(const std::string& str)
{
    if (str.size() < 4)
        throw std::invalid_argument("cannot parse reftime match expression \"" + str + "\": expecting at least 4 characters");
    std::unique_ptr<arki::core::FuzzyTime> res(new arki::core::FuzzyTime);
    // Parse the year directly
    res->set_easter(std::stoi(str.substr(str.size() - 4)));
    return res.release();
}

arki::core::FuzzyTime* parse_datetime(const std::string& str)
{
    DTParser parser(str);
    auto res = parser.getDate();
    parser.end();
    return res.release();
}

void parse_time(const std::string& str, int* res)
{
    DTParser parser(str);
    for (int i = 0; i < 3; ++i)
        res[i] = -1;
    parser.getTime(res);
    parser.eatOneOf("zZ");
    parser.end();
};


IParser::IParser(const std::string& str, struct LexInterval& res) : ISParser(str, res)
{
    if (cur == str.end())
        error("number or 'a' expected");
    if (*cur == 'a' || *cur == 'A')
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
    switch (*cur)
    {
        case 'h': {
            std::string name(cur, str.end());
            if (name == "h" || name == "hour" || name == "hours")
            {
                res.idx = 3;
                return;
            }
            error("expected h, hour or hours");
            break;
        }
        case 'm': {
            std::string name(cur, str.end());
            if (name == "m" || name == "min" || name == "minute" || name == "minutes")
            {
                res.idx = 4;
                return;
            }
            if (name == "month" || name == "months")
            {
                res.idx = 1;
                return;
            }
            error("expected m, min, minute, minutes, month or months");
            break;
        }
        case 's': {
            std::string name(cur, str.end());
            if (name == "s" || name == "sec" || name == "second" || name == "seconds")
            {
                res.idx = 5;
                return;
            }
            error("expected s, sec, second or seconds");
            break;
        }
        case 'w': {
            std::string name(cur, str.end());
            if (name == "w" || name == "week" || name == "weeks")
            {
                res.val *= 7;
                res.idx = 2;
                return;
            }
            error("expected w, week or weeks");
            break;
        }
        case 'd': {
            std::string name(cur, str.end());
            if (name == "d" || name == "day" || name == "days")
            {
                res.idx = 2;
                return;
            }
            error("expected d, day or days");
            break;
        }
        case 'y': {
            std::string name(cur, str.end());
            if (name == "y" || name == "year" || name == "years")
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


SParser::SParser(const std::string& str, struct LexInterval& res) : ISParser(str, res)
{
    if (cur == str.end())
        error("expecting time step");
    if (*cur == '%')
        ++cur;
    else
        eatInsensitive("every");
    eatSpaces();
    res.val = num();
    eatSpaces();
    itype();
}

void SParser::itype()
{
    switch (*cur)
    {
        case 'h': {
            std::string name(cur, str.end());
            if (name == "h" || name == "hour" || name == "hours")
            {
                res.idx = 3;
                return;
            }
            error("expected h, hour or hours");
            break;
        }
        case 'm': {
                      std::string name(cur, str.end());
            if (name == "m" || name == "min" || name == "minute" || name == "minutes")
            {
                res.idx = 4;
                return;
            }
            error("expected m, min, minute, minutes");
            break;
        }
        case 's': {
            std::string name(cur, str.end());
            if (name == "s" || name == "sec" || name == "second" || name == "seconds")
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