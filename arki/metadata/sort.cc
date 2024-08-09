#include "sort.h"
#include "arki/types/reftime.h"
#include "arki/metadata.h"
#include "arki/exceptions.h"
#include "arki/utils/string.h"
#include "arki/utils/regexp.h"
#include <vector>
#include <cctype>
#include <cstring>

using namespace std;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace metadata {
namespace sort {

/// Comparison function for one metadata item
struct Item
{
    types::Code code;
    bool reverse;

    Item(const std::string& expr)
    {
        if (expr.empty())
            throw_consistency_error("parsing sort expression", "metadata name is the empty string");
        size_t start = 0;
        switch (expr[0])
        {
            case '+': reverse = false; start = 1; break;
            case '-': reverse = true; start = 1; break;
            default: reverse = false; start = 0; break;
        }
        while (start < expr.size() && isspace(expr[start]))
            ++start;
        code = types::parseCodeName(expr.substr(start));
    }
    Item(types::Code code, bool reverse) : code(code), reverse(reverse) {}

    int compare(const Metadata& a, const Metadata& b) const
    {
        int res = types::Type::nullable_compare(a.get(code), b.get(code));
        return reverse ? -res : res;
    }
};

/// Serializer for Item
ostream& operator<<(ostream& out, const Item& i)
{
    if (i.reverse) out << "-";
    out << str::lower(types::formatCode(i.code));
    return out;
}

/**
 * Compare metadata according to sequence of sort items
 */
class Items : public std::vector<Item>, public Compare
{
public:
    Items(const std::string& expr)
    {
        Splitter splitter("[ \t]*,[ \t]*", REG_EXTENDED);
        for (Splitter::const_iterator i = splitter.begin(expr);
                i != splitter.end(); ++i)
            push_back(Item(*i));
        if (empty())
            push_back(Item("reftime"));
    }
    virtual ~Items() {}

    int compare(const Metadata& a, const Metadata& b) const override
    {
        for (const_iterator i = begin(); i != end(); ++i)
        {
            int cmp = i->compare(a, b);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    std::string toString() const override
    {
        return str::join(",", begin(), end());
    }
};

struct IntervalCompare : public Items
{
    Interval m_interval;

    IntervalCompare(Interval interval, const std::string& expr)
        : Items(expr), m_interval(interval) {}

    Interval interval() const override { return m_interval; }

    int compare(const Metadata& a, const Metadata& b) const override
    {
        return Items::compare(a, b);
    }

    std::string toString() const override
    {
        switch (m_interval)
        {
            case NONE: return Items::toString();
            case MINUTE: return "minute:"+Items::toString();
            case HOUR: return "hour:"+Items::toString();
            case DAY: return "day:"+Items::toString();
            case MONTH: return "month:"+Items::toString();
            case YEAR: return "year:"+Items::toString();
            default:
            {
                stringstream ss;
                ss << "cannot format sort expression: interval code " << (int)m_interval << " is not valid";
                throw std::runtime_error(ss.str());
            }
        }
        return Items::toString();
    }
};

static Compare::Interval parseInterval(const std::string& name)
{
    // TODO: convert into something faster, like a hash lookup or a gperf lookup
    string nname = str::lower(str::strip(name));
    if (nname == "minute") return Compare::MINUTE;
    if (nname == "hour") return Compare::HOUR;
    if (nname == "day") return Compare::DAY;
    if (nname == "month") return Compare::MONTH;
    if (nname == "year") return Compare::YEAR;
    throw_consistency_error("parsing interval name", "unsupported interval: " + name + ".  Valid intervals are minute, hour, day, month and year");
}

unique_ptr<Compare> Compare::parse(const std::string& expr)
{
    size_t pos = expr.find(':');
    if (pos == string::npos)
    {
        return unique_ptr<Compare>(new sort::Items(expr));
    } else {
        return unique_ptr<Compare>(new sort::IntervalCompare(sort::parseInterval(expr.substr(0, pos)), expr.substr(pos+1)));
    }
}

void Stream::setEndOfPeriod(const core::Time& time)
{
    int mo = time.mo;
    int da = time.da;
    int ho = time.ho;
    int mi = time.mi;
    int se = time.se;
    switch (sorter.interval())
    {
        case Compare::YEAR: mo = -1; // Falls through
        case Compare::MONTH: da = -1; // Falls through
        case Compare::DAY: ho = -1; // Falls through
        case Compare::HOUR: mi = -1; // Falls through
        case Compare::MINUTE: se = -1; break;
        default:
        {
            stringstream ss;
            ss << "cannot set end of period: interval type has invalid value: " << (int)sorter.interval();
            throw std::runtime_error(ss.str());
        }
    }
    endofperiod.reset(new Time());
    endofperiod->set_upperbound(time.ye, mo, da, ho, mi, se);
}

bool Stream::add(std::shared_ptr<Metadata> m)
{
    if (const Reftime* rt = m->get<Reftime>())
    {
        auto time = rt->get_Position();
        if (hasInterval && (!endofperiod.get() || time > *endofperiod))
        {
            flush();
            buffer.push_back(m);
            setEndOfPeriod(time);
        }
        else
            buffer.push_back(m);
    } else {
        if (hasInterval)
            flush();
        buffer.push_back(m);
    }
    return true;
}

bool Stream::flush()
{
    if (buffer.empty()) return true;
    buffer.sort(sorter);
    return buffer.move_to(next_dest);
}

}
}
}
