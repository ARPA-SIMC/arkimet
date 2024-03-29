#include "arki/dataset/index/base.h"
#include "arki/utils/string.h"
#include "arki/utils/regexp.h"
#include "arki/types.h"
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::dataset::index;

namespace arki {
namespace dataset {
namespace index {

std::set<types::Code> parseMetadataBitmask(const std::string& components)
{
    set<types::Code> res;
    Splitter splitter("[ \t]*,[ \t]*", REG_EXTENDED);
    for (Splitter::const_iterator i = splitter.begin(str::lower(components));
            i != splitter.end(); ++i)
    {
        res.insert(types::parseCodeName(*i));
    }
    return res;
}

std::string fmtin(const std::vector<int>& vals)
{
    if (vals.empty())
        throw NotFound();

    stringstream res;
    if (vals.size() == 1)
        res << "=" << *vals.begin();
    else
    {
        res << "IN(";
        for (vector<int>::const_iterator i = vals.begin();
                i != vals.end(); ++i)
            if (i == vals.begin())
                res << *i;
            else
                res << "," << *i;
        res << ")";
    }
    return res.str();
}

}
}
}
