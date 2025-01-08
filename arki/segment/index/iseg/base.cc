#include "base.h"
#include "arki/utils/string.h"
#include "arki/types.h"
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

namespace arki::segment::index::iseg {

std::string fmtin(const std::vector<int>& vals)
{
    if (vals.empty())
        throw NotFound();

    std::stringstream res;
    if (vals.size() == 1)
        res << "=" << *vals.begin();
    else
    {
        res << "IN(";
        for (std::vector<int>::const_iterator i = vals.begin();
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
