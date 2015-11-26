#include "config.h"
#include <arki/dataset/index/base.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/utils/string.h>
#include <arki/wibble/regexp.h>
#include <sstream>
#include <ctime>

// FIXME: for debugging
//#include <iostream>

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
    wibble::Splitter splitter("[ \t]*,[ \t]*", REG_EXTENDED);
    for (wibble::Splitter::const_iterator i = splitter.begin(str::lower(components));
            i != splitter.end(); ++i)
    {
        res.insert(types::parseCodeName(*i));
    }
    return res;
}

struct Adder
{
	string res;

	template<typename VAL>
	void add(const VAL& val, const char* name)
	{
		switch (val.size())
		{
			case 0: break;
			case 1: res += val.begin()->encode(); break;
			default:
					std::stringstream err;
					err << "can only handle metadata with 1 " << name << " (" << val.size() << " found)";
					throw wibble::exception::Consistency("generating ID", err.str());
		}
	}
};

std::string IDMaker::id(const Metadata& md) const
{
    string res;
    for (set<types::Code>::const_iterator i = components.begin(); i != components.end(); ++i)
        if (const Type* t = md.get(*i))
            res += t->encodeBinary();
    return str::encode_base64(res);
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
// vim:set ts=4 sw=4:
