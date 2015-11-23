#include "typeset.h"

using namespace std;
using namespace wibble;
using namespace arki::types;

namespace arki {
namespace types {

TypeSet::TypeSet() {}

TypeSet::TypeSet(const TypeSet& o)
{
    for (const_iterator i = o.begin(); i != o.end(); ++i)
        vals.insert(*i ? (*i)->clone() : 0);
}

TypeSet::~TypeSet()
{
    for (container::iterator i = vals.begin(); i != vals.end(); ++i)
        delete *i;
}

const Type* TypeSet::insert(const Type& val)
{
    return insert(val.cloneType());
}

const Type* TypeSet::insert(std::unique_ptr<Type>&& val)
{
    pair<container::iterator, bool> res = vals.insert(val.get());
    if (res.second) val.release();
    return *res.first;
}

const Type* TypeSet::find(const Type& val) const
{
    const_iterator i = vals.find(&val);
    if (i != vals.end()) return *i;
    return 0;
}

bool TypeSet::erase(const Type& val)
{
    return vals.erase(&val) > 0;
}

bool TypeSet::operator==(const TypeSet& o) const
{
    if (size() != o.size()) return false;
    const_iterator a = begin();
    const_iterator b = o.begin();
    while (a != end() && b != o.end())
        if (**a != **b)
            return false;
    return true;
}

}
}
