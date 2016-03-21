#include "intern.h"

using namespace std;
using namespace arki::types;

namespace arki {
namespace summary {

TypeIntern::TypeIntern()
{
}

TypeIntern::~TypeIntern()
{
}

const types::Type* TypeIntern::lookup(const Type& item) const
{
    return known_items.find(item);
}

const Type* TypeIntern::intern(const Type& item)
{
    const Type* res = known_items.find(item);
    if (res) return res;
    return known_items.insert(item);
}

const types::Type* TypeIntern::intern(std::unique_ptr<types::Type>&& item)
{
    const Type* res = known_items.find(*item);
    if (res) return res;
    return known_items.insert(move(item));
}

}
}
