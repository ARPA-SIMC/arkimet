#include "formatter.h"
#include "utils/lua.h"
#include "types.h"
#include "formatter/lua.h"
#include <arki/utils/string.h>

using namespace std;
using namespace arki::types;

namespace arki {

static std::function<std::unique_ptr<Formatter>()> factory;


Formatter::Formatter() {}
Formatter::~Formatter() {}
string Formatter::format(const Type& v) const
{
    stringstream ss;
    ss << v;
    return ss.str();
}

unique_ptr<Formatter> Formatter::create()
{
    if (factory)
        return factory();
#if HAVE_LUA
    return unique_ptr<Formatter>(new formatter::Lua);
#else
    return unique_ptr<Formatter>(new Formatter);
#endif
}

void Formatter::set_factory(std::function<std::unique_ptr<Formatter>()> new_factory)
{
    factory = new_factory;
}

}
