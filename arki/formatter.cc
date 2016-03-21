#include "formatter.h"
#include "utils/lua.h"
#include "types.h"
#include "formatter/lua.h"
#include <arki/utils/string.h>

using namespace std;
using namespace arki::types;

namespace arki {

Formatter::Formatter() {}
Formatter::~Formatter() {}
string Formatter::operator()(const Type& v) const
{
    stringstream ss;
    ss << v;
    return ss.str();
}

unique_ptr<Formatter> Formatter::create()
{
#if HAVE_LUA
    return unique_ptr<Formatter>(new formatter::Lua);
#else
    return unique_ptr<Formatter>(new Formatter);
#endif
}

}
