#include "formatter.h"
#include "types.h"
#include <sstream>

using namespace arki::types;

namespace arki {

static std::function<std::unique_ptr<Formatter>()> factory;


Formatter::Formatter() {}
Formatter::~Formatter() {}
std::string Formatter::format(const Type& v) const
{
    std::stringstream ss;
    ss << v;
    return ss.str();
}

std::unique_ptr<Formatter> Formatter::create()
{
    if (factory)
        return factory();
    return std::unique_ptr<Formatter>(new Formatter);
}

void Formatter::set_factory(std::function<std::unique_ptr<Formatter>()> new_factory)
{
    factory = new_factory;
}

}
