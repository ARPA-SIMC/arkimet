#ifndef ARKI_FORMATTER_LUA_H
#define ARKI_FORMATTER_LUA_H

#include <string>
#include <arki/formatter.h>

namespace arki {
struct Lua;
}

namespace arki {
namespace formatter {

class Lua : public Formatter
{
	template<typename T>
	std::string invoke(const char* func, const char* type, const T& v) const;

protected:
	mutable arki::Lua *L;

public:
	Lua();
	virtual ~Lua();

    std::string format(const types::Type& v) const override;
};

}
}

#endif
