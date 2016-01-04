#ifndef ARKI_METADATA_CONSUMER_H
#define ARKI_METADATA_CONSUMER_H

/// Metadata consumer interface and tools

#include <memory>
#include <string>
#include <iosfwd>
#include <arki/defs.h>

struct lua_State;

namespace arki {
class Metadata;

namespace metadata {

// Metadata consumer that passes the metadata to a Lua function
struct LuaConsumer
{
    lua_State* L;
    int funcid;

    LuaConsumer(lua_State* L, int funcid);
    virtual ~LuaConsumer();
    bool eat(std::unique_ptr<Metadata>&& md);

    static std::unique_ptr<LuaConsumer> lua_check(lua_State* L, int idx);
};

}
}
#endif
