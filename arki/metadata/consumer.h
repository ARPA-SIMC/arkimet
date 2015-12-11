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

/**
 * Generic interface for metadata observer, used to process a stream of
 * metadata, but leaving their ownership to the caller.
 */
struct Eater
{
    virtual ~Eater() {}
    /**
     * Observer a metadata, without changing it.
     *
     * If the result is true, then the observer is happy to continue observing
     * more metadata. If it's false, then the observer is satisfied and must
     * not be sent any more metadata.
     */
    virtual bool eat(std::unique_ptr<Metadata>&& md) = 0;

    /// Push to the LUA stack a userdata to access this Consumer
    void lua_push(lua_State* L);
};

// Metadata consumer that passes the metadata to a Lua function
struct LuaConsumer : public Eater
{
	lua_State* L;
	int funcid;

	LuaConsumer(lua_State* L, int funcid);
	virtual ~LuaConsumer();
    bool eat(std::unique_ptr<Metadata>&& md) override;

	static std::unique_ptr<LuaConsumer> lua_check(lua_State* L, int idx);
};

}
}
#endif
