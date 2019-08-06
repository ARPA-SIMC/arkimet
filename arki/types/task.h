#ifndef ARKI_TYPES_TASK_H
#define ARKI_TYPES_TASK_H

#include <arki/types/core.h>

struct lua_State;

namespace arki {
namespace types {

template<> struct traits<Task>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/**
 * A Task annotation
 */
struct Task : public CoreType<Task>
{
	std::string task;

	Task(const std::string& value) : task(value) {}

    int compare(const Type& o) const override;
    bool equals(const Type& o) const override;

    /// CODEC functions
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    static std::unique_ptr<Task> decode(BinaryDecoder& dec);
    static std::unique_ptr<Task> decodeString(const std::string& val);
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(Emitter& e, const emitter::Keys& keys, const Formatter* f=0) const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    Task* clone() const override;

    /// Create a task
    static std::unique_ptr<Task> create(const std::string& value);
    static std::unique_ptr<Task> decodeMapping(const emitter::memory::Mapping& val);
    static std::unique_ptr<Task> decode_structure(const emitter::Keys& keys, const emitter::Reader& val);

	static void lua_loadlib(lua_State* L);

    // Register this type tree with the type system
    static void init();
};

}
}
#endif



















