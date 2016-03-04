#ifndef ARKI_TYPES_ASSIGNEDDATASET_H
#define ARKI_TYPES_ASSIGNEDDATASET_H

#include <arki/types.h>
#include <arki/core/time.h>

struct lua_State;

namespace arki {
namespace types {


struct AssignedDataset;

template<>
struct traits<AssignedDataset>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;
    static const char* type_lua_tag;

    typedef unsigned char Style;
};

/**
 * A metadata annotation
 */
struct AssignedDataset : public types::CoreType<AssignedDataset>
{
    core::Time changed;
    std::string name;
    std::string id;

    AssignedDataset(const core::Time& changed, const std::string& name, const std::string& id)
        : changed(changed), name(name), id(id) {}

    int compare(const Type& o) const override;
    bool equals(const Type& o) const override;

    /// CODEC functions
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    static std::unique_ptr<AssignedDataset> decode(BinaryDecoder& dec);
    static std::unique_ptr<AssignedDataset> decodeString(const std::string& val);
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;

    // Lua functions
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    AssignedDataset* clone() const override;

    /// Create a attributed dataset definition with the current time
    static std::unique_ptr<AssignedDataset> create(const std::string& name, const std::string& id);

    /// Create a attributed dataset definition with the givem time
    static std::unique_ptr<AssignedDataset> create(const core::Time& time, const std::string& name, const std::string& id);

    static std::unique_ptr<AssignedDataset> decodeMapping(const emitter::memory::Mapping& val);
};

}
}
#endif
