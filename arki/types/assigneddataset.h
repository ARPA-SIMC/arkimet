#ifndef ARKI_TYPES_ASSIGNEDDATASET_H
#define ARKI_TYPES_ASSIGNEDDATASET_H

#include <arki/types/core.h>
#include <arki/core/time.h>

namespace arki {
namespace types {


struct AssignedDataset;

template<>
struct traits<AssignedDataset>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;
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
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    static std::unique_ptr<AssignedDataset> decode(core::BinaryDecoder& dec);
    static std::unique_ptr<AssignedDataset> decodeString(const std::string& val);
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;

    AssignedDataset* clone() const override;

    // Register this type with the type system
    static void init();

    /// Create a attributed dataset definition with the current time
    static std::unique_ptr<AssignedDataset> create(const std::string& name, const std::string& id);

    /// Create a attributed dataset definition with the givem time
    static std::unique_ptr<AssignedDataset> create(const core::Time& time, const std::string& name, const std::string& id);

    static std::unique_ptr<AssignedDataset> decode_structure(const structured::Keys& keys, const structured::Reader& val);
};

}
}
#endif
