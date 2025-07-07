#ifndef ARKI_TYPES_ASSIGNEDDATASET_H
#define ARKI_TYPES_ASSIGNEDDATASET_H

#include <arki/core/time.h>
#include <arki/types/encoded.h>

namespace arki {
namespace types {

struct AssignedDataset;

template <> struct traits<AssignedDataset>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;
};

/**
 * Annotate a metadata with the dataset name it's in.
 *
 * This, like BBox, is deprecated and only maintained in order to parse old
 * Metadata which might have included it.
 */
struct AssignedDataset : public Encoded
{
protected:
    void get(core::Time& changed, std::string& name, std::string& id) const;

    /// Create a attributed dataset definition with the current time
    static std::unique_ptr<AssignedDataset> create(const std::string& name,
                                                   const std::string& id);

    /// Create a attributed dataset definition with the givem time
    static std::unique_ptr<AssignedDataset> create(const core::Time& time,
                                                   const std::string& name,
                                                   const std::string& id);

public:
    using Encoded::Encoded;

    types::Code type_code() const override
    {
        return traits<AssignedDataset>::type_code;
    }
    size_t serialisationSizeLength() const override
    {
        return traits<AssignedDataset>::type_sersize_bytes;
    }
    std::string tag() const override
    {
        return traits<AssignedDataset>::type_tag;
    }

    AssignedDataset* clone() const override
    {
        return new AssignedDataset(data, size);
    }

    bool equals(const Type& o) const override;
    int compare(const Type& o) const override;

    /// CODEC functions
    static std::unique_ptr<AssignedDataset> decode(core::BinaryDecoder& dec,
                                                   bool reuse_buffer);
    static std::unique_ptr<AssignedDataset>
    decodeString(const std::string& val);
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys,
                         const Formatter* f = 0) const override;

    static void write_documentation(stream::Text& out, unsigned heading_level)
    {
    }

    // Register this type with the type system
    static void init();

    static std::unique_ptr<AssignedDataset>
    decode_structure(const structured::Keys& keys,
                     const structured::Reader& val);
};

} // namespace types
} // namespace arki
#endif
