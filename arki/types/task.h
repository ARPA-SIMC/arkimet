#ifndef ARKI_TYPES_TASK_H
#define ARKI_TYPES_TASK_H

#include <arki/types/encoded.h>

namespace arki {
namespace types {

template<> struct traits<Task>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;

    typedef unsigned char Style;
};

/**
 * A Task annotation
 */
class Task : public Encoded
{
public:
    using Encoded::Encoded;

    types::Code type_code() const override { return traits<Task>::type_code; }
    size_t serialisationSizeLength() const override { return traits<Task>::type_sersize_bytes; }
    std::string tag() const override { return traits<Task>::type_tag; }

    std::string get() const;

    int compare(const Type& o) const override;

    /// CODEC functions
    static std::unique_ptr<Task> decode(core::BinaryDecoder& dec, bool reuse_buffer);
    static std::unique_ptr<Task> decodeString(const std::string& val);
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;

    Task* clone() const override;

    /// Create a task
    static std::unique_ptr<Task> create(const std::string& value);
    static std::unique_ptr<Task> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    static void write_documentation(stream::Text& out, unsigned heading_level);

    // Register this type tree with the type system
    static void init();
};

}
}
#endif
