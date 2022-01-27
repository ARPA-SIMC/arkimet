#ifndef ARKI_TYPES_NOTE_H
#define ARKI_TYPES_NOTE_H

#include <arki/types/encoded.h>
#include <arki/core/time.h>

namespace arki {
namespace types {

template<>
struct traits<Note>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;
};

/**
 * A metadata annotation
 */
class Note : public Encoded
{
    constexpr static const char* doc = R"(
A timestamped annotation about how arkimet processed this element.

This can be used to track and audit the life of data in the archive.

It can be used for consultation only, and cannot be used in searches.
)";

public:
    using Encoded::Encoded;

    types::Code type_code() const override { return traits<Note>::type_code; }
    size_t serialisationSizeLength() const override { return traits<Note>::type_sersize_bytes; }
    std::string tag() const override { return traits<Note>::type_tag; }

    Note* clone() const override { return new Note(data, size); }

    int compare(const Type& o) const override;

    void get(core::Time& time, std::string& content) const;

    /// CODEC functions
    static std::unique_ptr<Note> decode(core::BinaryDecoder& dec, bool reuse_buffer);
    static std::unique_ptr<Note> decodeString(const std::string& val);
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;

    // Register this type with the type system
    static void init();

    /// Create a note with the current time
    static std::unique_ptr<Note> create(const std::string& content);

    /// Create a note with the given time and content
    static std::unique_ptr<Note> create(const core::Time& time, const std::string& content);
    static std::unique_ptr<Note> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    static void write_documentation(stream::Text& out, unsigned heading_level);
};

}
}
#endif
