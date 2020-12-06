#ifndef ARKI_TYPES_RUN_H
#define ARKI_TYPES_RUN_H

#include <arki/types/encoded.h>

namespace arki {
namespace types {

namespace run {

/// Style values
enum class Style: unsigned char {
    MINUTE = 1,
};

}

template<>
struct traits<Run>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;

    typedef run::Style Style;
};

/**
 * The run of some data.
 *
 * It can contain information like centre, process, subcentre, subprocess and
 * other similar data.
 */
class Run : public Encoded
{
public:
    using Encoded::Encoded;

    ~Run();

    typedef run::Style Style;

    types::Code type_code() const override { return traits<Run>::type_code; }
    size_t serialisationSizeLength() const override { return traits<Run>::type_sersize_bytes; }
    std::string tag() const override { return traits<Run>::type_tag; }

    Run* clone() const override = 0;

    int compare(const Type& o) const override;

    static run::Style style(const uint8_t* data, unsigned size);
    static unsigned get_Minute(const uint8_t* data, unsigned size);

    run::Style style() const { return style(data, size); }
    unsigned get_Minute() const { return get_Minute(data, size); }

    /// Convert a string into a style
    static Style parseStyle(const std::string& str);
    /// Convert a style into its string representation
    static std::string formatStyle(Style s);

    /// CODEC functions
    static std::unique_ptr<Run> decode(core::BinaryDecoder& dec, bool reuse_buffer);
    static std::unique_ptr<Run> decodeString(const std::string& val);
    static std::unique_ptr<Run> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    // Register this type tree with the type system
    static void init();
    static std::unique_ptr<Run> createMinute(unsigned int hour, unsigned int minute=0);
};

namespace run {

class Minute : public Run
{
public:
    using Run::Run;

    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    int compare_local(const Minute& o) const;

    Minute* clone() const override { return new Minute(data, size); }
};

}
}
}
#endif
