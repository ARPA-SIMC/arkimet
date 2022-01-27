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

class Run : public Encoded
{
    constexpr static const char* doc = R"(
Time of day when the model was run that generated this data.

This is generally filled when needed from the data reference time.

.. note::
   TODO: [Enrico] I have a vague memory that this was introduced to distinguish
   metadata that would otherwise be the same, and only change according to the
   model run time.

   I would like to document it with an example of when this is needed, but I
   cannot find any at the moment. If no example can be found, it may be time to
   check if this metadata item is still at all needed.
)";

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

    static void write_documentation(stream::Text& out, unsigned heading_level);
};

namespace run {

class Minute : public Run
{
public:
    constexpr static const char* name = "Minute";
    constexpr static const char* doc = R"(
Model run time of day, in minutes from midnight
)";
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
