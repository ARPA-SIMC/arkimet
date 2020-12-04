#ifndef ARKI_TYPES_REFTIME_H
#define ARKI_TYPES_REFTIME_H

#include <arki/types/encoded.h>
#include <arki/core/time.h>

namespace arki {
namespace types {
namespace reftime {

enum class Style: unsigned char {
    POSITION = 1,
    PERIOD = 2,
};

}

template<>
struct traits<Reftime>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;

    typedef reftime::Style Style;
};

template<> struct traits<reftime::Position> : public traits<Reftime> {};
template<> struct traits<reftime::Period> : public traits<Reftime> {};

/**
 * The vertical reftime or layer of some data
 *
 * It can contain information like reftimetype and reftime value.
 */
struct Reftime : public Encoded
{
    using Encoded::Encoded;

    typedef reftime::Style Style;

    types::Code type_code() const override { return traits<Reftime>::type_code; }
    size_t serialisationSizeLength() const override { return traits<Reftime>::type_sersize_bytes; }
    std::string tag() const override { return traits<Reftime>::type_tag; }

    Reftime* clone() const override = 0;

    int compare(const Type& o) const override;

    static reftime::Style style(const uint8_t* data, unsigned size);
    static core::Time get_Position(const uint8_t* data, unsigned size);
    static void get_Period(const uint8_t* data, unsigned size, core::Time& begin, core::Time& end);

    reftime::Style style() const { return style(data, size); }
    core::Time get_Position() const { return get_Position(data, size); }
    void get_Period(core::Time& begin, core::Time& end) const { return get_Period(data, size, begin, end); }

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

    /// CODEC functions
    static std::unique_ptr<Reftime> decode(core::BinaryDecoder& dec, bool reuse_buffer);
    static std::unique_ptr<Reftime> decodeString(const std::string& val);
    static std::unique_ptr<Reftime> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    /**
     * Expand a datetime range, returning the new range endpoints in begin
     * and end.
     */
    virtual void expand_date_range(core::Interval& interval) const = 0;

    // Register this type tree with the type system
    static void init();

    /// If begin == end create a Position reftime, else create a Period reftime
    static std::unique_ptr<Reftime> createPosition(const core::Time& position);
};

namespace reftime {

inline std::ostream& operator<<(std::ostream& o, Style s) { return o << Reftime::formatStyle(s); }


class Position : public Reftime
{
public:
    using Reftime::Reftime;

    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    Position* clone() const override { return new Position(data, size); }

    void expand_date_range(core::Interval& interval) const override;
};

}

}
}

#endif
