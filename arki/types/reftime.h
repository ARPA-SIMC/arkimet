#ifndef ARKI_TYPES_REFTIME_H
#define ARKI_TYPES_REFTIME_H

#include <arki/types/styled.h>
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
struct Reftime : public StyledType<Reftime>
{
	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

    /// CODEC functions
    static std::unique_ptr<Reftime> decode(core::BinaryDecoder& dec);
    static std::unique_ptr<Reftime> decodeString(const std::string& val);
    static std::unique_ptr<Reftime> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    /// Beginning of the period in this Reftime
    virtual const core::Time& period_begin() const = 0;
    /// End of the period in this Reftime
    virtual const core::Time& period_end() const = 0;

    /**
     * Expand a datetime range, returning the new range endpoints in begin
     * and end.
     *
     * A NULL unique_ptr signifies the initial state of an invalid range, and
     * both begin and end will be set to non-NULL as soon as the first
     * expand_date_range is called on them.
     */
    virtual void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) const = 0;

    /**
     * Expand a datetime range, returning the new range endpoints in begin
     * and end.
     *
     * begin and end are assumed to be valid times.
     */
    virtual void expand_date_range(core::Time& begin, core::Time& end) const = 0;

    // Register this type tree with the type system
    static void init();

    /// If begin == end create a Position reftime, else create a Period reftime
    static std::unique_ptr<Reftime> create(const core::Time& begin, const core::Time& end);
    static std::unique_ptr<Reftime> createPosition(const core::Time& position);
    static std::unique_ptr<Reftime> createPeriod(const core::Time& begin, const core::Time& end);
};

namespace reftime {

inline std::ostream& operator<<(std::ostream& o, Style s) { return o << Reftime::formatStyle(s); }


struct Position : public Reftime
{
    core::Time time;

    Position(const core::Time& time);

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    int compare_local(const Reftime& o) const override;
    bool equals(const Type& o) const override;

    const core::Time& period_begin() const override { return time; }
    const core::Time& period_end() const override { return time; }

    Position* clone() const override;

    void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) const override;
    void expand_date_range(core::Time& begin, core::Time& end) const override;

    static std::unique_ptr<Position> create(const core::Time& position);
    static std::unique_ptr<Position> decode_structure(const structured::Keys& keys, const structured::Reader& val);
};

struct Period : public Reftime
{
    core::Time begin;
    core::Time end;

    Period(const core::Time& begin, const core::Time& end);

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;

    int compare_local(const Reftime& o) const override;
    bool equals(const Type& o) const override;

    const core::Time& period_begin() const override { return begin; }
    const core::Time& period_end() const override { return end; }

    Period* clone() const override;

    void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) const override;
    void expand_date_range(core::Time& begin, core::Time& end) const override;

    static std::unique_ptr<Period> create(const core::Time& begin, const core::Time& end);
    static std::unique_ptr<Period> decode_structure(const structured::Keys& keys, const structured::Reader& val);
};

}

}
}

#endif
