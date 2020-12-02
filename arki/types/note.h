#ifndef ARKI_TYPES_NOTE_H
#define ARKI_TYPES_NOTE_H

#include <arki/types/core.h>
#include <arki/core/time.h>

namespace arki {
namespace types {

struct Note;

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
class Note : public CoreType<Note>
{
protected:
    core::Time time;
    std::string content;

public:
    Note(const std::string& content) : time(core::Time::create_now()), content(content) {}
    Note(const core::Time& time, const std::string& content) : time(time), content(content) {}

    void get(core::Time& time, std::string& content) const;

    int compare(const Type& o) const override;
    virtual int compare(const Note& o) const;
    bool equals(const Type& o) const override;

    /// CODEC functions
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    static std::unique_ptr<Note> decode(core::BinaryDecoder& dec);
    static std::unique_ptr<Note> decodeString(const std::string& val);
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;

    Note* clone() const override;

    // Register this type with the type system
    static void init();

    /// Create a note with the current time
    static std::unique_ptr<Note> create(const std::string& content);

    /// Create a note with the given time and content
    static std::unique_ptr<Note> create(const core::Time& time, const std::string& content);
    static std::unique_ptr<Note> decode_structure(const structured::Keys& keys, const structured::Reader& val);
};

}
}
#endif
