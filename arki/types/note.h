#ifndef ARKI_TYPES_NOTE_H
#define ARKI_TYPES_NOTE_H

#include <arki/types.h>
#include <arki/types/time.h>

struct lua_State;

namespace arki {
namespace types {

struct Note;

template<>
struct traits<Note>
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
struct Note : public CoreType<Note>
{
    Time time;
    std::string content;

    Note(const std::string& content) : time(Time::createNow()), content(content) {}
    Note(const Time& time, const std::string& content) : time(time), content(content) {}

    int compare(const Type& o) const override;
    virtual int compare(const Note& o) const;
    bool equals(const Type& o) const override;

    /// CODEC functions
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    static std::unique_ptr<Note> decode(BinaryDecoder& dec);
    static std::unique_ptr<Note> decodeString(const std::string& val);
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    Note* clone() const override;

    /// Create a note with the current time
    static std::unique_ptr<Note> create(const std::string& content);

    /// Create a note with the given time and content
    static std::unique_ptr<Note> create(const Time& time, const std::string& content);
    static std::unique_ptr<Note> decodeMapping(const emitter::memory::Mapping& val);
};

}
}
#endif
