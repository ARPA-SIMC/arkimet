#ifndef ARKI_TYPES_SOURCE_URL_H
#define ARKI_TYPES_SOURCE_URL_H

#include <arki/types/source.h>

namespace arki {
namespace types {
namespace source {

struct URL : public Source
{
    std::string url;

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Source& o) const override;
    bool equals(const Type& o) const override;
    URL* clone() const override;

    static std::unique_ptr<URL> create(const std::string& format, const std::string& url);
    static std::unique_ptr<URL> decode_structure(const structured::Keys& keys, const structured::Reader& reader);
};


}
}
}
#endif
