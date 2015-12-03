#include "url.h"
#include <arki/utils/codec.h>
#include <arki/utils/lua.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;

namespace arki {
namespace types {
namespace source {

Source::Style URL::style() const { return Source::URL; }

void URL::encodeWithoutEnvelope(Encoder& enc) const
{
	Source::encodeWithoutEnvelope(enc);
	enc.addVarint(url.size());
	enc.addString(url);
}

std::ostream& URL::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
			 << format << "," << url
			 << ")";
}
void URL::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Source::serialiseLocal(e, f);
    e.add("url"); e.add(url);
}
std::unique_ptr<URL> URL::decodeMapping(const emitter::memory::Mapping& val)
{
    return URL::create(
            val["f"].want_string("parsing url source format"),
            val["url"].want_string("parsing url source url"));
}

const char* URL::lua_type_name() const { return "arki.types.source.url"; }

#ifdef HAVE_LUA
bool URL::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "url")
		lua_pushlstring(L, url.data(), url.size());
	else
		return Source::lua_lookup(L, name);
	return true;
}
#endif

int URL::compare_local(const Source& o) const
{
    if (int res = Source::compare_local(o)) return res;
	// We should be the same kind, so upcast
	const URL* v = dynamic_cast<const URL*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a URL Source, but is a ") + typeid(&o).name() + " instead");

	if (url < v->url) return -1;
	if (url > v->url) return 1;
	return 0;
}
bool URL::equals(const Type& o) const
{
	const URL* v = dynamic_cast<const URL*>(&o);
	if (!v) return false;
	return format == v->format && url == v->url;
}

URL* URL::clone() const
{
    return new URL(*this);
}

std::unique_ptr<URL> URL::create(const std::string& format, const std::string& url)
{
    URL* res = new URL;
    res->format = format;
    res->url = url;
    return unique_ptr<URL>(res);
}

}
}
}
