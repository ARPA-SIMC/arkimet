#include "url.h"
#include "arki/core/binary.h"
#include "arki/structured/emitter.h"
#include "arki/structured/reader.h"
#include "arki/structured/keys.h"
#include "arki/exceptions.h"
#include <ostream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {
namespace source {

Source::Style URL::style() const { return source::Style::URL; }

void URL::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Source::encodeWithoutEnvelope(enc);
    enc.add_varint(url.size());
    enc.add_raw(url);
}

std::ostream& URL::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
			 << format << "," << url
			 << ")";
}
void URL::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Source::serialise_local(e, keys, f);
    e.add(keys.source_url); e.add(url);
}

std::unique_ptr<URL> URL::decode_structure(const structured::Keys& keys, const structured::Reader& reader)
{
    return URL::create(
            reader.as_string(keys.source_format, "source format"),
            reader.as_string(keys.source_url, "source url"));
}

int URL::compare_local(const Source& o) const
{
    if (int res = Source::compare_local(o)) return res;
	// We should be the same kind, so upcast
	const URL* v = dynamic_cast<const URL*>(&o);
	if (!v)
		throw_consistency_error(
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
