#include "core/binary.h"
#include "types.h"
#include "types/utils.h"
#include "utils/sys.h"
#include "utils/string.h"
#include "utils/regexp.h"
#include "structured/emitter.h"
#include "structured/keys.h"
#include "structured/reader.h"
#include "formatter.h"
#include "stream/text.h"

using namespace std;
using namespace arki::utils;
using namespace arki::core;

namespace arki {
namespace types {

Code checkCodeName(const std::string& name)
{
    // TODO: convert into something faster, like a hash lookup or a gperf lookup
    string nname = str::strip(str::lower(name));
	if (nname == "time") return TYPE_TIME;
	if (nname == "origin") return TYPE_ORIGIN;
	if (nname == "product") return TYPE_PRODUCT;
	if (nname == "level") return TYPE_LEVEL;
	if (nname == "timerange") return TYPE_TIMERANGE;
	if (nname == "reftime") return TYPE_REFTIME;
	if (nname == "note") return TYPE_NOTE;
	if (nname == "source") return TYPE_SOURCE;
	if (nname == "assigneddataset") return TYPE_ASSIGNEDDATASET;
	if (nname == "area") return TYPE_AREA;
	if (nname == "proddef") return TYPE_PRODDEF;
	if (nname == "summaryitem") return TYPE_SUMMARYITEM;
	if (nname == "summarystats") return TYPE_SUMMARYSTATS;
	if (nname == "bbox") return TYPE_BBOX;
	if (nname == "run") return TYPE_RUN;
	if (nname == "task") 		return TYPE_TASK;
	if (nname == "quantity") 	return TYPE_QUANTITY;
	if (nname == "value") 	return TYPE_VALUE;
	return TYPE_INVALID;
}

Code parseCodeName(const std::string& name)
{
    Code res = checkCodeName(name);
    if (res == TYPE_INVALID)
    {
        string msg("cannot parse yaml data: unsupported field type: ");
        msg += name;
        throw std::runtime_error(std::move(msg));
    }
    return res;
}

std::string formatCode(const Code& c)
{
	switch (c)
	{
		case TYPE_ORIGIN: return "ORIGIN";
		case TYPE_PRODUCT: return "PRODUCT";
		case TYPE_LEVEL: return "LEVEL";
		case TYPE_TIMERANGE: return "TIMERANGE";
		case TYPE_REFTIME: return "REFTIME";
		case TYPE_NOTE: return "NOTE";
		case TYPE_SOURCE: return "SOURCE";
		case TYPE_ASSIGNEDDATASET: return "ASSIGNEDDATASET";
		case TYPE_AREA: return "AREA";
		case TYPE_PRODDEF: return "PRODDEF";
		case TYPE_SUMMARYITEM: return "SUMMARYITEM";
		case TYPE_SUMMARYSTATS: return "SUMMARYSTATS";
		case TYPE_BBOX: return "BBOX";
		case TYPE_RUN: return "RUN";
		case TYPE_TASK:	return "TASK";
		case TYPE_QUANTITY:	return "QUANTITY";
		case TYPE_VALUE:	return "VALUE";
        default: {
            stringstream res;
            res << "unknown(" << (int)c << ")";
            return res.str();
        }
    }
}

std::set<types::Code> parse_code_names(const std::string& names)
{
    std::set<types::Code> res;
    Splitter splitter("[ \t]*,[ \t]*", REG_EXTENDED);
    for (Splitter::const_iterator i = splitter.begin(str::lower(names));
            i != splitter.end(); ++i)
    {
        res.insert(types::parseCodeName(*i));
    }
    return res;
}

int Type::compare(const Type& o) const
{
	return type_code() - o.type_code();
}

bool Type::operator==(const std::string& o) const
{
    // Temporarily decode it for comparison
    std::unique_ptr<Type> other = decodeString(type_code(), o);
    return operator==(*other);
}

std::string Type::to_string() const
{
    stringstream ss;
    writeToOstream(ss);
    return ss.str();
}

void Type::encode_for_indexing(core::BinaryEncoder& enc) const
{
    encodeWithoutEnvelope(enc);
}

void Type::encodeBinary(core::BinaryEncoder& enc) const
{
    vector<uint8_t> contents;
    contents.reserve(256);
    core::BinaryEncoder contentsenc(contents);
    encodeWithoutEnvelope(contentsenc);

    enc.add_varint((unsigned)type_code());
    enc.add_varint(contents.size());
    enc.add_raw(contents);
}

std::vector<uint8_t> Type::encodeBinary() const
{
    vector<uint8_t> contents;
    core::BinaryEncoder enc(contents);
    encodeBinary(enc);
    return contents;
}

void Type::serialise(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.start_mapping();
    e.add(keys.type_name, tag());
    if (f) e.add(keys.type_desc, f->format(*this));
    serialise_local(e, keys, f);
    e.end_mapping();
}

std::string Type::exactQuery() const
{
    stringstream ss;
    ss << "creating a query to match " << tag() << " is not implemented";
    return ss.str();
}

unique_ptr<Type> Type::decode(core::BinaryDecoder& dec)
{
    types::Code code;
    core::BinaryDecoder inner = dec.pop_type_envelope(code);
    return decodeInner(code, inner);
}

unique_ptr<Type> Type::decodeInner(types::Code code, core::BinaryDecoder& dec)
{
    return std::unique_ptr<Type>(types::MetadataType::get(code)->decode_func(dec, false));
}

unique_ptr<Type> Type::decode_inner(types::Code code, core::BinaryDecoder& dec, bool reuse_buffer)
{
    return std::unique_ptr<Type>(types::MetadataType::get(code)->decode_func(dec, reuse_buffer));
}

unique_ptr<Type> decodeString(types::Code code, const std::string& val)
{
    return std::unique_ptr<Type>(types::MetadataType::get(code)->string_decode_func(val));
}

std::unique_ptr<Type> decode_structure(const structured::Keys& keys, const structured::Reader& reader)
{
    const std::string& type = reader.as_string(keys.type_name, "item type");
    return decode_structure(keys, parseCodeName(type), reader);
}

std::unique_ptr<Type> decode_structure(const structured::Keys& keys, types::Code code, const structured::Reader& reader)
{
    return std::unique_ptr<Type>(types::MetadataType::get(code)->structure_decode_func(keys, reader));
}

std::string tag(types::Code code)
{
    return types::MetadataType::get(code)->tag;
}

void Type::document(stream::Text& out)
{
    out.rst_header("Metadata types");

    MetadataType::document_types(out, 2);
}

}
}
