#include "config.h"

#include <arki/types.h>
#include <arki/types/utils.h>
#include <arki/utils/string.h>
#include <cstring>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

// Registry of item information
static const size_t decoders_size = 1024;
static const MetadataType** decoders = 0;

MetadataType::MetadataType(
        types::Code type_code,
        int serialisationSizeLen,
        const std::string& tag,
        item_decoder decode_func,
        string_decoder string_decode_func,
        structure_decoder structure_decode_func,
        lua_libloader lua_loadlib_func)
    : type_code(type_code),
      serialisationSizeLen(serialisationSizeLen),
      tag(tag),
      decode_func(decode_func),
      string_decode_func(string_decode_func),
      structure_decode_func(structure_decode_func),
      lua_loadlib_func(lua_loadlib_func)
{
}

MetadataType::~MetadataType()
{
	if (!decoders)
		return;
	
	decoders[type_code] = 0;
}

void MetadataType::register_type(MetadataType* type)
{
    // Ensure that the map is created before we add items to it
    if (!decoders)
    {
        decoders = new const MetadataType*[decoders_size];
        memset(decoders, 0, decoders_size * sizeof(const MetadataType*));
    }

    decoders[type->type_code] = type;
}

const MetadataType* MetadataType::get(types::Code code)
{
    if (!decoders || decoders[code] == 0)
    {
        stringstream ss;
        ss << "cannot parse binary data: no decoder found for item type " << code;
        throw std::runtime_error(ss.str());
    }
    return decoders[code];
}

void MetadataType::lua_loadlib(lua_State* L)
{
	for (size_t i = 0; i < decoders_size; ++i)
		if (decoders[i] != 0)
			decoders[i]->lua_loadlib_func(L);
}

void split(const std::string& str, std::set<std::string>& result, const std::string& delimiters)
{
    std::string::size_type lastPos = str.find_first_not_of(delimiters, 0);      // Skip delimiters at beginning.
    std::string::size_type pos     = str.find_first_of(delimiters, lastPos);    // Find first "non-delimiter".
    while (std::string::npos != pos || std::string::npos != lastPos)
    {
        result.insert(str::strip(str.substr(lastPos, pos - lastPos)));
        lastPos = str.find_first_not_of(delimiters, pos);   // Skip delimiters.  Note the "not_of"
        pos = str.find_first_of(delimiters, lastPos);       // Find next "non-delimiter"
    }
}

void split(const std::string& str, std::vector<std::string>& result, const std::string& delimiters)
{
    std::string::size_type lastPos = str.find_first_not_of(delimiters, 0);      // Skip delimiters at beginning.
    std::string::size_type pos     = str.find_first_of(delimiters, lastPos);    // Find first "non-delimiter".
    while (std::string::npos != pos || std::string::npos != lastPos)
    {
        result.push_back(str::strip(str.substr(lastPos, pos - lastPos)));
        lastPos = str.find_first_not_of(delimiters, pos);   // Skip delimiters.  Note the "not_of"
        pos = str.find_first_of(delimiters, lastPos);       // Find next "non-delimiter"
    }
}

}
}
