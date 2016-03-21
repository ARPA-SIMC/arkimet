#ifndef ARKI_TYPES_UTILS_H
#define ARKI_TYPES_UTILS_H

#include <arki/types.h>
#include <stdexcept>
#include <string>
#include <sstream>
#include <set>
#include <cstring>

#include <arki/libconfig.h>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define LUATAG_TYPES "arki.types"

struct lua_State;

namespace arki {
struct BinaryDecoder;

namespace emitter {
namespace memory {
struct Mapping;
}
}

namespace types {

template<class F>
struct is_item_decoder;

template<class R, class... Args>
struct is_item_decoder<R(Args...)>
{
    static constexpr bool value = false;
};

template<class R>
struct is_item_decoder<R(BinaryDecoder&)>
{
    static constexpr bool value = true;
};

/**
 * This class is used to register types with the arkimet metadata type system.
 *
 * Registration is done by declaring a static RegisterItem object, passing the
 * metadata details in the constructor.
 */
struct MetadataType
{
    typedef std::unique_ptr<Type> (*item_decoder)(BinaryDecoder& dec);
    typedef std::unique_ptr<Type> (*string_decoder)(const std::string& val);
    typedef std::unique_ptr<Type> (*mapping_decoder)(const emitter::memory::Mapping& val);
    typedef void (*lua_libloader)(lua_State* L);
    typedef void (*intern_stats)();

	types::Code type_code;
	int serialisationSizeLen;
	std::string tag;
	item_decoder decode_func;
	string_decoder string_decode_func;
	mapping_decoder mapping_decode_func;
	lua_libloader lua_loadlib_func;
	intern_stats intern_stats_func;
	
	MetadataType(
		types::Code type_code,
		int serialisationSizeLen,
		const std::string& tag,
		item_decoder decode_func,
		string_decoder string_decode_func,
		mapping_decoder mapping_decode_func,
		lua_libloader lua_loadlib_func,
		intern_stats intern_stats_func = 0
		);
	~MetadataType();

	// Get information about the given metadata
	static const MetadataType* get(types::Code);

    template<typename T>
    static MetadataType create(intern_stats intern_stats_func = 0)
    {
        return MetadataType(
            traits<T>::type_code,
            traits<T>::type_sersize_bytes,
            traits<T>::type_tag,
            (MetadataType::item_decoder)T::decode,
            (MetadataType::string_decoder)T::decodeString,
            (MetadataType::mapping_decoder)T::decodeMapping,
            T::lua_loadlib,
            intern_stats_func
        );
    }

    template<typename T>
    static void register_type(intern_stats intern_stats_func = 0)
    {
        static_assert(is_item_decoder<decltype(T::decode)>::value, "decode function must take a BinaryDecoder as argument");
        // FIXME: when we remove create() we can make MetadataType not register
        // itself and remove the need of this awkward new
        new MetadataType(
            traits<T>::type_code,
            traits<T>::type_sersize_bytes,
            traits<T>::type_tag,
            (MetadataType::item_decoder)T::decode,
            (MetadataType::string_decoder)T::decodeString,
            (MetadataType::mapping_decoder)T::decodeMapping,
            T::lua_loadlib,
            intern_stats_func
        );
    }

	static void lua_loadlib(lua_State* L);
};

void debug_intern_stats();

// Parse the outer style of a TYPE(val1, val2...) string
template<typename T>
static typename T::Style outerParse(const std::string& str, std::string& inner)
{
    if (str.empty())
    {
        std::string msg("cannot parse ");
        msg += typeid(T).name();
        msg += ": string is empty";
        throw std::runtime_error(std::move(msg));
    }
    size_t pos = str.find('(');
    if (pos == std::string::npos)
    {
        std::string msg("cannot parse ");
        msg += typeid(T).name();
        msg += ": no open parenthesis found in '";
        msg += str;
        msg += "'";
        throw std::runtime_error(std::move(msg));
    }
    if (str[str.size() - 1] != ')')
    {
        std::string msg("cannot parse ");
        msg += typeid(T).name();
        msg += ": string '";
        msg += str;
        msg += "' does not end with closed parenthesis";
        throw std::runtime_error(std::move(msg));
    }
    inner = str.substr(pos+1, str.size() - pos - 2);
    return T::parseStyle(str.substr(0, pos));
}

// Parse a list of numbers of the given size
template<int SIZE, int REQUIRED=SIZE>
struct NumberList
{
	int vals[SIZE];
    unsigned found;
	std::string tail;

	unsigned size() const { return SIZE; }

	NumberList(const std::string& str, const std::string& what, bool has_tail=false)
        : found(0)
	{
		const char* start = str.c_str();
		for (unsigned i = 0; i < SIZE; ++i)
		{
			// Skip colons and spaces, if any
			while (*start && (::isspace(*start) || *start == ','))
				++start;
            if (!*start)
            {
                if (found < REQUIRED)
                {
                    std::stringstream ss;
                    ss << "cannot parse " << what << ": found " << i << " values instead of " << SIZE;
                    throw std::runtime_error(ss.str());
                }
                else
                    break;
            }

            // Parse the number
            char* endptr;
            vals[i] = ::strtol(start, &endptr, 10);
            if (endptr == start)
            {
                std::string msg("cannot parse ");
                msg += what;
                msg += ": expected a number, but found \"";
                msg += start;
                msg += '"';
                throw std::runtime_error(std::move(msg));
            }

            start = endptr;
            ++found;
        }
        if (!has_tail && *start)
        {
            std::string msg("cannot parse ");
            msg += what;
            msg += ": found trailing characters at the end: \"";
            msg += start;
            msg += '"';
            throw std::runtime_error(std::move(msg));
        }
		else if (has_tail && *start)
		{
			// Skip colons and spaces, if any
			while (*start && (::isspace(*start) || *start == ','))
				++start;
			tail = start;
		}
	}
};

// functions to split strings
void split(const std::string& str, std::set<std::string>& result, const std::string& delimiters = ",");
void split(const std::string& str, std::vector<std::string>& result, const std::string& delimiters = ",");

}
}

#endif
