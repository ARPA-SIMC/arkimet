// -*- C++ -*-
#ifndef ARKI_UTILS_YAML_H
#define ARKI_UTILS_YAML_H

/// Yaml decoding functions

#include <arki/core/fwd.h>
#include <string>

namespace arki {
namespace utils {

/**
 * Parse a record of Yaml-style field: value couples.
 *
 * Parsing stops either at end of record (one empty line) or at end of file.
 *
 * The value is deindented properly.
 *
 * Example code:
 * \code
 *  utils::YamlStream stream;
 *  map<string, string> record;
 *  std::copy(stream.begin(inputstream), stream.end(), inserter(record));
 * \endcode
 */
class YamlStream
{
public:
    // TODO: add iterator_traits
    class const_iterator
    {
        core::LineReader* in = nullptr;
        std::pair<std::string, std::string> value;
        std::string line;

    public:
        const_iterator(core::LineReader& in);
        const_iterator() {}

        const_iterator& operator++();

        const std::pair<std::string, std::string>& operator*() const
        {
            return value;
        }
        const std::pair<std::string, std::string>* operator->() const
        {
            return &value;
        }
        bool operator==(const const_iterator& ti) const
        {
            return in == ti.in;
        }
        bool operator!=(const const_iterator& ti) const
        {
            return in != ti.in;
        }
    };

    const_iterator begin(core::LineReader& in) { return const_iterator(in); }
    const_iterator end() { return const_iterator(); }
};

}
}
#endif
