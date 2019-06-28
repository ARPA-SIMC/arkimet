#ifndef ARKI_CORE_CFG_H
#define ARKI_CORE_CFG_H

#include <arki/core/fwd.h>
#include <stdexcept>
#include <map>
#include <string>

namespace arki {
namespace core {
namespace cfg {

class ParseError : public std::runtime_error
{
protected:
    std::string m_name;
    int m_line;
    std::string m_error;

    static std::string describe(const std::string& filename, int line, const std::string& error);

public:
    ParseError(const std::string& filename, int line, const std::string& error)
        : std::runtime_error(describe(filename, line, error)), m_name(filename), m_line(line), m_error(error) {}
    ~ParseError() throw () {}
};

class Parser;
class SectionParser;
class Sections;


/**
 * key -> value mapping of configuration options
 */
class Section : protected std::map<std::string, std::string>
{
protected:

public:
    using std::map<std::string, std::string>::map;
    using std::map<std::string, std::string>::clear;
    using std::map<std::string, std::string>::empty;
    using std::map<std::string, std::string>::size;
    using std::map<std::string, std::string>::begin;
    using std::map<std::string, std::string>::end;
    using std::map<std::string, std::string>::cbegin;
    using std::map<std::string, std::string>::cend;
    using std::map<std::string, std::string>::erase;

    /// Check if the configuration file contains the given key
    bool has(const std::string& key) const;

    /**
     * Retrieve the value for a key.
     *
     * The empty string is returned if the value is empty.
     */
    std::string value(const std::string& key) const;

    /**
     * Retrieve the value for a key as a bool.
     * 
     * The given default value is returned if the key is not found
     */
    bool value_bool(const std::string& key, bool def=false) const;

    /// Set a value
    void set(const std::string& key, const std::string& value);

    /// Set a value converting an integer value to a string
    void set(const std::string& key, int value);

    /// Write this configuration to the given output stream
    void write(std::ostream& out, const std::string& pathname) const;

    /// Parse configuration from the given LineReader
    static Section parse(core::LineReader& in, const std::string& pathname);

    /// Parse configuration from the given input file
    static Section parse(core::NamedFileDescriptor& in);

    /// Parse configuration from the given string
    static Section parse(const std::string& in, const std::string& pathname="memory buffer");
};


/**
 * name -> section mapping of configuration file sections
 */
class Sections : protected std::map<std::string, Section>
{
protected:

public:
    using std::map<std::string, Section>::map;
    using std::map<std::string, Section>::clear;
    using std::map<std::string, Section>::empty;
    using std::map<std::string, Section>::size;
    using std::map<std::string, Section>::begin;
    using std::map<std::string, Section>::end;
    using std::map<std::string, Section>::cbegin;
    using std::map<std::string, Section>::cend;
    using std::map<std::string, Section>::erase;
    using std::map<std::string, Section>::emplace;

    /**
     * Retrieve a section from this config file.
     *
     * nullptr is returned if there is no such section.
     */
    const Section* section(const std::string& key) const;

    /**
     * Retrieve a section from this config file.
     *
     * nullptr is returned if there is no such section.
     */
    Section* section(const std::string& key);

    /// Retrieve a section from this config file, creating it if it is missing.
    Section& obtain(const std::string& key);

    /// Write this configuration to the given output stream
    void write(std::ostream& out, const std::string& pathname) const;

    /// Parse configuration from the given LineReader
    static Sections parse(core::LineReader& in, const std::string& pathname);

    /// Parse configuration from the given input file.
    static Sections parse(core::NamedFileDescriptor& in);

    /// Parse configuration from the given string
    static Sections parse(const std::string& in, const std::string& pathname="memory buffer");

    friend class SectionParser;
};

}
}
}

#endif
