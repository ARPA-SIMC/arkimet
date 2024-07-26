#ifndef ARKI_CORE_CFG_H
#define ARKI_CORE_CFG_H

#include <arki/core/fwd.h>
#include <stdexcept>
#include <map>
#include <string>
#include <memory>
#include <iosfwd>

namespace arki {
namespace core {
namespace cfg {

class ParseError : public std::runtime_error
{
protected:
    std::string m_name;
    int m_line;
    std::string m_error;

    static std::string describe(const std::string& name, int line, const std::string& error);

public:
    ParseError(const std::string& name, int line, const std::string& error)
        : std::runtime_error(describe(name, line, error)), m_name(name), m_line(line), m_error(error) {}
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
public:
    using std::map<std::string, std::string>::map;
    using std::map<std::string, std::string>::clear;
    using std::map<std::string, std::string>::empty;
    using std::map<std::string, std::string>::size;
    using std::map<std::string, std::string>::begin;
    using std::map<std::string, std::string>::end;
    using std::map<std::string, std::string>::cbegin;
    using std::map<std::string, std::string>::cend;
    using std::map<std::string, std::string>::find;
    using std::map<std::string, std::string>::erase;
    using std::map<std::string, std::string>::operator=;

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

    /// Serialize to a string
    std::string to_string() const;

    /// Write to then given output stream
    void write(std::ostream& out) const;

    /// Dump the configuration to the given file
    void dump(FILE* out) const;

    /// Parse configuration from the given LineReader
    static std::shared_ptr<Section> parse(core::LineReader& in, const std::string& pathname);

    /// Parse configuration from the given input file
    static std::shared_ptr<Section> parse(core::NamedFileDescriptor& in);

    /// Parse configuration from the given string
    static std::shared_ptr<Section> parse(const std::string& in, const std::string& pathname="memory buffer");
};


/**
 * name -> section mapping of configuration file sections
 */
class Sections : protected std::map<std::string, std::shared_ptr<Section>>
{
public:
    using std::map<std::string, std::shared_ptr<Section>>::map;
    using std::map<std::string, std::shared_ptr<Section>>::clear;
    using std::map<std::string, std::shared_ptr<Section>>::empty;
    using std::map<std::string, std::shared_ptr<Section>>::size;
    using std::map<std::string, std::shared_ptr<Section>>::begin;
    using std::map<std::string, std::shared_ptr<Section>>::end;
    using std::map<std::string, std::shared_ptr<Section>>::cbegin;
    using std::map<std::string, std::shared_ptr<Section>>::cend;
    using std::map<std::string, std::shared_ptr<Section>>::find;
    using std::map<std::string, std::shared_ptr<Section>>::erase;
    using std::map<std::string, std::shared_ptr<Section>>::emplace;

    Sections() = default;
    Sections(const Sections&);
    Sections(Sections&&) = default;
    Sections& operator=(const Sections&);
    Sections& operator=(Sections &&) = default;

    /**
     * Retrieve a section from this config file.
     *
     * nullptr is returned if there is no such section.
     */
    std::shared_ptr<const Section> section(const std::string& key) const;

    /**
     * Retrieve a section from this config file.
     *
     * nullptr is returned if there is no such section.
     */
    std::shared_ptr<Section> section(const std::string& key);

    /// Retrieve a section from this config file, creating it if it is missing.
    std::shared_ptr<Section> obtain(const std::string& key);

    /// Serialize to a string
    std::string to_string() const;

    /// Write to then given output stream
    void write(std::ostream& out) const;

    /// Dump the configuration to the given file
    void dump(FILE* out) const;

    /// Parse configuration from the given LineReader
    static std::shared_ptr<Sections> parse(core::LineReader& in, const std::string& pathname);

    /// Parse configuration from the given input file.
    static std::shared_ptr<Sections> parse(core::NamedFileDescriptor& in);

    /// Parse configuration from the given string
    static std::shared_ptr<Sections> parse(const std::string& in, const std::string& pathname="memory buffer");

    friend class SectionParser;
};

}
}
}

#endif
