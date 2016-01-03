#ifndef ARKI_CONFIGFILE_H
#define ARKI_CONFIGFILE_H

#include <map>
#include <string>
#include <iosfwd>
#include <stdexcept>

namespace arki {

class ConfigFileParseError : public std::runtime_error
{
protected:
    std::string m_name;
    int m_line;
    std::string m_error;

    static std::string describe(const std::string& filename, int line, const std::string& error);

public:
    ConfigFileParseError(const std::string& filename, int line, const std::string& error)
        : std::runtime_error(describe(filename, line, error)), m_name(filename), m_line(line), m_error(error) {}
    ~ConfigFileParseError() throw () {}
};


class ConfigFileParserHelper;

/**
 * A tree of configuration information
 */
class ConfigFile
{
public:
	/**
	 * Represent the source file information of where a value has been found
	 */
	struct FilePos {
		std::string pathname;
		size_t lineno;

		FilePos(const std::string& pathname, const size_t& lineno):
			pathname(pathname), lineno(lineno) {}
	};

private:
	/// The configuration values in this node
	std::map<std::string, std::string> m_values;

	/**
	 * The source file information of where the configuration values have been
	 * found.
	 *
	 * If a values_pos is missing for a value, it means that the source
	 * information is unknown.
	 */
	std::map<std::string, FilePos> values_pos;

	/**
	 * Subnodes of this node
	 */
	std::map<std::string, ConfigFile*> sections;

	/**
	 * Parse one normal configuration file line
	 *
	 * The line may contain a comment, a key=value assignment or be empty.
	 */
	void parseLine(ConfigFileParserHelper& h, const std::string& line);

	/**
	 * Parse configuration from the given input stream.
	 *
	 * Finding the beginning of a new section will stop
	 * the parsing.
	 */
	void parseSection(ConfigFileParserHelper& h, std::istream& in);

public:
	ConfigFile();
	ConfigFile(const ConfigFile& cfg);
	~ConfigFile();

    ConfigFile& operator=(const ConfigFile& cfg);

	//typedef std::map<std::string, std::string>::iterator iterator;
	typedef std::map<std::string, std::string>::const_iterator const_iterator;
	typedef std::map<std::string, ConfigFile*>::iterator section_iterator;
	typedef std::map<std::string, ConfigFile*>::const_iterator const_section_iterator;

	//iterator begin() { return values.begin(); }
	//iterator end() { return values.end(); }
	const_iterator begin() const { return m_values.begin(); }
	const_iterator end() const { return m_values.end(); }
	section_iterator sectionBegin() { return sections.begin(); }
	section_iterator sectionEnd() { return sections.end(); }
	const_section_iterator sectionBegin() const { return sections.begin(); }
	const_section_iterator sectionEnd() const { return sections.end(); }

    /// Remove all contents of this ConfigFile
    void clear();

	/// Number of sections at this level (does not include subsections of sections)
	size_t sectionSize() const { return sections.size(); }

	/// Number of values at this level (does not include values inside subsections)
	size_t valueSize() const { return m_values.size(); }

	/// Get the values map
	const std::map<std::string, std::string>& values() const { return m_values; }

	/**
	 * Copy all the values from c into this ConfigFile, merging with existing
	 * values and sections.
	 */
	void merge(const ConfigFile& c);

	/**
	 * Copy all the values from c into the given section of this ConfigFile,
	 * creating the section if it does not exist and merging with existing
	 * values and subsections.
	 */
	void mergeInto(const std::string& sectionName, const ConfigFile& c);

	/**
	 * Retrieve the value for a key.
	 *
	 * The empty string is returned if the value is empty.
	 */
	std::string value(const std::string& key) const;

	/**
	 * Retrieve information about where a value has been parsed.
	 *
	 * @returns
	 *   none if there is no information for the given key,
	 *   else, a pointer to a ConfigFile::FilePos structure, which will be
	 *   valid as long as this ConfigFile object is not modified.
	 */
	const FilePos* valueInfo(const std::string& key) const;

    /// Set a value
    void setValue(const std::string& key, const std::string& value);

    /// Set a value converting any type to a string
    void setValue(const std::string& key, int value);

	/**
	 * Retrieve a section from this config file.
	 *
	 * 0 is returned if there is no such section.
	 */
	ConfigFile* section(const std::string& key) const;

	/**
	 * Retrieve a section from this config file, creating it if it is
	 * missing.
	 */
	ConfigFile* obtainSection(const std::string& key);

	/**
	 * Delete a section, if present
	 */
	void deleteSection(const std::string& key);

	/**
	 * Parse configuration from the given input stream.
	 *
	 * The sections that are found are added to this ConfigFile.
	 *
	 * @param in
	 *   The input stream to parse.
	 * @param fileName
	 *   The file name to use to generate useful parse error messages.
	 */
	void parse(std::istream& in, const std::string& fileName);

    /**
     * Parse configuration from the given string.
     *
     * The values and sections that are found are added to this ConfigFile.
     */
    void parse(const std::string& in, const std::string& file_name="memory buffer");

    /**
     * Output this configuration to the given output stream
     */
    void output(std::ostream& out, const std::string& fileName, const std::string& secName=std::string()) const;

    /// Serialize to a string
    std::string serialize() const;

	/**
	 * Convenient function to parse a boolean
	 */
	static bool boolValue(const std::string& str, bool def = false);
};

}
#endif
