#ifndef ARKI_SUMMARY_H
#define ARKI_SUMMARY_H

#include <arki/core/fwd.h>
#include <arki/types/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/structured/fwd.h>
#include <arki/matcher/fwd.h>
#include <arki/types/itemset.h>
#include <arki/utils/geosfwd.h>
#include <vector>
#include <map>
#include <set>
#include <string>
#include <memory>
#include <iosfwd>

struct lua_State;

namespace arki {
namespace summary {
struct Table;
struct Stats;
}

namespace summary {
class LuaIter;

struct Visitor
{
    virtual ~Visitor() {}
    virtual bool operator()(const std::vector<const types::Type*>& md, const Stats& stats) = 0;

	/// Return the metadata code for a given md vector position
	static types::Code codeForPos(size_t pos);

	/**
	 * Return md vector position for a given code
	 *
	 * Returns -1 if the given type code is not included in summaries
	 */
	static int posForCode(types::Code code);
};

struct StatsVisitor
{
	virtual ~StatsVisitor() {}
	virtual bool operator()(const Stats& stats) = 0;
};

struct ItemVisitor
{
    virtual ~ItemVisitor() {}
    virtual bool operator()(const types::Type& item) = 0;
};

}

/**
 * Summary information of a bundle of summary
 */
class Summary
{
protected:
    summary::Table* root;

private:
    Summary(const Summary& s);
    Summary& operator=(const Summary& s);

public:
    Summary();
    ~Summary();

    /**
     * Check that two Summary contain the same information
     *
     * Warning: expensive! Use only in the test suite
     */
    bool operator==(const Summary& m) const;

    /**
     * Check that two Summary contain different information
     *
     * Warning: expensive! Use only in the test suite
     */
    bool operator!=(const Summary& m) const { return !operator==(m); }

	/**
	 * Create a new, empty in-memory summary document
	 */
	void clear();

	/**
	 * Return the number of metadata described by this summary.
	 */
	size_t count() const;

	/**
	 * Return the total size of all the metadata described by this summary.
	 */
	unsigned long long size() const;

    /// Dump the internal structure of this summary for debugging purposes
    void dump(std::ostream& out) const;

	/**
	 * Read a summary from a POSIX file descriptor.
	 *
	 * The filename string is used to generate nicer parse error messages when
	 * throwing exceptions, and can be anything.
	 *
	 * @returns false when end-of-file is reached
	 */
	bool read(int fd, const std::string& filename);

    /**
     * Read a summary from a buffer.
     *
     * The filename string is used to generate nicer parse error messages when
     * throwing exceptions, and can be anything.
     *
     * @returns false when end-of-file is reached
     */
    bool read(core::BinaryDecoder& dec, const std::string& filename);

    /**
     * Decode the summary, without the outer bundle headers, from the given buffer.
     */
    void read_inner(core::BinaryDecoder& dec, unsigned version, const std::string& filename);

    /// Decode from structured data
    void read(const structured::Keys& keys, const structured::Reader& val);

	/**
	 * Read data from the given file
	 */
	void readFile(const std::string& fname);

    /**
     * Read a summary document encoded in Yaml from the given file descriptor.
     *
     * The filename string is used to generate nicer parse error messages when
     * throwing exceptions, and can be anything.
     *
     * Summary items are read from the file until the end of file is found.
     */
    bool readYaml(core::LineReader& in, const std::string& filename);

    /**
     * Write the summary to the given output file.
     *
     * The filename string is used to generate nicer parse error messages when
     * throwing exceptions, and can be anything.
     */
    void write(core::NamedFileDescriptor& out) const;

    /**
     * Write the summary to the given output file.
     *
     * The filename string is used to generate nicer parse error messages when
     * throwing exceptions, and can be anything.
     */
    void write(core::AbstractOutputFile& out) const;

	/**
	 * Write the summary to the given file name.
	 *
	 * The file will be created with a temporary name, then renamed to the
	 * final name.
	 *
	 * \warn The temporary file name will NOT be created securely.
	 */
	void writeAtomically(const std::string& filename);

    /// Format the summary as a yaml string
    std::string to_yaml(const Formatter* formatter=nullptr) const;

    /**
     * Write the summary as YAML text to the given output stream.
     */
    void write_yaml(core::NamedFileDescriptor& out, const Formatter* f=nullptr) const;

    /**
     * Write the summary as YAML text to the given output stream.
     */
    void write_yaml(core::AbstractOutputFile& out, const Formatter* f=nullptr) const;

    /// Serialise using an emitter
    void serialise(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const;

    /**
     * Encode to a string
     */
    std::vector<uint8_t> encode(bool compressed = false) const;

    /**
     * Check if this summary matches the given matcher
     *
     * Return true if there is at least a metadata in this summary that is
     * potentially matched by the matcher
     */
    bool match(const Matcher& matcher) const;

	/**
	 * Add to summary those items that are matched by the matcher
	 */
	void filter(const Matcher& matcher, Summary& result) const;

	/**
	 * Add information about this metadata to the summary
	 */
	void add(const Metadata& md);

	/**
	 * Add information about several metadata to the summary: the
	 * summarisable metadata items are taken from 'md', and the statistics
	 * from 'st'
	 */
	void add(const Metadata& md, const summary::Stats& st);

	/**
	 * Merge a summary into this summary
	 */
	void add(const Summary& s);

	/**
	 * Merge a summary into this summary, keeping only the given metadata
	 * items
	 */
	void add(const Summary& s, const std::set<types::Code>& keep_only);

	/**
	 * Visit all the contents of this summary
	 *
	 * Returns true if the visit was completed, false if the visitor
	 * aborted the visit.
	 */
	bool visit(summary::Visitor& visitor) const;

	/**
	 * Visit all the contents of this summary
	 *
	 * Returns true if the visit was completed, false if the visitor
	 * aborted the visit.
	 */
	bool visitFiltered(const Matcher& matcher, summary::Visitor& visitor) const;

    /**
     * Get the reference time interval covered by the metadata bundle.
     *
     * Note: an end period of (0, 0, 0, 0, 0, 0) means "now".
     */
    std::unique_ptr<types::Reftime> getReferenceTime() const;

    /**
     * Expand the given begin and end ranges to include the datetime extremes
     * of this summary.
     *
     * If begin and end are unset, set them to the datetime extremes of this
     * summary.
     */
    void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) const;

	/**
	 * Get the convex hull of the union of all bounding boxes covered by the
	 * metadata bundle.
	 */
	std::unique_ptr<arki::utils::geos::Geometry> getConvexHull() const;

    /**
     * Return all the unique combination of metadata items that are found
     * by the matcher in this summary.
     *
     * Only metadata items for which there is an expression in matcher are
     * present in the output.
     */
    std::vector<types::ItemSet> resolveMatcher(const Matcher& matcher) const;

    /**
     * Return all the unique combination of metadata items that are found
     * by the matcher in this summary.
     *
     * Metadata are added to res, sorted and avoiding duplicated.
     *
     * Return the number of matching items found (0 if nothing matched)
     */
    size_t resolveMatcher(const Matcher& matcher, std::vector<types::ItemSet>& res) const;

	// LUA functions
	/// Push to the LUA stack a userdata to access this Origin
	void lua_push(lua_State* L);

	/**
	 * Check that the element at \a idx is a Summary userdata
	 *
	 * @return the Summary element, or 0 if the check failed
	 */
	static Summary* lua_check(lua_State* L, int idx);

	/**
	 * Load summary functions into a lua VM
	 */
	static void lua_openlib(lua_State* L);

	friend class matcher::AND;
};

}
#endif
