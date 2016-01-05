#ifndef ARKI_METADATA_H
#define ARKI_METADATA_H

/// metadata - Handle arkimet metadata

#include <arki/itemset.h>
#include <arki/types.h>
#include <arki/types/note.h>
#include <arki/types/source.h>
#include <string>

struct lua_State;

namespace arki {

namespace types {
namespace source {
struct Blob;
}
}

namespace metadata {

struct ReadContext
{
    std::string basedir;
    std::string pathname;

    ReadContext();
    ReadContext(const std::string& pathname);
    ReadContext(const std::string& pathname, const std::string& basedir);
};

}

class Formatter;

/**
 * Metadata information about a message
 */
struct Metadata : public ItemSet
{
protected:
    /// Annotations, kept binary-serialized to string
    std::vector<uint8_t> m_notes;

    /// Source of this data
    types::Source* m_source;

    /// Inline data, or cached version of previously read data
    std::vector<uint8_t> m_data;

public:
    Metadata();
    Metadata(const Metadata&);
    ~Metadata();
    Metadata& operator=(const Metadata&);

    Metadata* clone() const;

    /// Check if a source has been set
    bool has_source() const { return m_source; }
    /// Return the source if present, else raise an exception
    const types::Source& source() const;
    /// Return the Blob source if it exists, else 0
    const types::source::Blob* has_source_blob() const;
    /// Return the Blob source if possible, else raise an exception
    const types::source::Blob& sourceBlob() const;
    /// Set a new source, replacing the old one if present
    void set_source(std::unique_ptr<types::Source>&& s);
    /// Set the source of this metadata as Inline, with the given data
    void set_source_inline(const std::string& format, std::vector<uint8_t>&& buf);
    /// Unsets the source
    void unset_source();

    std::vector<types::Note> notes() const;
    const std::vector<uint8_t>& notes_encoded() const;
    void set_notes(const std::vector<types::Note>& notes);
    void set_notes_encoded(const std::vector<uint8_t>& notes);
    void add_note(const types::Note& note);
    void add_note(const std::string& note);

	/**
	 * Check that two Metadata contain the same information
	 */
	bool operator==(const Metadata& m) const;

	/**
	 * Check that two Metadata contain different information
	 */
	bool operator!=(const Metadata& m) const { return !operator==(m); }

	int compare(const Metadata& m) const;
	bool operator<(const Metadata& o) const { return compare(o) < 0; }
	bool operator<=(const Metadata& o) const { return compare(o) <= 0; }
	bool operator>(const Metadata& o) const { return compare(o) > 0; }
	bool operator>=(const Metadata& o) const { return compare(o) >= 0; }

    /// Clear all the contents of this Metadata
    void clear();

    /// Decode from structured data
    void read(const emitter::memory::Mapping& val);

    /**
     * Read a metadata document from the given input stream.
     *
     * The filename string is used to generate nicer parse error messages when
     * throwing exceptions, and can be anything.
     *
     * If readInline is true, in case the data is transmitted inline, it reads
     * the data as well: this is what you expect.
     *
     * If it's false, then the reader needs to check from the Metadata source
     * if it is inline, and in that case proceed to read the inline data.
     *
     * @returns false when end-of-file is reached
     */
    bool read(int in, const metadata::ReadContext& filename, bool readInline=true);

    /**
     * Read a metadata document from the given memory buffer.
     *
     * The filename string is used to generate nicer parse error messages when
     * throwing exceptions, and can be anything.
     *
     * If readInline is true, in case the data is transmitted inline, it reads
     * the data as well: this is what you expect.
     *
     * If it's false, then the reader needs to check from the Metadata source
     * if it is inline, and in that case proceed to read the inline data.
     *
     * @returns false when end-of-file is reached
     */
    bool read(BinaryDecoder& dec, const metadata::ReadContext& filename, bool readInline=true);

    /**
     * Decode the metadata, without the outer bundle headers, from the given buffer.
     */
    void read_inner(BinaryDecoder& dec, unsigned version, const metadata::ReadContext& filename);

    /// Read the inline data from the given file handle
    void readInlineData(int in, const std::string& filename);

    /// Read the inline data from the given memory buffer
    void readInlineData(BinaryDecoder& dec, const std::string& filename);

    /**
     * Read a metadata document encoded in Yaml from the given file descriptor.
     *
     * The filename string is used to generate nicer parse error messages when
     * throwing exceptions, and can be anything.
     *
     * @returns false when end-of-file is reached
     */
    bool readYaml(LineReader& in, const std::string& filename);

	/**
	 * Write the metadata to the given output stream.
	 *
	 * The filename string is used to generate nicer parse error messages when
	 * throwing exceptions, and can be anything.
	 */
	void write(int outfd, const std::string& filename) const;

	/**
	 * Write the metadata as YAML text to the given output stream.
	 */
	void writeYaml(std::ostream& out, const Formatter* formatter = 0) const;

    /// Serialise using an emitter
    void serialise(Emitter& e, const Formatter* f=0) const;

    /// Encode to a buffer. Inline data will not be added.
    std::vector<uint8_t> encodeBinary() const;

    /// Encode to an Encoder. Inline data will not be added.
    void encodeBinary(BinaryEncoder& enc) const;


    /// Get the raw data described by this metadata
    const std::vector<uint8_t>& getData();

    /// Return True if getData can be called without causing I/O
    bool has_cached_data() const;

    /**
     * Set cached data for non-inline sources, so that getData() won't have
     * to read it again.
     */
    void set_cached_data(std::vector<uint8_t>&& buf);

    /**
     * If the source is not inline, but the data are cached in memory, drop
     * them.
     *
     * Data for non-inline metadata can be cached in memory, for example,
     * by a getData() call or a set_cached_data() call.
     */
    void drop_cached_data();

    /// Read the data and inline them in the metadata
    void makeInline();

    /// Make all source blobs absolute
    void make_absolute();

    /// Return the size of the data, if known, else returns 0
    size_t data_size() const;

    /**
     * Flush open data readers.
     *
     * A persistent data reader is used to read data, in order to keep the last
     * file opened and buffered to speed up reading multiple data items from
     * the same file. This function tells the data reader to close its open
     * files.
     *
     * It is useful for testing cases when data files are moved or
     * compressed.
     */
    static void flushDataReaders();

    /// Create an empty Metadata
    static std::unique_ptr<Metadata> create_empty();

    /// Create a copy of a Metadata
    static std::unique_ptr<Metadata> create_copy(const Metadata& md);

#if 0
    /// Read one Metadata from a Yaml stream and return it
    static std::unique_ptr<Metadata> create_from_yaml(std::istream& in, const std::string& filename);
#endif

    /// Read all metadata from a buffer into the given consumer
    static void read_buffer(const std::vector<uint8_t>& buf, const metadata::ReadContext& fname, metadata_dest_func dest);

    /// Read all metadata from a file into the given consumer
    static void read_file(const std::string& fname, metadata_dest_func dest);

    /// Read all metadata from a file into the given consumer
    static void read_file(const metadata::ReadContext& fname, metadata_dest_func dest);

    /// Read all metadata from a file into the given consumer
    static void read_file(int in, const metadata::ReadContext& file, metadata_dest_func mdc);

    /**
     * Read a metadata group into the given consumer
     */
    static void read_group(BinaryDecoder& dec, unsigned version, const metadata::ReadContext& file, metadata_dest_func dest);

	// LUA functions
	/// Push to the LUA stack a userdata to access this Metadata
	void lua_push(lua_State* L);

    /**
     * Push a userdata to access this Metadata, and hand over its ownership to
     * Lua's garbage collector
     */
    static void lua_push(lua_State* L, std::unique_ptr<Metadata>&& md);

	/**
	 * Check that the element at \a idx is a Metadata userdata
	 *
	 * @return the Metadata element, or 0 if the check failed
	 */
	static Metadata* lua_check(lua_State* L, int idx);

	/**
	 * Load metadata functions into a lua VM
	 */
	static void lua_openlib(lua_State* L);
};

}
#endif
