#ifndef ARKI_METADATA_H
#define ARKI_METADATA_H

/// metadata - Handle arkimet metadata

#include <arki/core/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/types/fwd.h>
#include <arki/types/itemset.h>
#include <arki/types/note.h>
#include <memory>
#include <string>

namespace arki {
namespace metadata {

class Data;

struct ReadContext
{
    /**
     * Absolute path of the root directory for relative paths in the Metadata's
     * source filenames
     */
    std::string basedir;

    /**
     * Absolute path of the file to read if basedir is empty, else relative
     * path rooted on basedir.
     */
    std::string pathname;

    ReadContext();

    /**
     * A file to read, with metadata source filenames rooted in
     * dirname(pathname)
     */
    ReadContext(const std::string& pathname);

    /**
     * A file to read, with metadata source filenames rooted in the given
     * directory.
     */
    ReadContext(const std::string& pathname, const std::string& basedir);
};

}

class Formatter;

/**
 * Metadata information about a message.
 *
 * Binary representation of metadata:
 *  - 'MD'
 *  - 2b version (currently always 0)
 *  - 4b length
 *  - sequence of Type::encodeBinary for all types except NOTE and SOURCE,
 *    ordered by type_code
 *  - sequence of Type::encodeBinary for all NOTE types, in order of insertion
 *  - Type::encodeBinary of the SOURCE metadata
 */
struct Metadata
{
protected:
    /// Buffer pointing to the encoded version of this metadata, to reuse for items
    const uint8_t* m_encoded = nullptr;

    /// Size of the m_encoded buffer
    unsigned m_encoded_size = 0;

    /// Metadata items currently set
    types::ItemSet m_items;

    /// Annotations, kept binary-serialized to string
    std::vector<types::Note*> m_notes;

    /// Source of this data
    types::Source* m_source = nullptr;

    /// Inline data, or cached version of previously read data
    std::shared_ptr<metadata::Data> m_data;

    void add_note(const types::Note& note);

public:
    Metadata();
    /// Create an empty metadata with a copy of the given buffer as encoded
    /// buffer
    Metadata(const uint8_t* encoded, unsigned size);
    Metadata(const Metadata&) = delete;
    ~Metadata();

    Metadata& operator=(const Metadata&) = delete;

    std::shared_ptr<Metadata> clone() const;

    bool has(types::Code code) const { return m_items.has(code); }
    const types::Type* get(types::Code code) const { return m_items.get(code); }
    template<typename T>
    const T* get() const { return m_items.get<T>(); }
    void set(const types::Type& item) { m_items.set(item); }
    template<typename T>
    void set(std::unique_ptr<T> i) { m_items.set(std::move(i)); }
    void set(const std::string& type, const std::string& val) { m_items.set(type, val); }
    void unset(types::Code code) { m_items.unset(code); }

    template<typename T, typename ...Args>
    void set(Args&&... args)
    {
        set(T::create(std::forward<Args>(args)...));
    }

    template<typename T, typename ...Args>
    void test_set(Args&&... args)
    {
        test_set(T::create(std::forward<Args>(args)...));
    }

    /// Copy all types from md into this metadata
    void merge(const Metadata& md);

    void diff_items(const Metadata& o, std::function<void(types::Code code, const types::Type* first, const types::Type* second)> dest) const;

    void test_set(const types::Type& item)
    {
        std::unique_ptr<types::Type> clone(item.clone());
        m_items.set(std::move(clone));
    }

    void test_set(std::unique_ptr<types::Type> item)
    {
        m_items.set(std::move(item));
    }

    template<typename T>
    void test_set(std::unique_ptr<T> i) { test_set(std::unique_ptr<types::Type>(i.release())); }

    void test_set(const std::string& type, const std::string& val)
    {
        test_set(types::decodeString(types::parseCodeName(type.c_str()), val));
    }

    /*
     * Source access and manipulation
     */

    /// Check if a source has been set
    bool has_source() const { return m_source; }
    /// Return the source if present, else raise an exception
    const types::Source& source() const;
    /// Return the Blob source if it exists, else 0
    const types::source::Blob* has_source_blob() const;
    /// Return the Blob source if possible, else raise an exception
    const types::source::Blob& sourceBlob() const;
    /// Return the Blob source if possible, else raise an exception
    types::source::Blob& sourceBlob();
    /// Set a new source, replacing the old one if present
    void set_source(std::unique_ptr<types::Source>&& s);
    /// Set the source of this metadata as Inline, with the given data
    void set_source_inline(const std::string& format, std::shared_ptr<metadata::Data> data);
    /// Unsets the source
    void unset_source();

    /*
     * Notes access and manipulation
     */

    void clear_notes();
    const std::vector<types::Note*>& notes() const;
    void encode_notes(core::BinaryEncoder& enc) const;
    void set_notes_encoded(const std::vector<uint8_t>& notes);
    void add_note(const std::string& note);

	/**
	 * Check that two Metadata contain the same information
	 */
	bool operator==(const Metadata& m) const;

	/**
	 * Check that two Metadata contain different information
	 */
	bool operator!=(const Metadata& m) const { return !operator==(m); }

    /// Compare the items, excluding source and notes
    int compare_items(const Metadata& m) const;
    int compare(const Metadata& m) const;
    bool operator<(const Metadata& o) const { return compare(o) < 0; }
    bool operator<=(const Metadata& o) const { return compare(o) <= 0; }
    bool operator>(const Metadata& o) const { return compare(o) > 0; }
    bool operator>=(const Metadata& o) const { return compare(o) >= 0; }

    /// Decode from structured data
    static std::shared_ptr<Metadata> read_structure(const structured::Keys& keys, const structured::Reader& val);

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
     * @returns an empty shared_ptr when end-of-file is reached
     */
    static std::shared_ptr<Metadata> read_binary(int in, const metadata::ReadContext& filename, bool readInline=true);

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
     * @returns an empty shared_ptr when end-of-file is reached
     */
    static std::shared_ptr<Metadata> read_binary(core::BinaryDecoder& dec, const metadata::ReadContext& filename, bool readInline=true);

    /**
     * Decode the metadata, without the outer bundle headers, from the given buffer.
     */
    static std::shared_ptr<Metadata> read_binary_inner(core::BinaryDecoder& dec, unsigned version, const metadata::ReadContext& filename);

    /// Read the inline data from the given file handle
    void read_inline_data(core::NamedFileDescriptor& fd);

    /// Read the inline data from the given file handle
    void read_inline_data(core::AbstractInputFile& fd);

    /// Read the inline data from the given memory buffer
    void readInlineData(core::BinaryDecoder& dec, const std::string& filename);

    /**
     * Read a metadata document encoded in Yaml from the given file descriptor.
     *
     * The filename string is used to generate nicer parse error messages when
     * throwing exceptions, and can be anything.
     *
     * @returns an empty shared_ptr when end-of-file is reached
     */
    static std::shared_ptr<Metadata> read_yaml(core::LineReader& in, const std::string& filename);

    /**
     * Write the metadata to the given output stream.
     *
     * The filename string is used to generate nicer parse error messages when
     * throwing exceptions, and can be anything.
     */
    void write(core::NamedFileDescriptor& out) const;

    /**
     * Write the metadata to the given output stream.
     *
     * The filename string is used to generate nicer parse error messages when
     * throwing exceptions, and can be anything.
     */
    void write(core::AbstractOutputFile& out) const;

    /// Format the metadata as a yaml string
    std::string to_yaml(const Formatter* formatter=nullptr) const;

    /**
     * Write the metadata as YAML text to the given output stream.
     */
    void write_yaml(core::NamedFileDescriptor& out, const Formatter* formatter=nullptr) const;

    /**
     * Write the metadata as YAML text to the given output stream.
     */
    void write_yaml(core::AbstractOutputFile& out, const Formatter* formatter=nullptr) const;

    /// Serialise using an emitter
    void serialise(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const;

    /// Encode to a buffer. Inline data will not be added.
    std::vector<uint8_t> encodeBinary() const;

    /// Encode to an Encoder. Inline data will not be added.
    void encodeBinary(core::BinaryEncoder& enc) const;


    /// Get the raw data described by this metadata
    const metadata::Data& get_data();

    /**
     * Stream the data referred by this metadata to the given file descriptor.
     *
     * Return the number of bytes written
     */
    size_t stream_data(core::NamedFileDescriptor& out);

    /**
     * Stream the data referred by this metadata to the given file descriptor.
     *
     * Return the number of bytes written
     */
    size_t stream_data(core::AbstractOutputFile& out);

    /// Return True if get_data can be called without causing I/O
    bool has_cached_data() const;

    /**
     * Set cached data for non-inline sources, so that get_data() won't have
     * to read it again.
     */
    void set_cached_data(std::shared_ptr<metadata::Data> data);

    /**
     * If the source is not inline, but the data are cached in memory, drop
     * them.
     *
     * Data for non-inline metadata can be cached in memory, for example,
     * by a get_data() call or a set_cached_data() call.
     */
    void drop_cached_data();

    /// Read the data and inline them in the metadata
    void makeInline();

    /// Make all source blobs absolute
    void make_absolute();

    /// Return the size of the data, if known, else returns 0
    size_t data_size() const;

    /// Read all metadata from a buffer into the given consumer
    static bool read_buffer(const std::vector<uint8_t>& buf, const metadata::ReadContext& file, metadata_dest_func dest);

    /// Read all metadata from a buffer into the given consumer
    static bool read_buffer(const uint8_t* buf, std::size_t size, const metadata::ReadContext& file, metadata_dest_func dest);

    /// Read all metadata from a buffer into the given consumer
    static bool read_buffer(core::BinaryDecoder& dec, const metadata::ReadContext& file, metadata_dest_func dest);

    /// Read all metadata from a file into the given consumer
    static bool read_file(const std::string& fname, metadata_dest_func dest);

    /// Read all metadata from a file into the given consumer
    static bool read_file(const metadata::ReadContext& fname, metadata_dest_func dest);

    /// Read all metadata from a file into the given consumer
    static bool read_file(int in, const metadata::ReadContext& file, metadata_dest_func dest);

    /// Read all metadata from a file into the given consumer
    static bool read_file(core::NamedFileDescriptor& fd, metadata_dest_func dest);

    /// Read all metadata from a file into the given consumer
    static bool read_file(core::AbstractInputFile& fd, const metadata::ReadContext& file, metadata_dest_func dest);

    /**
     * Read a metadata group into the given consumer
     */
    static bool read_group(core::BinaryDecoder& dec, unsigned version, const metadata::ReadContext& file, metadata_dest_func dest);
};

}
#endif
