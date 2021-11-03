#ifndef ARKI_METADATA_H
#define ARKI_METADATA_H

/// metadata - Handle arkimet metadata

#include <arki/core/fwd.h>
#include <arki/stream/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/types/fwd.h>
#include <arki/structured/fwd.h>
#include <memory>
#include <string>
#include <vector>

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


/**
 * Store Type elements, in the order Metadata encodes them.
 *
 * The order is:
 *  - all Types, ordered by type_code, except NOTE and SOURCE
 *  - all Note types, in order of insertion
 *  - one Source type
 */
class Index
{
protected:
    std::vector<types::Type*> items;

public:
    typedef std::vector<types::Type*>::const_iterator const_iterator;

    Index() = default;
    ~Index();

    Index(const Index&) = delete;
    Index(Index&&) = delete;
    Index& operator=(const Index&) = delete;
    Index& operator=(Index&&) = delete;

    const_iterator begin() const { return items.begin(); }
    const_iterator end() const { return items.end(); }
    size_t empty() const { return items.empty(); }
    size_t size() const { return items.size(); }

    /**
     * Append an element regardless of invariants.
     *
     * t could be any type, including Notes and Source.
     *
     * This can be used to construct an Index from data already in the right
     * order, or to temporarily break invariants with the guarantee to sort
     * items later.
     */
    void raw_append(std::unique_ptr<types::Type> t);

    /**
     * Return an iterator to the first position after the last value (excluding
     * NOTE and SOURCE).
     *
     * This can be used as an end iterator to iterate values.
     */
    const_iterator values_end() const;

    /**
     * Return a begin-end iterator pair that can be used to iterate notes.
     *
     * Note that [begin() to notes.first] can be used to iterate values: this
     * can be used to get the extremes to iterate both values and notes in a
     * single call.
     */
    std::pair<const_iterator, const_iterator> notes() const;

    /**
     * Check if the index contains an item of the given code
     */
    bool has(types::Code code) const;

    /**
     * Return the item with the given code.
     *
     * This should not be used for notes. In case code == TYPE_NOTE, it returns
     * the first one.
     */
    const types::Type* get(types::Code code) const;

    /**
     * Get the Source element.
     *
     * If not present, returns nullptr;
     */
    const types::Source* get_source() const;

    /**
     * Get the Source element, non-const version.
     *
     * If not present, returns nullptr;
     */
    types::Source* get_source();

    /**
     * Set or replace the Source item
     */
    void set_source(std::unique_ptr<types::Source> s);

    /// Remove the Source element, if present
    void unset_source();

    /// Remove all Note elements
    void clear_notes();

    /// Append a note at the end of the note list
    void append_note(std::unique_ptr<types::Note> n);

    /// Return the last element in the note list, or nullptr if there are no
    /// notes
    const types::Note* get_last_note() const;

    /**
     * Fill this index with clones of the items in o.
     *
     * This method breaks the ordering invariant if called while the Index is
     * not empty to begin with.
     */
    void clone_fill(const Index& o);

    /**
     * Set or replace the given value.
     *
     * The result is unpredictable if value is a Source or a Note.
     */
    void set_value(std::unique_ptr<types::Type> item);

    /**
     * Unset the given value.
     */
    void unset_value(types::Code code);
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
class Metadata
{
protected:
    /// Buffer pointing to the encoded version of this metadata, to reuse for items
    const uint8_t* m_encoded = nullptr;

    /// Size of the m_encoded buffer
    unsigned m_encoded_size = 0;

    /// Metadata items currently set
    metadata::Index m_index;

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

    bool has(types::Code code) const { return m_index.has(code); }
    const types::Type* get(types::Code code) const { return m_index.get(code); }

    template<typename T>
    const T* get() const
    {
        const types::Type* i = m_index.get(types::traits<T>::type_code);
        if (!i) return 0;
        // Since we checked the type code, we can use reinterpret_cast instead
        // of slow dynamic_cast
        return reinterpret_cast<const T*>(i);
    }

    // TODO: periodically make the set and unset methods protected, and see if
    // they are used in performance critical code: in that case they should be
    // refactored to construct a metadata more efficiently

    /// Set a value (not Note or Source)
    void set(const types::Type& item);
    /// Set a value (not Note or Source)
    template<typename T>
    void set(std::unique_ptr<T> i) { m_index.set_value(std::move(i)); }
    /// Unset a value (not Note or Source)
    void unset(types::Code code) { m_index.unset_value(code); }

    /// Copy all types from md into this metadata, excluding Notes and Source
    void merge(const Metadata& md);

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
    void test_set(const types::Type& item);
    void test_set(std::unique_ptr<types::Type> item);
    template<typename T>
    void test_set(std::unique_ptr<T> i) { test_set(std::unique_ptr<types::Type>(i.release())); }
    void test_set(const std::string& type, const std::string& val);

    /*
     * Source access and manipulation
     */

    /// Check if a source has been set
    bool has_source() const { return m_index.get_source() != nullptr; }
    /// Return the source if present, else raise an exception
    const types::Source& source() const;
    /// Return the Blob source if it exists, else 0
    const types::source::Blob* has_source_blob() const;
    /// Return the Blob source if possible, else raise an exception
    const types::source::Blob& sourceBlob() const;
    /// Return the Blob source if possible, else raise an exception
    types::source::Blob& sourceBlob();
    /// Set a new source, replacing the old one if present
    void set_source(std::unique_ptr<types::Source> s);
    /// Set the source of this metadata as Inline, with the given data
    void set_source_inline(const std::string& format, std::shared_ptr<metadata::Data> data);
    /// Unsets the source
    void unset_source();
    /// Read the data and inline them in the metadata
    void makeInline();
    /// Make all source blobs absolute
    void make_absolute();

    /*
     * Notes access and manipulation
     */

    void clear_notes();
    std::pair<metadata::Index::const_iterator, metadata::Index::const_iterator> notes() const { return m_index.notes(); }
    void encode_notes(core::BinaryEncoder& enc) const;
    void set_notes_encoded(const uint8_t* data, unsigned size);
    void add_note(const std::string& note);
    const types::Note& get_last_note() const;

    /// Check if the items match, ignoring Source, Notes, and Value items
    bool items_equal(const Metadata& m) const;

    /**
     * Check that two Metadata contain the same information, including Source
     * and Notes
     */
    bool operator==(const Metadata& m) const;

    /**
     * Check that two Metadata contain different information, including Source
     * and notes
     */
    bool operator!=(const Metadata& m) const { return !operator==(m); }

    /**
     * Report the items (excluding Source and Notes) that differ between the
     * two metadata.
     *
     * dest called with only first set, means an item that is in this metadata
     * but not in the other.
     *
     * dest called with only second set, means an item that is in the other
     * metadata but not in this one.
     *
     * dest called with both first and second set, means an item that is in
     * both but has a different value.
     */
    void diff_items(const Metadata& o, std::function<void(types::Code code, const types::Type* first, const types::Type* second)> dest) const;

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
     */
    void write(core::NamedFileDescriptor& out, bool skip_data=false) const;

    /**
     * Write the metadata to the stream output.
     */
    void write(StreamOutput& out, bool skip_data=false) const;

    /// Format the metadata as a yaml string
    std::string to_yaml(const Formatter* formatter=nullptr) const;

    /// Serialise using an emitter
    void serialise(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const;

    /// Encode to a buffer. Inline data will not be added.
    std::vector<uint8_t> encodeBinary() const;

    /// Encode to an Encoder. Inline data will not be added.
    void encodeBinary(core::BinaryEncoder& enc) const;


    /// Get the raw data described by this metadata
    const metadata::Data& get_data();

    /**
     * Stream the data referred by this metadata to the given output.
     *
     * Return the number of bytes written
     */
    stream::SendResult stream_data(StreamOutput& out);

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

    /// Return the size of the data, if known, else returns 0
    size_t data_size() const;

    /// Dump the contents of the index to the given file descriptor
    void dump_internals(FILE* out) const;

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
