#ifndef ARKI_METADATA_COLLECTION_H
#define ARKI_METADATA_COLLECTION_H

/// In-memory collection of metadata

#include <arki/defs.h>
#include <arki/core/fwd.h>
#include <arki/dataset/fwd.h>
#include <arki/metadata/fwd.h>
#include <vector>
#include <string>

namespace arki {
struct Summary;

namespace metadata {

/**
 * Consumer that collects all metadata into a vector
 */
class Collection
{
protected:
    std::vector<std::shared_ptr<Metadata>> vals;

public:
    Collection() = default;
    Collection(const Collection& o) = default;
    Collection(Collection&& o) = default;
    /// Construct a collection filled by the results of query_data
    Collection(dataset::Dataset& ds, const dataset::DataQuery& q);
    Collection(dataset::Dataset& ds, const std::string& q);
    Collection(dataset::Reader& ds, const dataset::DataQuery& q);
    Collection(dataset::Reader& ds, const std::string& q);
    ~Collection();

    Collection& operator=(const Collection& o) = default;
    Collection& operator=(Collection&& o) = default;

    bool operator==(const Collection& o) const;

    void clear() { vals.clear(); }
    bool empty() const { return vals.empty(); }
    size_t size() const { return vals.size(); }
    /// Remove the last element
    void pop_back() { vals.pop_back(); }

    /// Append a copy of md
    void push_back(const std::shared_ptr<Metadata> md) { vals.push_back(md); }

    /// Append a copy of md
    void push_back(const Metadata& md);

    // TODO: make an iterator adapter that iterates on Metadata references, to make the interface consistent
    typedef std::vector<std::shared_ptr<Metadata>>::const_iterator const_iterator;
    const_iterator begin() const { return vals.begin(); }
    const_iterator end() const { return vals.end(); }

    const Metadata& operator[](unsigned idx) const { return *vals[idx]; }
    Metadata& operator[](unsigned idx) { return *vals[idx]; }

    const Metadata& back() const { return *vals.back(); }
    Metadata& back() { return *vals.back(); }

    /**
     * Return a collection with a copy of the metadata in this one
     */
    Collection clone() const;

    /**
     * Create a batch for acquire_batch with the contents of this collection
     */
    dataset::WriterBatch make_import_batch() const;

    /// Return a metadata_dest_func that inserts into this collection
    metadata_dest_func inserter_func();

    /// Append results from a query_data
    void add(dataset::Dataset& ds, const dataset::DataQuery& q);

    /// Append results from a query_data
    void add(dataset::Reader& reader, const dataset::DataQuery& q);

    /// Append md
    void acquire(std::shared_ptr<Metadata> md, bool with_data=false);

	/**
	 * Write all the metadata to a file, atomically, using AtomicWriter
	 */
	void writeAtomically(const std::string& fname) const;

	/**
	 * Append all metadata to the given file
	 */
	void appendTo(const std::string& fname) const;

    /// Write all metadata to the given output file
    void write_to(core::NamedFileDescriptor& out) const;

    /// Write all metadata to the given output file
    void write_to(core::AbstractOutputFile& out) const;

    /// Read metadata from \a pathname and append them to this collection
    void read_from_file(const metadata::ReadContext& rc);

    /// Read metadata from \a pathname and append them to this collection
    void read_from_file(const std::string& pathname);

    /// Read metadata from a file descriptor and append them to this collection
    void read_from_file(core::NamedFileDescriptor& fd);

    /// Add all metadata to a summary
    void add_to_summary(Summary& out) const;

    /// Send all metadata to a consumer function, emptying this Collection
    bool move_to(metadata_dest_func dest);

    /**
     * Strip paths from sources in all Metadata, to make them all refer to a
     * filename only.
     *
     * This is useful to prepare a Metadata bundle for being written next to
     * the file(s) that it describes.
     */
    void strip_source_paths();

	/**
	 * Ensure that all metadata point to data in the same file and that
	 * they completely cover the file.
	 *
	 * @returns the data file name
	 */
	std::string ensureContiguousData(const std::string& source = std::string("metadata")) const;

	/// Sort with the given order
	void sort(const sort::Compare& cmp);
	void sort(const std::string& cmp);
	void sort(); // Sort by reftime

    /**
     * Expand the given begin and end ranges to include the datetime extremes
     * of this collection.
     *
     * If begin and end are unset, set them to the datetime extremes of this
     * collection.
     *
     * Returns true if all the metadata items had a reftime set, false if some
     * elements had no reftime information.
     */
    bool expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) const;

    /// Call drop_cached_data on all metadata in the collection
    void drop_cached_data();
};

struct TestCollection : public Collection
{
    using Collection::Collection;

    TestCollection() = default;

    /// Construct a collection filled with the data scanned from the given file
    /// using scan::any
    TestCollection(const std::string& pathname, bool with_data=false);

    /// Construct a collection filled with the data scanned from the given file
    /// using scan::any
    void scan_from_file(const std::string& pathname, bool with_data);

    /// Construct a collection filled with the data scanned from the given file
    /// using scan::any
    void scan_from_file(const std::string& pathname, const std::string& format, bool with_data);
};

}
}
#endif
