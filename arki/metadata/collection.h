#ifndef ARKI_METADATA_COLLECTION_H
#define ARKI_METADATA_COLLECTION_H

/// In-memory collection of metadata

#include <arki/metadata/consumer.h>
#include <vector>
#include <string>
#include <iosfwd>

namespace arki {
struct Metadata;
struct Summary;
struct ReadonlyDataset;

namespace dataset {
struct DataQuery;
}

namespace sort {
struct Compare;
}

namespace metadata {

/**
 * Consumer that collects all metadata into a vector
 */
class Collection : public Eater
{
protected:
    std::vector<Metadata*> vals;

public:
    Collection();
    /// Construct a collection filled by the results of query_data
    Collection(ReadonlyDataset& ds, const dataset::DataQuery& q);
    /// Construct a collection filled with the data scanned from the given file
    /// using scan::any
    Collection(const std::string& pathname);
    virtual ~Collection();

    void clear();
    bool empty() const { return vals.empty(); }
    size_t size() const { return vals.size(); }
    const Metadata& operator[](unsigned idx) const { return *vals[idx]; }
    Metadata& operator[](unsigned idx) { return *vals[idx]; }

    typedef std::vector<Metadata*>::const_iterator const_iterator;
    const_iterator begin() const { return vals.begin(); }
    const_iterator end() const { return vals.end(); }

    const Metadata& back() const { return *vals.back(); }

    /// Remove the last element
    void pop_back();

    /// Return a metadata_dest_func that inserts into this collection
    metadata_dest_func inserter_func();

    /// Append results from a query_data
    void add(ReadonlyDataset& ds, const dataset::DataQuery& q);

    /// Append a copy of md
    void push_back(const Metadata& md);

    /// Append md
    bool eat(std::unique_ptr<Metadata>&& md) override;

	/**
	 * Write all the metadata to a file, atomically, using AtomicWriter
	 */
	void writeAtomically(const std::string& fname) const;

	/**
	 * Append all metadata to the given file
	 */
	void appendTo(const std::string& fname) const;

	/**
	 * Write all metadata to the given output stream
	 */
	void writeTo(std::ostream& out, const std::string& fname) const;

    /**
     * Write all metadata to the given output file
     */
    void write_to(int out, const std::string& fname) const;

    /// Read metadata from \a pathname and append them to this collection
    void read_from_file(const std::string& pathname);

    /// Add all metadata to a summary
    void add_to_summary(Summary& out) const;

    /**
     * Send a copy of all metadata to an eater
     */
    bool copy_to_eater(Eater& out) const;

    /// Send all metadata to an eater, emptying this Collection
    bool move_to_eater(Eater& out);

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

	/**
	 * If all the metadata here entirely cover a single data file, replace
	 * it with a compressed version
	 */
	void compressDataFile(size_t groupsize = 512, const std::string& source = std::string("metadata"));

	/// Sort with the given order
	void sort(const sort::Compare& cmp);
	void sort(const std::string& cmp);
	void sort(); // Sort by reftime
};

}
}

// vim:set ts=4 sw=4:
#endif
