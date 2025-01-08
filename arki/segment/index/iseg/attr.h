#ifndef ARKI_DATASET_INDEX_ATTR_H
#define ARKI_DATASET_INDEX_ATTR_H

#include <arki/types/fwd.h>
#include <arki/utils/sqlite.h>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <set>
#include <cstdint>

namespace arki {
namespace matcher {
class OR;
}

namespace segment::index::iseg {

/**
 * Associate in the base one ID per value of metadata item.
 *
 * Values are stored in their encode_for_indexing form, and are considered
 * equal when their encoded form matches bit by bit
 */
class AttrSubIndex
{
public:
	// Name of the metadata we index
	std::string name;
	// Serialisation code of the item type that we index
	types::Code code;

protected:
    utils::sqlite::SQLiteDB& m_db;

    // Precompiled get id statement
    mutable utils::sqlite::PrecompiledQuery* m_select_id;
    // Return the database ID given a string blob. Returns -1 if not found
    int q_select_id(const std::vector<uint8_t>& blob) const;

	// Precompiled select one statement
	mutable utils::sqlite::PrecompiledQuery* m_select_one;
    // Runs the Item given an ID. Returns an undefined item if not found
    std::unique_ptr<types::Type> q_select_one(int id) const;

	// Precompiled select all statement
	mutable utils::sqlite::PrecompiledQuery* m_select_all;

    // Precompiled insert statement
    utils::sqlite::PrecompiledQuery* m_insert;
    // Insert the blob in the database and return its new ID
    int q_insert(const std::vector<uint8_t>& blob);

    /// Add an element to the cache
    void add_to_cache(int id, const types::Type& item) const;
    void add_to_cache(int id, const types::Type& item, const std::vector<uint8_t>& encoded) const;

	// Parsed item cache
	mutable std::map<int, types::Type*> m_cache;

    // Cache of known IDs
    mutable std::map<std::vector<uint8_t>, int> m_id_cache;

public:
	AttrSubIndex(utils::sqlite::SQLiteDB& db, types::Code serCode);
	~AttrSubIndex();

	void initDB();
	void initQueries() const {}

	/**
	 * Get the ID of the metadata item handled by this AttrSubIndex.
	 *
	 * @returns -1 if the relevant item is not defined in md
	 *
	 * It can raise NotFound if the metadata item is not in the database at
	 * all.
	 */
	int id(const Metadata& md) const;

	void read(int id, Metadata& md) const;

	std::vector<int> query(const matcher::OR& m) const;

	int insert(const Metadata& md);

private:
    AttrSubIndex(const AttrSubIndex&);
    AttrSubIndex& operator==(const AttrSubIndex&);
};

typedef AttrSubIndex RAttrSubIndex;
typedef AttrSubIndex WAttrSubIndex;

class Attrs
{
protected:
	std::vector<AttrSubIndex*> m_attrs;

public:
	typedef std::vector<AttrSubIndex*>::const_iterator const_iterator;

	const_iterator begin() const { return m_attrs.begin(); }
	const_iterator end() const { return m_attrs.end(); }

	Attrs(utils::sqlite::SQLiteDB& db, const std::set<types::Code>& attrs);
	~Attrs();

	void initDB();

    size_t size() const { return m_attrs.size(); }

	/**
	 * Obtain the IDs of the metadata items in this metadata that
	 * correspond to the member items of this aggregate, inserting the new
	 * metadata items in the database if they are missing
	 */
	std::vector<int> obtainIDs(const Metadata& md) const;
};

}
}

#endif
