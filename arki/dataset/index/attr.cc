#include <arki/dataset/index/attr.h>
#include <arki/matcher.h>
#include <arki/binary.h>
#include <arki/wibble/exception.h>
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils::sqlite;

namespace arki {
namespace dataset {
namespace index {

int AttrSubIndex::q_select_id(const std::vector<uint8_t>& blob) const
{
    if (not m_select_id)
    {
        m_select_id = new utils::sqlite::PrecompiledQuery("sel_id", m_db);
        m_select_id->compile("SELECT id FROM sub_" + name + " where data=?");
    }

    // Else, fetch it from the database
    m_select_id->reset();
    m_select_id->bind(1, blob);
    int id = -1;
    while (m_select_id->step())
    {
        id = m_select_id->fetch<int>(0);
    }

    return id;
}

unique_ptr<Type> AttrSubIndex::q_select_one(int id) const
{
	if (not m_select_one)
	{
		m_select_one = new utils::sqlite::PrecompiledQuery("sel_one", m_db);
		m_select_one->compile("SELECT data FROM sub_" + name + " where id=?");
	}

	// Reset the query
	m_select_one->reset();
	m_select_one->bind(1, id);

    // Decode every blob and run the matcher on it
    unique_ptr<Type> res;
    while (m_select_one->step())
    {
        const void* buf = m_select_one->fetchBlob(0);
        int len = m_select_one->fetchBytes(0);
        BinaryDecoder dec((const uint8_t*)buf, len);
        res = types::decodeInner(code, dec);
    }
    return res;
}

int AttrSubIndex::q_insert(const std::vector<uint8_t>& blob)
{
    if (not m_insert)
    {
        m_insert = new utils::sqlite::PrecompiledQuery("attr_insert", m_db);
        m_insert->compile("INSERT INTO sub_" + name + " (data) VALUES (?)");
    }

    m_insert->reset();
    m_insert->bind(1, blob);
    m_insert->step();

    return m_db.lastInsertID();
}


AttrSubIndex::AttrSubIndex(utils::sqlite::SQLiteDB& db, types::Code code)
	: name(types::tag(code)), code(code),
	  m_db(db), m_select_id(0), m_select_one(0),
	  m_select_all(0), m_insert(0)
{
}

AttrSubIndex::~AttrSubIndex()
{
    for (map<int, Type*>::iterator i = m_cache.begin(); i != m_cache.end(); ++i)
        delete i->second;
	if (m_select_id) delete m_select_id;
	if (m_select_one) delete m_select_one;
	if (m_select_all) delete m_select_all;
	if (m_insert) delete m_insert;
}

void AttrSubIndex::add_to_cache(int id, const types::Type& item) const
{
    vector<uint8_t> encoded;
    BinaryEncoder enc(encoded);
    item.encodeWithoutEnvelope(enc);
    add_to_cache(id, item, encoded);
}

void AttrSubIndex::add_to_cache(int id, const types::Type& item, const std::vector<uint8_t>& encoded) const
{
    map<int, Type*>::iterator i = m_cache.find(id);
    if (i == m_cache.end())
        m_cache.insert(make_pair(id, item.clone()));
    else
    {
        delete i->second;
        i->second = item.clone();
    }
    m_id_cache.insert(make_pair(encoded, id));
}

int AttrSubIndex::id(const Metadata& md) const
{
    const Type* item = md.get(code);
    if (!item) return -1;

    // Encode the item
    vector<uint8_t> encoded;
    BinaryEncoder enc(encoded);
    item->encodeWithoutEnvelope(enc);

    // First look up in cache
    auto i = m_id_cache.find(encoded);
    if (i != m_id_cache.end())
        return i->second;

	// Else, fetch it from the database
	int id = q_select_id(encoded);

    // Add it to the cache
    if (id != -1)
        add_to_cache(id, *item, encoded);
    else
        throw NotFound();

    return id;
}

void AttrSubIndex::read(int id, Metadata& md) const
{
    map<int, Type*>::const_iterator i = m_cache.find(id);
    if (i != m_cache.end())
    {
        md.set(i->second->cloneType());
        return;
    }

    unique_ptr<Type> item = q_select_one(id);
    md.set(item->cloneType());
    add_to_cache(id, *item);
}

std::vector<int> AttrSubIndex::query(const matcher::OR& m) const
{
	// Compile the select all query
	if (not m_select_all)
	{
		m_select_all = new utils::sqlite::PrecompiledQuery("sel_all", m_db);
		m_select_all->compile("SELECT id, data FROM sub_" + name);
	}

	std::vector<int> ids;

    // Decode every blob in the database and run the matcher on it
    m_select_all->reset();
    while (m_select_all->step())
    {
        const void* buf = m_select_all->fetchBlob(1);
        int len = m_select_all->fetchBytes(1);
        BinaryDecoder dec((const uint8_t*)buf, len);
        unique_ptr<Type> t = types::decodeInner(code, dec);
        if (m.matchItem(*t))
            ids.push_back(m_select_all->fetch<int>(0));
    }
    return ids;
}

void AttrSubIndex::initDB()
{
	// Create the table
	std::string query = "CREATE TABLE IF NOT EXISTS sub_" + name + " ("
		"id INTEGER PRIMARY KEY,"
		" data BLOB NOT NULL,"
		" UNIQUE(data))";
	m_db.exec(query);
}

int AttrSubIndex::insert(const Metadata& md)
{
    const Type* item = md.get(code);
    if (!item) return -1;

    // Extract the blob to insert
    std::vector<uint8_t> blob;
    BinaryEncoder enc(blob);
    item->encodeWithoutEnvelope(enc);

    // Try to serve it from cache if possible
    auto ci = m_id_cache.find(blob);
    if (ci != m_id_cache.end())
        return ci->second;

	// Check if we already have the blob in the database
	int id = q_select_id(blob);
	if (id == -1)
		// If not, insert it
		id = q_insert(blob);

    add_to_cache(id, *item, blob);
    return id;
}


Attrs::Attrs(utils::sqlite::SQLiteDB& db, const std::set<types::Code>& attrs)
{
	// Instantiate subtables
	for (set<types::Code>::const_iterator i = attrs.begin();
			i != attrs.end(); ++i)
	{
		if (*i == TYPE_REFTIME) continue;
		m_attrs.push_back(new AttrSubIndex(db, *i));
	}
}

Attrs::~Attrs()
{
	for (std::vector<AttrSubIndex*>::iterator i = m_attrs.begin(); i != m_attrs.end(); ++i)
		delete *i;
}

void Attrs::initDB()
{
	for (std::vector<AttrSubIndex*>::iterator i = m_attrs.begin(); i != m_attrs.end(); ++i)
		(*i)->initDB();
}

std::vector<int> Attrs::obtainIDs(const Metadata& md) const
{
	vector<int> ids;
	ids.reserve(m_attrs.size());
	for (const_iterator i = begin(); i != end(); ++i)
		ids.push_back((*i)->insert(md));
	return ids;
}

}
}
}
// vim:set ts=4 sw=4:
