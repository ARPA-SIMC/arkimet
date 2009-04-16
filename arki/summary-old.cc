/*
 * summary - Handle a summary of a group of summary
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/summary-old.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/utils.h>
#include <arki/formatter.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/timerange.h>
#include <arki/types/area.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/buffer.h>

#include "config.h"

#ifdef HAVE_LUA
extern "C" {
#include <lauxlib.h>
#include <lualib.h>
}
#endif

#ifdef HAVE_GEOS
#include <memory>
#if GEOS_VERSION < 3
#include <geos/geom.h>

using namespace geos;

typedef DefaultCoordinateSequence CoordinateArraySequence;
#else
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/GeometryFactory.h>

using namespace geos::geom;
#endif
#endif

//#define DEBUG_THIS
#ifdef DEBUG_THIS
#warning Debug enabled
#include <iostream>
#endif

using namespace std;
using namespace wibble;

namespace arki {

namespace oldsummary {

int Item::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Item* v = dynamic_cast<const Item*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a summary::Item, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int Item::compare(const Item& o) const
{
	return utils::compareMaps(*this, o);
}

bool Item::operator==(const Type& o) const
{
	const Item* v = dynamic_cast<const Item*>(&o);
	if (!v) return false;
	return operator==(*v);
}

bool Item::operator==(const Item& t) const
{
	const_iterator a = begin();
	const_iterator b = t.begin();
	for ( ; a != end() && b != t.end(); ++a, ++b)
		if (*a != *b)
			return false;
	return a == end() && b == t.end();
}

UItem<> Item::get(types::Code code) const
{
	const_iterator i = find(code);
	if (i == end())
		return UItem<>();
	return i->second;
}


bool Item::accepts(types::Code code)
{
	switch (code)
	{
		case types::TYPE_ORIGIN:
		case types::TYPE_PRODUCT:
  		case types::TYPE_LEVEL:
  		case types::TYPE_TIMERANGE:
  		case types::TYPE_AREA:
		case types::TYPE_ENSEMBLE:
		case types::TYPE_BBOX:
		case types::TYPE_RUN:
			return true;
		default:
			return false;
	}
}

types::Code Item::serialisationCode() const { return types::TYPE_SUMMARYITEM; }
size_t Item::serialisationSizeLength() const { return 2; }
std::string Item::tag() const { return "summaryitem"; }

std::string Item::encodeWithoutEnvelope() const
{
	string res;
	for (const_iterator i = begin(); i != end(); ++i)
		res += i->second->encodeWithEnvelope();
	return res;
}

std::ostream& Item::writeToOstream(std::ostream& o) const
{
	return o << toYaml(0);
}

std::string Item::toYaml(size_t indent, const Formatter* f) const
{
	stringstream res;
	toYaml(res, indent, f);
	return res.str();
}

void Item::toYaml(std::ostream& out, size_t indent, const Formatter* f) const
{
	string ind(indent, ' ');
	for (const_iterator i = begin(); i != end(); ++i)
	{
		out << ind << str::ucfirst(i->second->tag()) << ": ";
		i->second->writeToOstream(out);
		if (f)
			out << "\t# " << (*f)(i->second);
		out << endl;
	}
}

arki::Item<Item> Item::decode(const unsigned char* buf, size_t len, const std::string& filename)
{
	arki::Item<Item> res(new Item);

	// Parse the various elements
	const unsigned char* end = buf + len;
	for (const unsigned char* cur = buf; cur < end; )
	{
		const unsigned char* el_start = cur;
		size_t el_len = end - cur;
		types::Code el_type = types::decodeEnvelope(el_start, el_len);
		res->insert(make_pair(el_type, types::decodeInner(el_type, el_start, el_len)));
		cur = el_start + el_len;
	}
	return res;
}

arki::Item<Item> Item::decodeString(const std::string& str)
{
	using namespace str;

	arki::Item<Item> res(new Item);
	stringstream in(str, ios_base::in);
	YamlStream yamlStream;
	for (YamlStream::const_iterator i = yamlStream.begin(in);
			i != yamlStream.end(); ++i)
	{
		types::Code type = types::parseCodeName(i->first);
		res->insert(make_pair(type, types::decodeString(type, i->second)));
	}
	return res;
}

struct LuaItemIter
{
	const Item& s;
	Item::const_iterator iter;

	LuaItemIter(const Item& s) : s(s), iter(s.begin()) {}
	
	// Lua iterator for summaries
	static int iterate(lua_State* L)
	{
		LuaItemIter& i = **(LuaItemIter**)lua_touserdata(L, lua_upvalueindex(1));
		if (i.iter != i.s.end())
		{
			lua_pushstring(L, str::tolower(types::formatCode(i.iter->first)).c_str());
			i.iter->second->lua_push(L);
			++i.iter;
			return 2;
		}
		else
			return 0;  /* no more values to return */
	}

	static int gc (lua_State *L) {
		LuaItemIter* i = *(LuaItemIter**)lua_touserdata(L, 1);
		if (i) delete i;
		return 0;
	}
};

#ifdef HAVE_LUA
int Item::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the Item reference from the userdata value
	luaL_checkudata(L, udataidx, "arki_summaryitem");
	void* userdata = lua_touserdata(L, udataidx);
	const Item& v = **(const Item**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "iter")
	{
		// Iterate

		/* create a userdatum to store an iterator structure address */
		oldsummary::LuaItemIter**d = (oldsummary::LuaItemIter**)lua_newuserdata(L, sizeof(oldsummary::LuaItemIter*));

		// Get the metatable for the iterator
		if (luaL_newmetatable(L, "arki_summary_item_iter"));
		{
			/* set its __gc field */
			lua_pushstring(L, "__gc");
			lua_pushcfunction(L, oldsummary::LuaItemIter::gc);
			lua_settable(L, -3);
		}

		// Set its metatable
		lua_setmetatable(L, -2);

		// Instantiate the iterator
		*d = new oldsummary::LuaItemIter(v);

		// Creates and returns the iterator function (its sole upvalue, the
		// iterator userdatum, is already on the stack top
		lua_pushcclosure(L, oldsummary::LuaItemIter::iterate, 1);
		return 1;
	} else {
		// Parse the name as an arkimet type name
		types::Code code;
		try {	
			code = types::parseCodeName(name);
		} catch (std::exception& e) {
			lua_pushnil(L);
			return 1;
		}

		// Return the value of the requested type, if we have it
		UItem<> val = v.get(code);
		if (val.defined())
			val->lua_push(L);
		else
			lua_pushnil(L);
		return 1;
	}
}
static int arkilua_lookup_summaryitem(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, Item::lua_lookup, 2);
	return 1;
}
void Item::lua_push(lua_State* L) const
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	const Item** s = (const Item**)lua_newuserdata(L, sizeof(const Item*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki_summaryitem"));
	{
		// If the metatable wasn't previously created, create it now
		// Set the __index metamethod to the lookup function
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup_summaryitem);
		lua_settable(L, -3);
	}

	lua_setmetatable(L, -2);
}
#endif


Stats::Stats(size_t count, unsigned long long size, const types::Reftime* reftime)
	: count(count), size(size)
{
	reftimeMerger.merge(reftime);
}

int Stats::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Stats* v = dynamic_cast<const Stats*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a summary::Stats, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int Stats::compare(const Stats& o) const
{
	if (int res = count - o.count) return res;
	if (int res = size - o.size) return res;
	return reftimeMerger.compare(o.reftimeMerger);
}

bool Stats::operator==(const Type& o) const
{
	const Stats* v = dynamic_cast<const Stats*>(&o);
	if (!v) return false;
	return operator==(*v);
}

bool Stats::operator==(const Stats& o) const
{
	return count == o.count && size == o.size && reftimeMerger == o.reftimeMerger;
}

void Stats::merge(const arki::Item<Stats>& s)
{
	count += s->count;
	size += s->size;
	reftimeMerger.merge(s->reftimeMerger);
}

void Stats::merge(size_t ocount, unsigned long long osize, const types::Reftime* reftime)
{
	count += ocount;
	size += osize;
	reftimeMerger.merge(reftime);
}

types::Code Stats::serialisationCode() const { return types::TYPE_SUMMARYSTATS; }
size_t Stats::serialisationSizeLength() const { return 2; }
std::string Stats::tag() const { return "summarystats"; }

std::string Stats::encodeWithoutEnvelope() const
{
	arki::Item<types::Reftime> reftime(reftimeMerger.makeReftime());
	string encoded = utils::encodeUInt(count, 4);
	encoded += reftime.encode();
	encoded += utils::encodeULInt(size, 8);
	return encoded;
}

std::ostream& Stats::writeToOstream(std::ostream& o) const
{
	return o << toYaml(0);
}

std::string Stats::toYaml(size_t indent) const
{
	stringstream res;
	toYaml(res, indent);
	return res.str();
}

void Stats::toYaml(std::ostream& out, size_t indent) const
{
	arki::Item<types::Reftime> reftime(reftimeMerger.makeReftime());
	string ind(indent, ' ');
	out << ind << "Count: " << count << endl;
	out << ind << "Size: " << size << endl;
	out << ind << "Reftime: " << reftime << endl;
}

arki::Item<Stats> Stats::decode(const unsigned char* buf, size_t len, const std::string& filename)
{
	using namespace utils;

	arki::Item<Stats> res(new Stats);

	// First decode the count
	if (len < 4)
		throw wibble::exception::Consistency("parsing summary stats in file " + filename, "summary stats has size " + str::fmt(len) + " but at least 4 bytes are needed");
	res->count = decodeUInt(buf, 4);
	buf += 4; len -= 4;

	// Then decode the reftime
	const unsigned char* el_start = buf;
	size_t el_len = len;
	types::Code el_type = types::decodeEnvelope(el_start, el_len);
	if (el_type == types::TYPE_REFTIME)
		res->reftimeMerger.merge(types::Reftime::decode(el_start, el_len));
	else
		throw wibble::exception::Consistency("parsing summary stats in file " + filename, "cannot handle element " + str::fmt(el_type));
	len -= el_start + el_len - buf;
	buf = el_start + el_len;

	// Then decode the size (optional, for backward compatibility)
	if (len < 8)
		res->size = 0;
	else
	{
		res->size = decodeULInt(buf, 8);
		buf += 8; len -= 8;
	}

	return res;
}

arki::Item<Stats> Stats::decodeString(const std::string& str)
{
	using namespace str;

	arki::Item<Stats> res(new Stats);
	stringstream in(str, ios_base::in);
	YamlStream yamlStream;
	for (YamlStream::const_iterator i = yamlStream.begin(in);
			i != yamlStream.end(); ++i)
	{
		string name = tolower(i->first);
		if (name == "count")
			res->count = strtoul(i->second.c_str(), 0, 10);
		else if (name == "size")
			res->size = strtoull(i->second.c_str(), 0, 10);
		else if (name == "reftime")
			res->reftimeMerger.merge(types::Reftime::decodeString(i->second));
	}
	return res;
}

#ifdef HAVE_LUA
int Stats::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the Stats reference from the userdata value
	luaL_checkudata(L, udataidx, "arki_summarystats");
	void* userdata = lua_touserdata(L, udataidx);
	const Stats& v = **(const Stats**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "count")
	{
		lua_pushnumber(L, v.count);
		return 1;
	}
	else if (name == "reftime")
	{
		arki::Item<types::Reftime> reftime(v.reftimeMerger.makeReftime());
		reftime->lua_push(L);
		return 1;
	}
	else
	{
		lua_pushnil(L);
		return 1;
	}
}
static int arkilua_lookup_summarystats(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, Stats::lua_lookup, 2);
	return 1;
}
void Stats::lua_push(lua_State* L) const
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	const Stats** s = (const Stats**)lua_newuserdata(L, sizeof(const Stats*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki_summarystats"));
	{
		// If the metatable wasn't previously created, create it now
		// Set the __index metamethod to the lookup function
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup_summarystats);
		lua_settable(L, -3);
	}

	lua_setmetatable(L, -2);
}
#endif

static types::MetadataType summaryItemType(
	types::TYPE_SUMMARYITEM, 2, "summaryitem",
	(types::MetadataType::item_decoder)(&oldsummary::Item::decode),
	(types::MetadataType::string_decoder)(&oldsummary::Item::decodeString));

static types::MetadataType summaryStatsType(
	types::TYPE_SUMMARYSTATS, 2, "summarystats",
	(types::MetadataType::item_decoder)(&oldsummary::Stats::decode),
	(types::MetadataType::string_decoder)(&oldsummary::Stats::decodeString));

}

OldSummary::~OldSummary()
{
}

void OldSummary::reset()
{
	m_filename.clear();
	m_items.clear();
}

bool OldSummary::operator==(const OldSummary& m) const
{
	//if (m_filename != m.m_filename) return false;
	if (m_items != m.m_items) return false;
	return true;
}

void OldSummary::clear()
{
	reset();
	m_filename = "(in memory)";
}

size_t OldSummary::count() const
{
	using namespace oldsummary;

	size_t res = 0;
	for (map_t::const_iterator i = m_items.begin();
			i != m_items.end(); ++i)
		res += i->second->count;
	return res;
}

unsigned long long OldSummary::size() const
{
	using namespace oldsummary;

	unsigned long long res = 0;
	for (map_t::const_iterator i = m_items.begin();
			i != m_items.end(); ++i)
		res += i->second->size;
	return res;
}

template<typename T>
inline bool increment(const set<T>& s, typename set<T>::const_iterator& i)
{
	// Inrement the iterator, wrapping it around when it reaches the end.
	// If wraparound happens, returns true as if it were a carry flag
	++i;
	if (i == s.end())
	{
		i = s.begin();
		return true;
	}

	return false;
}

void OldSummary::add(const Metadata& md)
{
	using namespace oldsummary;

	// Generate one item per combination of metadata fields in the metadata
	// (this is a cartesian product, and can potentially explode, but it rarely
	// does as basically all metadata will only have one item per data type)

	bool added = false;
	UItem<const types::Reftime> reftime;
	arki::Item<oldsummary::Item> item(new oldsummary::Item);
	for (Metadata::const_iterator i = md.begin(); i != md.end(); ++i)
	{
		if (i->first == types::TYPE_REFTIME)
			reftime = i->second->upcast<types::Reftime>();
		if (oldsummary::Item::accepts(i->first))
		{
			item->insert(make_pair(i->first, i->second));
			added = true;
		}
	}

	if (added)
	{
		// Add it
		map_t::iterator it = m_items.find(item);
		if (it == m_items.end())
			m_items.insert(make_pair(item, new Stats(1, md.dataSize(), reftime)));
		else
			it->second->merge(1, md.dataSize(), reftime);
	}
}

void OldSummary::add(const Item<oldsummary::Item>& i, const Item<oldsummary::Stats>& s)
{
	using namespace oldsummary;

	map_t::iterator it = m_items.find(i);
	if (it == m_items.end())
		m_items.insert(make_pair(i, s));
	else
		it->second->merge(s);
}

void OldSummary::add(const OldSummary& s)
{
	using namespace oldsummary;

	for (map_t::const_iterator i = s.m_items.begin();
			i != s.m_items.end(); ++i)
		add(i->first, i->second);
}

Item<types::Reftime> OldSummary::getReferenceTime() const
{
	using namespace oldsummary;

	types::reftime::Collector merger;

	for (map_t::const_iterator i = m_items.begin();
			i != m_items.end(); ++i)
		merger.merge(i->second->reftimeMerger);

	return merger.makeReftime();
}

Item<types::BBox> OldSummary::getConvexHull() const
{
#ifdef HAVE_GEOS
	GeometryFactory gf;
	auto_ptr< vector<Geometry*> > geoms(new vector<Geometry*>);
	try {
		for (map_t::const_iterator i = m_items.begin();
				i != m_items.end(); ++i)
		{
			UItem<> ui = i->first->get(types::TYPE_BBOX);
			if (!ui.defined()) continue;
			Geometry* g = ui.upcast<types::BBox>()->geometry(gf);
			//cerr << "Got: " << g << g->getGeometryType() << endl;
			if (!g) continue;
			//cerr << "Adding: " << g->toString() << endl;
			geoms->push_back(g);
		}
	} catch (...) {
		// If an exception was thrown, delete all the Geometry objects that we created
		for (vector<Geometry*>::iterator i = geoms->begin(); i != geoms->end(); ++i)
			delete *i;
		return types::bbox::INVALID::create();
	}
	auto_ptr<GeometryCollection> gc(gf.createGeometryCollection(geoms.release()));
	auto_ptr<Geometry> hull(gc->convexHull());
	//cerr << "Hull: " << hull->toString() << endl;
	auto_ptr<CoordinateSequence> hcs(hull->getCoordinates());
	vector< pair<float, float> > points;
	for (size_t i = 0; i < hcs->getSize(); ++i)
	{
		const Coordinate& c = hcs->getAt(i);
		points.push_back(make_pair(c.y, c.x));
	}
	return types::bbox::HULL::create(points);
#else
	return bbox::INVALID::create();
#endif
}

bool OldSummary::readYaml(std::istream& in, const std::string& filename)
{
	using namespace oldsummary;
	using namespace str;

	m_filename = filename;

	arki::UItem<oldsummary::Item> item;

	YamlStream yamlStream;
	for (YamlStream::const_iterator i = yamlStream.begin(in);
			i != yamlStream.end(); ++i)
	{
		types::Code type = types::parseCodeName(i->first);
		switch (type)
		{
			case types::TYPE_SUMMARYITEM: item = oldsummary::Item::decodeString(i->second); break;
			case types::TYPE_SUMMARYSTATS: add(item, Stats::decodeString(i->second)); break;
			default:
				throw wibble::exception::Consistency("parsing file " + filename,
					"cannot handle element " + fmt(type));
		}
	}
	return !in.eof();
}

void OldSummary::write(std::ostream& out, const std::string& filename) const
{
	// Prepare the encoded data
	string encoded = encode();

	// Write out
	out.write(encoded.data(), encoded.size());
	if (out.fail())
		throw wibble::exception::File(filename, "writing " + str::fmt(encoded.size()) + " bytes to the file");
}

void OldSummary::writeYaml(std::ostream& out, const Formatter* f) const
{
	using namespace oldsummary;

	for (map_t::const_iterator i = m_items.begin();
			i != m_items.end(); ++i)
	{
		out << "SummaryItem:" << endl;
		i->first->toYaml(out, 2, f);
		out << "SummaryStats:" << endl;
		i->second->toYaml(out, 2);
	}
}

bool OldSummary::read(std::istream& in, const std::string& filename)
{
	wibble::sys::Buffer buf;
	string signature;
	unsigned version;
	if (!types::readBundle(in, filename, buf, signature, version))
		return false;

	// Ensure first 2 bytes are SU
	if (signature != "SU")
		throw wibble::exception::Consistency("parsing file " + filename, "oldsummary entry does not start with 'SU'");

	read(buf, version, filename);

	return true;
}

void OldSummary::read(const wibble::sys::Buffer& buf, unsigned version, const std::string& filename)
{
	using namespace oldsummary;

	reset();
	m_filename = filename;

	// Check version and ensure we can decode
	if (version != 0)
		throw wibble::exception::Consistency("parsing file " + m_filename, "version of the file is " + str::fmt(version) + " but I can only decode version 0");

	UItem<oldsummary::Item> item;
	
	// Parse the various elements
	const unsigned char* end = (const unsigned char*)buf.data() + buf.size();
	for (const unsigned char* cur = (const unsigned char*)buf.data(); cur < end; )
	{
		const unsigned char* el_start = cur;
		size_t el_len = end - cur;
		types::Code el_type = types::decodeEnvelope(el_start, el_len);

		switch (el_type)
		{
			case types::TYPE_SUMMARYITEM: {
				item = oldsummary::Item::decode(el_start, el_len, m_filename);
				break;
			}
			case types::TYPE_SUMMARYSTATS:
				add(item, Stats::decode(el_start, el_len, m_filename));
				break;
			default:
				throw wibble::exception::Consistency("parsing file " + m_filename, "cannot handle element " + str::fmt(el_type));
		}

		cur = el_start + el_len;
	}
}

std::string OldSummary::encode() const
{
	using namespace oldsummary;
	using namespace utils;

	// Encode the various information
	string encoded;
	for (map_t::const_iterator i = m_items.begin();
			i != m_items.end(); ++i)
	{
		encoded += i->first.encode();
		encoded += i->second.encode();
	}
	// Prepend header
	return "SU" + encodeUInt(0, 2) + encodeUInt(encoded.size(), 4) + encoded;
}

std::ostream& operator<<(std::ostream& o, const OldSummary& s)
{
	s.writeYaml(o);
	return o;
}

#ifdef HAVE_LUA
namespace oldsummary {
	struct LuaIter
	{
		const OldSummary& s;
		OldSummary::const_iterator iter;

		LuaIter(const OldSummary& s) : s(s), iter(s.begin()) {}
		
		// Lua iterator for summaries
		static int iterate(lua_State* L)
		{
			LuaIter& i = **(LuaIter**)lua_touserdata(L, lua_upvalueindex(1));
			if (i.iter != i.s.end())
			{
				i.iter->first->lua_push(L);
				i.iter->second->lua_push(L);
				++i.iter;
				return 2;
			}
			else
				return 0;  /* no more values to return */
		}

		static int gc (lua_State *L) {
			LuaIter* i = *(LuaIter**)lua_touserdata(L, 1);
			if (i) delete i;
			return 0;
		}
	};
}

int OldSummary::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the OldSummary reference from the userdata value
	luaL_checkudata(L, udataidx, "arki_summary");
	void* userdata = lua_touserdata(L, udataidx);
	const OldSummary& v = **(const OldSummary**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "count")
	{
		lua_pushinteger(L, v.count());
		return 1;
	}
	else if (name == "size")
	{
		lua_pushinteger(L, v.size());
		return 1;
	}
	else if (name == "iter")
	{
		// Iterate

		/* create a userdatum to store an iterator structure address */
		oldsummary::LuaIter**d = (oldsummary::LuaIter**)lua_newuserdata(L, sizeof(oldsummary::LuaIter*));

		// Get the metatable for the iterator
		if (luaL_newmetatable(L, "arki_summary_iter"));
		{
			/* set its __gc field */
			lua_pushstring(L, "__gc");
			lua_pushcfunction(L, oldsummary::LuaIter::gc);
			lua_settable(L, -3);
		}

		// Set its metatable
		lua_setmetatable(L, -2);

		// Instantiate the iterator
		*d = new oldsummary::LuaIter(v);

		// Creates and returns the iterator function (its sole upvalue, the
		// iterator userdatum, is already on the stack top
		lua_pushcclosure(L, oldsummary::LuaIter::iterate, 1);
		return 1;
	}
	else
	{
		lua_pushnil(L);
		return 1;
	}
}
static int arkilua_lookup_summary(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, OldSummary::lua_lookup, 2);
	return 1;
}
void OldSummary::lua_push(lua_State* L) const
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	const OldSummary** s = (const OldSummary**)lua_newuserdata(L, sizeof(const OldSummary*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki_summary"));
	{
		// If the metatable wasn't previously created, create it now
		// Set the __index metamethod to the lookup function
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup_summary);
		lua_settable(L, -3);
	}

	lua_setmetatable(L, -2);
}
#endif

}

// vim:set ts=4 sw=4:
