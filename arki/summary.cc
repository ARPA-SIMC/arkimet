/*
 * summary - Handle a summary of a group of summary
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/summary.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/utils/codec.h>
#include <arki/formatter.h>
#include <arki/types/utils.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/timerange.h>
#include <arki/types/area.h>
#include <arki/utils/geosdef.h>
#include <arki/utils/compress.h>
// #include <arki/utils/lua.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/buffer.h>

#include <fstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

//#define DEBUG_THIS
#ifdef DEBUG_THIS
#warning Debug enabled
#include <iostream>
#define codeclog(...) cerr << __VA_ARGS__ << endl
#else
#define codeclog(...) do {} while(0)
#endif

using namespace std;
using namespace wibble;
using namespace arki::utils::codec;

namespace arki {

namespace summary {

// Metadata Scan Order
static const types::Code mso[] = {
		types::TYPE_ORIGIN,
		types::TYPE_PRODUCT,
  		types::TYPE_LEVEL,
  		types::TYPE_TIMERANGE,
  		types::TYPE_AREA,
		types::TYPE_ENSEMBLE,
		types::TYPE_BBOX,
		types::TYPE_RUN };
static const size_t msoSize = sizeof(mso) / sizeof(types::Code);
static int* msoSerLen = 0;

// Reverse mapping
static int* itemMsoMap = 0;
static size_t itemMsoMapSize = 0;

static void buildMsoSerLen()
{
	if (msoSerLen) return;
	msoSerLen = new int[msoSize];
	for (size_t i = 0; i < msoSize; ++i)
	{
		const types::MetadataType* mdt = types::MetadataType::get(mso[i]);
		msoSerLen[i] = mdt ? mdt->serialisationSizeLen : 0;
	}
}
static void buildItemMsoMap()
{
	if (itemMsoMap) return;
	
	for (size_t i = 0; i < summary::msoSize; ++i)
		if ((size_t)summary::mso[i] > itemMsoMapSize)
			itemMsoMapSize = (size_t)summary::mso[i];
	itemMsoMapSize += 1;
	
	itemMsoMap = new int[itemMsoMapSize];
	for (size_t i = 0; i < itemMsoMapSize; ++i)
		itemMsoMap[i] = -1;
	for (size_t i = 0; i < summary::msoSize; ++i)
		itemMsoMap[(size_t)summary::mso[i]] = i;
}

types::Code Visitor::codeForPos(size_t pos)
{
	return mso[pos];
}

int Visitor::posForCode(types::Code code)
{
	if ((size_t)code >= itemMsoMapSize) return -1;
	return itemMsoMap[(size_t)code];
}

Node::Node() {}
Node::Node(const Metadata& m, size_t scanpos)
{
	for (size_t i = scanpos; i < msoSize; ++i)
		md.push_back(m.get(mso[i]));
	stats = new Stats(1, m.dataSize(), m.get(types::TYPE_REFTIME).upcast<types::reftime::Position>());
}
Node::Node(const Metadata& m, const refcounted::Pointer<Stats>& st, size_t scanpos)
{
	for (size_t i = scanpos; i < msoSize; ++i)
		md.push_back(m.get(mso[i]));
	stats = st;
}
Node::Node(const std::vector< UItem<> >& m, const refcounted::Pointer<Stats>& st, size_t scanpos)
{
	if (scanpos == 0)
		md = m;
	else
		for (size_t i = scanpos; i < msoSize; ++i)
			md.push_back(m[i]);
	stats = st;
}
Node::~Node()
{
}

bool Node::visitStats(StatsVisitor& visitor) const
{
	if (stats.ptr())
	{
		if (!visitor(stats))
			return false;
	}
	else
	{
		for (map< UItem<>, refcounted::Pointer<Node> >::const_iterator i = children.begin();
				i != children.end(); ++i)
			if (!i->second->visitStats(visitor))
				return false;
	}
	return true;
}

bool Node::visitItem(size_t msoidx, ItemVisitor& visitor) const
{
	if (msoidx < md.size())
	{
		return visitor(md[msoidx]);
	} else if (msoidx == md.size()) {
		for (std::map< UItem<>, refcounted::Pointer<Node> >::const_iterator i = children.begin();
				i != children.end(); ++i)
			if (!visitor(i->first))
				return false;
	} else {
		for (std::map< UItem<>, refcounted::Pointer<Node> >::const_iterator i = children.begin();
				i != children.end(); ++i)
			if (!i->second->visitItem(msoidx - md.size() - 1, visitor))
				return false;
	}
	return true;
}

bool Node::visit(Visitor& visitor, std::vector< UItem<> >& visitmd, size_t scanpos) const
{
	for (size_t i = 0; i < md.size(); ++i)
		visitmd[scanpos + i] = md[i];
	if (scanpos + md.size() == msoSize)
	{
		if (!visitor(visitmd, refcounted::Pointer<Stats>(stats)))
			return false;
	} else {
		for (std::map< UItem<>, refcounted::Pointer<Node> >::const_iterator i = children.begin();
				i != children.end(); ++i)
		{
			visitmd[scanpos + md.size()] = i->first;
			if (!i->second->visit(visitor, visitmd, scanpos + md.size() + 1))
				return false;
		}
	}
	return true;
}

bool Node::visitFiltered(const Matcher& matcher, Visitor& visitor, std::vector< UItem<> >& visitmd, size_t scanpos) const
{
	// Check if the matcher is ok with this node; if it's not, return true right away
	if (matcher.m_impl)
	{
		const matcher::AND& mand = *matcher.m_impl;
		for (size_t i = 0; i < md.size(); ++i)
		{
			matcher::AND::const_iterator j = mand.find(mso[scanpos + i]);
			if (j == mand.end()) continue;
			if (!md[i].defined()) return true;
			if (!j->second->matchItem(md[i])) return true;
		}
		if (stats.ptr())
		{
			matcher::AND::const_iterator rm = mand.find(types::TYPE_REFTIME);
			if (rm != mand.end() && !rm->second->matchItem(stats->reftimeMerger.makeReftime()))
				return true;
		}
	}

	for (size_t i = 0; i < md.size(); ++i)
		visitmd[scanpos + i] = md[i];
	if (scanpos + md.size() == msoSize)
	{
		if (!visitor(visitmd, refcounted::Pointer<Stats>(stats)))
			return false;
	} else {
		const matcher::Implementation* m = 0;
		if (matcher.m_impl)
		{
			const matcher::AND& mand = *matcher.m_impl;
			// Check with the matcher if we should bother about this child node
			matcher::AND::const_iterator j = mand.find(mso[scanpos + md.size()]);
			if (j != mand.end())
				m = j->second.ptr();
		}

		for (std::map< UItem<>, refcounted::Pointer<Node> >::const_iterator i = children.begin();
				i != children.end(); ++i)
		{
			if (m)
			{
				if (!i->first.defined()) continue;
				if (!m->matchItem(i->first)) continue;
			}
			visitmd[scanpos + md.size()] = i->first;
			if (!i->second->visitFiltered(matcher, visitor, visitmd, scanpos + md.size() + 1))
				return false;
		}
	}
	return true;
}

void Node::add(const Metadata& m, size_t scanpos)
{
	// Ensure that we can store it
	for (size_t i = 0; i < md.size(); ++i)
	{
		UItem<> mditem = m.get(mso[scanpos + i]);
		if (mditem != md[i])
		{
			// Split
			refcounted::Pointer<Node> child(new Node);
			for (size_t j = i + 1; j < md.size(); ++j)
				child->md.push_back(md[j]);
			child->children = children;
			child->stats = stats;
			stats = 0;
			children.clear();
			children[md[i]] = child;
			children[mditem] = new Node(m, scanpos + i + 1);
			md.resize(i);
			return;
		}
	}

	if (scanpos + md.size() == msoSize)
	{
		stats->merge(1, m.dataSize(), m.get(types::TYPE_REFTIME).upcast<types::reftime::Position>());
	} else {
		UItem<> childmd = m.get(mso[scanpos + md.size()]);
		std::map< UItem<>, refcounted::Pointer<Node> >::iterator i = children.find(childmd);
		if (i == children.end())
			children[childmd] = new Node(m, scanpos + md.size() + 1);
		else
			i->second->add(m, scanpos + md.size() + 1);
	}
}

void Node::add(const Metadata& m, const refcounted::Pointer<Stats>& st, size_t scanpos)
{
	// Ensure that we can store it
	for (size_t i = 0; i < md.size(); ++i)
	{
		UItem<> mditem = m.get(mso[scanpos + i]);
		if (mditem != md[i])
		{
			// Split
			refcounted::Pointer<Node> child(new Node);
			for (size_t j = i + 1; j < md.size(); ++j)
				child->md.push_back(md[j]);
			child->children = children;
			child->stats = stats;
			stats = 0;
			children.clear();
			children[md[i]] = child;
			children[mditem] = new Node(m, st, scanpos + i + 1);
			md.resize(i);
			return;
		}
	}

	if (scanpos + md.size() == msoSize)
	{
		stats->merge(st);
	} else {
		UItem<> childmd = m.get(mso[scanpos + md.size()]);
		std::map< UItem<>, refcounted::Pointer<Node> >::iterator i = children.find(childmd);
		if (i == children.end())
			children[childmd] = new Node(m, st, scanpos + md.size() + 1);
		else
			i->second->add(m, st, scanpos + md.size() + 1);
	}
}
void Node::add(const std::vector< UItem<> >& m, const refcounted::Pointer<Stats>& st, size_t scanpos)
{
	// Ensure that we can store it
	for (size_t i = 0; i < md.size(); ++i)
	{
		UItem<> mditem = m[scanpos + i];
		if (mditem != md[i])
		{
			// Split
			refcounted::Pointer<Node> child(new Node);
			for (size_t j = i + 1; j < md.size(); ++j)
				child->md.push_back(md[j]);
			child->children = children;
			child->stats = stats;
			stats = 0;
			children.clear();
			children[md[i]] = child;
			children[mditem] = new Node(m, st, scanpos + i + 1);
			md.resize(i);
			return;
		}
	}

	if (scanpos + md.size() == msoSize)
	{
		if (!stats.ptr())
			stats = st;
		else
			stats->merge(st);
	} else {
		UItem<> childmd = m[scanpos + md.size()];
		std::map< UItem<>, refcounted::Pointer<Node> >::iterator i = children.find(childmd);
		if (i == children.end())
			children[childmd] = new Node(m, st, scanpos + md.size() + 1);
		else
			i->second->add(m, st, scanpos + md.size() + 1);
	}
}

static void addEncodedItem(Encoder& enc, const UItem<>& item, size_t msoIdx)
{
	size_t serlen = msoSerLen[msoIdx];
	if (item.defined())
	{
		// FIXME: use a varint
		string buf;
		Encoder bufenc(buf);
		item->encodeWithoutEnvelope(bufenc);
		enc.addUInt(buf.size(), serlen);
		enc.addString(buf);
		codeclog("Item " << msoIdx << " encoded, size " << buf.size() << " in " << serlen << " bytes");
	} else {
		codeclog("Item " << msoIdx << " is undef, encoding 0 in " << serlen << " bytes");
		enc.addUInt(0, serlen);
	}
}

void Node::encode(Encoder& enc, const UItem<>& lead, size_t scanpos) const
{
	// Add the set of metadata that we have
	if (scanpos == 0)
	{
		codeclog("Encode no lead stripe size " << md.size());
		enc.addUInt(md.size(), 2);
	} else {
		codeclog("Encode stripe size " << md.size());
		enc.addUInt(md.size() + 1, 2);
		codeclog("Encode lead");
		addEncodedItem(enc, lead, scanpos - 1);
	}
	for (size_t i = 0; i < md.size(); ++i)
		addEncodedItem(enc, md[i], scanpos + i);

	if (stats.ptr())
	{
		// If we have stats, we are a terminal node
		enc.addUInt(0, 2);
		string buf;
		Encoder bufenc(buf);
		stats->encodeWithoutEnvelope(bufenc);
		codeclog("Encode 0 children, stats " << buf.size() << "b");
		enc.addUInt(buf.size(), 2);
		enc.addString(buf);
	} else {
		codeclog("Encode " << children.size() << " children");
		// Else, encode the children
		enc.addUInt(children.size(), 2);
		for (std::map< UItem<>, refcounted::Pointer<Node> >::const_iterator i = children.begin();
			i != children.end(); ++i)
		{
			codeclog("Encode child");
			i->second->encode(enc, i->first, scanpos + md.size() + 1);
		}
	}
}

static UItem<> decodeUItem(size_t msoIdx, const unsigned char*& buf, size_t& len)
{
	using namespace utils::codec;
	codeclog("Decode metadata item " << msoIdx << " len " << msoSerLen[msoIdx]);
	size_t itemsizelen = msoSerLen[msoIdx];
	ensureSize(len, itemsizelen, "Metadata item size");
	size_t itemsize = decodeUInt(buf, itemsizelen);
	buf += itemsizelen; len -= itemsizelen;
	codeclog("  item size " << itemsize);

	if (itemsize)
	{
		ensureSize(len, itemsize, "Metadata item");
		UItem<> res = decodeInner(mso[msoIdx], buf, itemsize);
		buf += itemsize; len -= itemsize;
		return res;
	} else
		return UItem<>();
}

static void decodeFollowing(Node& node, const unsigned char*& buf, size_t& len, size_t scanpos)
{
	codeclog("Decode following");
	using namespace utils::codec;
	ensureSize(len, 2, "Number of child stripes");
	size_t childnum = decodeUInt(buf, 2);
	buf += 2; len -= 2;

	codeclog("Found " << childnum << " children");
	if (childnum)
	{
		for (size_t i = 0; i < childnum; ++i)
		{
			// Decode the metadata stripe length
			ensureSize(len, 2, "Metadata stripe size");
			size_t stripelen = decodeUInt(buf, 2);
			buf += 2; len -= 2;
			codeclog("Decoding child " << i << " stripelen " << stripelen);
		
			if (stripelen == 0)
				throw wibble::exception::Consistency("decoding summary", "child node has empty metadata stripe");
			codeclog("Decoding lead item");
			// The first item in the stripe is the key in the child map
			UItem<> key = decodeUItem(scanpos, buf, len);
			refcounted::Pointer<Node> child = new Node;
			// Decode the rest of the metadata stripe
			for (size_t i = 1; i < stripelen; ++i)
			{
				codeclog("Decoding element " << i << " in stripe");
				child->md.push_back(decodeUItem(scanpos + i, buf, len));
			}
			node.children[key] = child;
			decodeFollowing(*child, buf, len, scanpos + stripelen);
		}
	} else {
		ensureSize(len, 2, "Summary statistics size");
		size_t statlen = decodeUInt(buf, 2);
		buf += 2; len -= 2;
		codeclog("Decoding stats in " << statlen << "b");
		ensureSize(len, 2, "Summary statistics");
		node.stats = Stats::decode(buf, statlen);
		buf += statlen; len -= statlen;
	}
}

refcounted::Pointer<Node> Node::decode(const unsigned char* buf, size_t len, size_t scanpos)
{
	using namespace utils::codec;
	codeclog("Start decoding scanpos " << scanpos);

	// Decode the metadata stripe length
	ensureSize(len, 2, "Metadata stripe size");
	size_t stripelen = decodeUInt(buf, 2);
	buf += 2; len -= 2;

	codeclog("Stripe size " << stripelen);

	refcounted::Pointer<Node> node = new Node;

	// Decode the metadata stripe
	for (size_t i = 0; i < stripelen; ++i)
		node->md.push_back(decodeUItem(scanpos + i, buf, len));

	// Decode the rest of the node
	decodeFollowing(*node, buf, len, scanpos + node->md.size());

	return node;
}

int Node::compare(const Node& node) const
{
	// Compare metadata stripes
	if (int res = md.size() - node.md.size()) return res;
	for (size_t i = 0; i < md.size(); ++i)
		if (int res = md[i].compare(node.md[i])) return res;

	// Compare children
	if (int res = children.size() - node.children.size()) return res;

	std::map< UItem<>, refcounted::Pointer<Node> >::const_iterator a = children.begin();
	std::map< UItem<>, refcounted::Pointer<Node> >::const_iterator b = node.children.begin();
	for (; a != children.end(); ++a, ++b)
	{
		if (int res = a->first.compare(b->first)) return res;
		if (int res = a->second->compare(*b->second)) return res;
	}

	if (stats.ptr() == node.stats.ptr()) return 0;
	if (stats.ptr() == 0 && node.stats.ptr() != 0) return -1;
	if (stats.ptr() != 0 && node.stats.ptr() == 0) return 1;
	return stats->compare(*node.stats);
}

#if 0
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
		summary::LuaItemIter**d = (summary::LuaItemIter**)lua_newuserdata(L, sizeof(summary::LuaItemIter*));

		// Get the metatable for the iterator
		if (luaL_newmetatable(L, "arki_summary_item_iter"));
		{
			/* set its __gc field */
			lua_pushstring(L, "__gc");
			lua_pushcfunction(L, summary::LuaItemIter::gc);
			lua_settable(L, -3);
		}

		// Set its metatable
		lua_setmetatable(L, -2);

		// Instantiate the iterator
		*d = new summary::LuaItemIter(v);

		// Creates and returns the iterator function (its sole upvalue, the
		// iterator userdatum, is already on the stack top
		lua_pushcclosure(L, summary::LuaItemIter::iterate, 1);
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

void Stats::merge(const refcounted::Pointer<Stats>& s)
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
const char* Stats::lua_type_name() const { return "arki.types.summary.stats"; }


void Stats::encodeWithoutEnvelope(Encoder& enc) const
{
	arki::Item<types::Reftime> reftime(reftimeMerger.makeReftime());
	enc.addUInt(count, 4);
	enc.addString(reftime.encode());
	enc.addULInt(size, 8);
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

refcounted::Pointer<Stats> Stats::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;

	refcounted::Pointer<Stats> res(new Stats);

	// First decode the count
	if (len < 4)
		throw wibble::exception::Consistency("parsing summary stats", "summary stats has size " + str::fmt(len) + " but at least 4 bytes are needed");
	res->count = decodeUInt(buf, 4);
	buf += 4; len -= 4;

	// Then decode the reftime
	const unsigned char* el_start = buf;
	size_t el_len = len;
	types::Code el_type = types::decodeEnvelope(el_start, el_len);
	if (el_type == types::TYPE_REFTIME)
		res->reftimeMerger.merge(types::Reftime::decode(el_start, el_len));
	else
		throw wibble::exception::Consistency("parsing summary stats", "cannot handle element " + str::fmt(el_type));
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

refcounted::Pointer<Stats> Stats::decodeString(const std::string& str)
{
	using namespace str;

	refcounted::Pointer<Stats> res(new Stats);
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
	luaL_checkudata(L, udataidx, "arki.summarystats");
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
	if (luaL_newmetatable(L, "arki.summarystats"));
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

static types::MetadataType summaryStatsType(
	types::TYPE_SUMMARYSTATS, 2, "summarystats",
	(types::MetadataType::item_decoder)(&summary::Stats::decode),
	(types::MetadataType::string_decoder)(&summary::Stats::decodeString));

}

#ifdef HAVE_LUA
#warning To be implemented
namespace {
#if 0
	struct LuaIter
	{
		const Summary& s;
		Summary::const_iterator iter;

		LuaIter(const Summary& s) : s(s), iter(s.begin()) {}
		
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
#endif
struct LuaPusher: public summary::Visitor
{
	lua_State* L;
	int index;

	LuaPusher(lua_State* L) : L(L), index(1) {}
	virtual bool operator()(const std::vector< UItem<> >& md, const refcounted::Pointer<summary::Stats>& stats)
	{
		// Table with the couple
		lua_newtable(L);
		// Table with the items
		lua_newtable(L);
		// Push the items
		for (size_t i = 0; i < md.size(); ++i)
		{
			// Name
			string name = str::tolower(types::formatCode(summary::mso[i]));
			lua_pushlstring(L, name.data(), name.size());
			// Value
			if (md[i].defined())
				md[i]->lua_push(L);
			else
				lua_pushnil(L);
			lua_rawset(L, -3);
		}
		lua_rawseti(L, -2, 1);
		// Push the stats
		stats->lua_push(L);
		lua_rawseti(L, -2, 2);
		// Push the couple into the table we are populating
		lua_rawseti(L, -2, index++);
		return true;
	}
};

}

typedef utils::lua::ManagedUD<Summary> SummaryUD;

static void arkilua_getmetatable(lua_State* L);

static int arkilua_count(lua_State* L)
{
	Summary* s = Summary::lua_check(L, 1);
	luaL_argcheck(L, s != NULL, 1, "`arki.summary' expected");
	lua_pushinteger(L, s->count());
	return 1;
}

static int arkilua_size(lua_State* L)
{
	Summary* s = Summary::lua_check(L, 1);
	luaL_argcheck(L, s != NULL, 1, "`arki.summary' expected");
	lua_pushinteger(L, s->size());
	return 1;
}

static int arkilua_data(lua_State* L)
{
	Summary* s = Summary::lua_check(L, 1);
	luaL_argcheck(L, s != NULL, 1, "`arki.summary' expected");
	// Return a big table with a dump of the summary inside
	lua_newtable(L);
	LuaPusher pusher(L);
	s->visit(pusher);
	return 1;
}

static int arkilua_filter(lua_State* L)
{
	// utils::lua::dumpstack(L, "FILTER", cerr);
	Summary* s = Summary::lua_check(L, 1);
	luaL_argcheck(L, s != NULL, 1, "`arki.summary' expected");
	Matcher m = Matcher::lua_check(L, 2);
	if (lua_gettop(L) > 2)
	{
		// s.filter(matcher, s1)
		Summary* s1 = Summary::lua_check(L, 3);
		luaL_argcheck(L, s1 != NULL, 3, "`arki.summary' expected");
		s->filter(m, *s1);
		return 0;
	} else {
		SummaryUD::create(L, new Summary(s->filter(m)), true);
		return 1;
	}
}

// Make a new summary
// Memory management of the copy will be done by Lua
static int arkilua_new(lua_State* L)
{
	// Make a new copy
	SummaryUD::create(L, new Summary, true);

	// Set the summary for the userdata
	arkilua_getmetatable(L);
	lua_setmetatable(L, -2);

	return 1;
}

// Make a copy of the metadata.
// Memory management of the copy will be done by Lua
static int arkilua_copy(lua_State* L)
{
	Summary* s = Summary::lua_check(L, 1);
	luaL_argcheck(L, s != NULL, 1, "`arki.summary' expected");

	// Make a new copy
	SummaryUD* ud = SummaryUD::create(L, new Summary, true);
	*(ud->val) = *s;

	// Set the summary for the userdata
	arkilua_getmetatable(L);
	lua_setmetatable(L, -2);

	return 1;
}


static int arkilua_gc (lua_State *L)
{
	SummaryUD* ud = (SummaryUD*)luaL_checkudata(L, 1, "arki.summary");
	if (ud != NULL && ud->collected)
		delete ud->val;
	return 0;
}

static int arkilua_tostring (lua_State *L)
{
	lua_pushstring(L, "summary");
	return 1;
}


#if 0
	else if (name == "iter")
	{
		// Iterate

		/* create a userdatum to store an iterator structure address */
		summary::LuaIter**d = (summary::LuaIter**)lua_newuserdata(L, sizeof(summary::LuaIter*));

		// Get the metatable for the iterator
		if (luaL_newmetatable(L, "arki_summary_iter"));
		{
			/* set its __gc field */
			lua_pushstring(L, "__gc");
			lua_pushcfunction(L, summary::LuaIter::gc);
			lua_settable(L, -3);
		}

		// Set its metatable
		lua_setmetatable(L, -2);

		// Instantiate the iterator
		*d = new summary::LuaIter(v);

		// Creates and returns the iterator function (its sole upvalue, the
		// iterator userdatum, is already on the stack top
		lua_pushcclosure(L, summary::LuaIter::iterate, 1);
		return 1;
	}
#endif

static const struct luaL_reg summaryclasslib [] = {
	{ "new", arkilua_new },
	{ NULL, NULL }
};

static const struct luaL_reg summarylib [] = {
	{ "count", arkilua_count },
	{ "size", arkilua_size },
	{ "data", arkilua_data },
	{ "filter", arkilua_filter },
	{ "copy", arkilua_copy },
	{ "__gc", arkilua_gc },
	{ "__tostring", arkilua_tostring },
	{ NULL, NULL }
};

static void arkilua_getmetatable(lua_State* L)
{
	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki.summary"));
	{
		// If the metatable wasn't previously created, create it now
		lua_pushstring(L, "__index");
		lua_pushvalue(L, -2);  /* pushes the metatable */
		lua_settable(L, -3);  /* metatable.__index = metatable */

		// Load normal methods
		luaL_register(L, NULL, summarylib);
	}
}

void Summary::lua_push(lua_State* L)
{
	SummaryUD::create(L, this, false);
	arkilua_getmetatable(L);
	lua_setmetatable(L, -2);
}

void Summary::lua_openlib(lua_State* L)
{
	luaL_register(L, "arki.summary", summaryclasslib);
}

Summary* Summary::lua_check(lua_State* L, int idx)
{
	SummaryUD* ud = (SummaryUD*)luaL_checkudata(L, idx, "arki.summary");
	if (ud) return ud->val;
	return NULL;
}
#endif

bool Summary::operator==(const Summary& m) const
{
	if (!root.ptr() && !m.root.ptr()) return true;
	if ((root.ptr() ? 1 : 0) - (m.root.ptr() ? 1 : 0)) return false;
	return root->compare(*m.root) == 0;
}

void Summary::clear()
{
	m_filename.clear();
	root = 0;
}

namespace summary {
struct StatsCount : public StatsVisitor
{
	size_t count;
	StatsCount() : count(0) {}
	virtual bool operator()(const refcounted::Pointer<Stats>& stats)
	{
		count += stats->count;
		return true;
	}
};
struct StatsSize : public StatsVisitor
{
	unsigned long long size;
	StatsSize() : size(0) {}
	virtual bool operator()(const refcounted::Pointer<Stats>& stats)
	{
		size += stats->size;
		return true;
	}
};
struct StatsReftime : public StatsVisitor
{
	types::reftime::Collector merger;

	virtual bool operator()(const refcounted::Pointer<Stats>& stats)
	{
		merger.merge(stats->reftimeMerger);
		return true;
	}
};
#ifdef HAVE_GEOS
struct StatsHull : public ItemVisitor
{
	ARKI_GEOS_GEOMETRYFACTORY& gf;
	vector<ARKI_GEOS_GEOMETRY*>* geoms;
	std::set< Item<types::Area> > seen;

	StatsHull(ARKI_GEOS_GEOMETRYFACTORY& gf) : gf(gf), geoms(new vector<ARKI_GEOS_GEOMETRY*>) {}
	virtual ~StatsHull()
	{
		if (geoms)
		{
			for (vector<ARKI_GEOS_GEOMETRY*>::iterator i = geoms->begin(); i != geoms->end(); ++i)
				delete *i;
			delete geoms;
		}
	}

	virtual bool operator()(const arki::UItem<>& area)
	{
		if (!area.defined()) return true;
		Item<types::Area> a = area.upcast<types::Area>();
		pair<set< Item<types::Area> >::iterator, bool> i = seen.insert(a);
		if (i.second)
		{
			const ARKI_GEOS_GEOMETRY* g = a->bbox();
			//cerr << "Got: " << g << g->getGeometryType() << endl;
			if (!g) return true;
			//cerr << "Adding: " << g->toString() << endl;
			geoms->push_back(g->clone());
		}
		return true;
	}

	auto_ptr<ARKI_GEOS_GEOMETRY> makeBBox()
	{
		if (geoms->empty())
			return auto_ptr<ARKI_GEOS_GEOMETRY>(0);

		auto_ptr<ARKI_GEOS_NS::GeometryCollection> gc(gf.createGeometryCollection(geoms));
		geoms = 0;
		return auto_ptr<ARKI_GEOS_GEOMETRY>(gc->convexHull());
	}
};
#endif
}

size_t Summary::count() const
{
	if (!root.ptr()) return 0;
	summary::StatsCount counter;
	root->visitStats(counter);
	return counter.count;
}

unsigned long long Summary::size() const
{
	if (!root.ptr()) return 0;
	summary::StatsSize counter;
	root->visitStats(counter);
	return counter.size;
}

Item<types::Reftime> Summary::getReferenceTime() const
{
	summary::StatsReftime counter;
	if (root.ptr())
		root->visitStats(counter);
	return counter.merger.makeReftime();
}

namespace {
struct ResolveVisitor : public summary::Visitor
{
	std::vector<ItemSet>& result;
	std::vector<types::Code> codes;

	ResolveVisitor(std::vector<ItemSet>& result, const Matcher& m) : result(result)
	{
		for (matcher::AND::const_iterator i = m.m_impl->begin(); i != m.m_impl->end(); ++i)
			codes.push_back(i->first);
	}
	virtual ~ResolveVisitor() {}
	virtual bool operator()(const std::vector< UItem<> >& md, const refcounted::Pointer<summary::Stats>& stats)
	{
		ItemSet is;
		for (std::vector<types::Code>::const_iterator i = codes.begin();
				i != codes.end(); ++i)
		{
			int pos = posForCode(*i);
			if (!md[pos].defined()) return true;
			is.set(md[pos]);
		}
		// Insertion sort, as we expect to have lots of duplicates
		std::vector<ItemSet>::iterator i = std::lower_bound(result.begin(), result.end(), is);
		if (i == result.end())
			result.push_back(is);
		else if (*i != is)
			result.insert(i, is);

		return true;
	}
};
}

std::vector<ItemSet> Summary::resolveMatcher(const Matcher& matcher) const
{
	if (matcher.empty()) return std::vector<ItemSet>();

	std::vector<ItemSet> result;
	ResolveVisitor visitor(result, matcher);
	visitFiltered(matcher, visitor);

	return result;
}

void Summary::resolveMatcher(const Matcher& matcher, std::vector<ItemSet>& res) const
{
	if (matcher.empty()) return;

	ResolveVisitor visitor(res, matcher);
	visitFiltered(matcher, visitor);
}

std::auto_ptr<ARKI_GEOS_GEOMETRY> Summary::getConvexHull(ARKI_GEOS_GEOMETRYFACTORY& gf) const
{
#ifdef HAVE_GEOS
	summary::StatsHull merger(gf);
	if (root.ptr())
	{
		summary::buildItemMsoMap();
		root->visitItem(summary::itemMsoMap[types::TYPE_AREA], merger);
	}
	return merger.makeBBox();
#else
	return std::auto_ptr<ARKI_GEOS_GEOMETRY>(0);
#endif
}


bool Summary::read(int fd, const std::string& filename)
{
	wibble::sys::Buffer buf;
	string signature;
	unsigned version;
	if (!types::readBundle(fd, filename, buf, signature, version))
		return false;

	// Ensure first 2 bytes are SU
	if (signature != "SU")
		throw wibble::exception::Consistency("parsing file " + filename, "summary entry does not start with 'SU'");

	read(buf, version, filename);

	return true;
}

bool Summary::read(std::istream& in, const std::string& filename)
{
	wibble::sys::Buffer buf;
	string signature;
	unsigned version;
	if (!types::readBundle(in, filename, buf, signature, version))
		return false;

	// Ensure first 2 bytes are SU
	if (signature != "SU")
		throw wibble::exception::Consistency("parsing file " + filename, "summary entry does not start with 'SU'");

	read(buf, version, filename);

	return true;
}

void Summary::read(const wibble::sys::Buffer& buf, unsigned version, const std::string& filename)
{
	using namespace summary;

	clear();
	m_filename = filename;

	// Check version and ensure we can decode
	switch (version)
	{
		case 1:
			// Standard summary
			if (buf.size() == 0) return;
			summary::buildMsoSerLen();

			root = summary::Node::decode((const unsigned char*)buf.data(), buf.size());
			break;
		case 2: {
			// LZO compressed summary
			if (buf.size() == 0) return;
			summary::buildMsoSerLen();

			// Read uncompressed size
			ensureSize(buf.size(), 4, "uncompressed item size");
			uint32_t uncsize = decodeUInt((const unsigned char*)buf.data(), 4);

			sys::Buffer decomp = utils::compress::unlzo((const unsigned char*)buf.data() + 4, buf.size() - 4, uncsize);
			root = summary::Node::decode((const unsigned char*)decomp.data(), decomp.size());
			break;
		}
		default:
			throw wibble::exception::Consistency("parsing file " + m_filename, "version of the file is " + str::fmt(version) + " but I can only decode version 1 or 2");
	}
}

std::string Summary::encode(bool compressed) const
{
	using namespace utils::codec;

	// Build the serialisation tables if it has not been done yet
	summary::buildMsoSerLen();

	// Encode
	string inner;
	Encoder innerenc(inner);
	if (root.ptr()) root->encode(innerenc);

	// Prepend header
	string res;
	Encoder enc(res);
	enc.addString("SU", 2);
	if (compressed)
	{
		sys::Buffer comp = utils::compress::lzo(inner.data(), inner.size());
		if (comp.size() + 4 >= inner.size())
		{
			// No point in compressing
			enc.addUInt(1, 2);
			enc.addUInt(inner.size(), 4);
			enc.addString(inner);
		} else {
			// Compression makes sense
			enc.addUInt(2, 2);
			// Add total size
			enc.addUInt(comp.size() + 4, 4);
			// Add uncompressed size
			enc.addUInt(inner.size(), 4);
			// Add compressed data
			enc.addBuffer(comp);
		}
	} else {
		enc.addUInt(1, 2);
		enc.addUInt(inner.size(), 4);
		enc.addString(inner);
	}
	return res;
}

void Summary::write(std::ostream& out, const std::string& filename) const
{
	// Prepare the encoded data
	string encoded = encode(true);

	// Write out
	out.write(encoded.data(), encoded.size());
	if (out.fail())
		throw wibble::exception::File(filename, "writing " + str::fmt(encoded.size()) + " bytes to the file");
}

void Summary::writeAtomically(const std::string& fname)
{
	// Write summary to disk
	string enc = encode(true);
	string tmpfile = fname + ".tmp" + str::fmt(getpid());
	int fd = open(tmpfile.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
	if (fd == -1)
		throw wibble::exception::File(tmpfile, "creating temporary file for the summary");
	try {
		int res = ::write(fd, enc.data(), enc.size());
		if (res < 0 || (unsigned)res != enc.size())
			throw wibble::exception::File(tmpfile, "writing " + str::fmt(enc.size()) + " bytes to the file");

		if (close(fd) == -1)
		{
			fd = -1;
			throw wibble::exception::File(tmpfile, "closing file");
		}
		fd = -1;
		if (rename(tmpfile.c_str(), fname.c_str()) == -1)
			throw wibble::exception::System("Renaming " + tmpfile + " into " + fname);
	} catch (...) {
		if (fd != -1)
			close(fd);
		throw;
	}
}

namespace summary {
struct YamlPrinter : public Visitor
{
	ostream& out;
	string indent;
	const Formatter* f;

	YamlPrinter(ostream& out, size_t indent, const Formatter* f = 0) : out(out), indent(indent, ' '), f(f) {}
	virtual bool operator()(const std::vector< UItem<> >& md, const refcounted::Pointer<Stats>& stats)
	{
		// Write the metadata items
		out << "SummaryItem:" << endl;
		for (vector< UItem<> >::const_iterator i = md.begin(); i != md.end(); ++i)
		{
			if (!i->defined()) continue;

			out << indent << str::ucfirst((*i)->tag()) << ": ";
			(*i)->writeToOstream(out);
			if (f) out << "\t# " << (*f)(*i);
			out << endl;
		}

		// Write the stats
		out << "SummaryStats:" << endl;
		arki::Item<types::Reftime> reftime(stats->reftimeMerger.makeReftime());
		out << indent << "Count: " << stats->count << endl;
		out << indent << "Size: " << stats->size << endl;
		out << indent << "Reftime: " << reftime << endl;
		return true;
	}
};
}

bool Summary::visit(summary::Visitor& visitor) const
{
	if (!root.ptr()) return true;

	summary::buildItemMsoMap();
	vector< UItem<> > visitmd(summary::msoSize, UItem<>());
	return root->visit(visitor, visitmd);
}

bool Summary::visitFiltered(const Matcher& matcher, summary::Visitor& visitor) const
{
	if (!root.ptr()) return true;

	summary::buildItemMsoMap();
	vector< UItem<> > visitmd(summary::msoSize, UItem<>());
	return root->visitFiltered(matcher, visitor, visitmd);
}

void Summary::writeYaml(std::ostream& out, const Formatter* f) const
{
	if (!root.ptr()) return;

	summary::YamlPrinter printer(out, 2, f);
	visit(printer);
}

static vector< UItem<> > decodeItem(const std::string& str)
{
	using namespace str;

	vector< UItem<> > itemmd(summary::msoSize, UItem<>());
	stringstream in(str, ios_base::in);
	YamlStream yamlStream;
	for (YamlStream::const_iterator i = yamlStream.begin(in);
			i != yamlStream.end(); ++i)
	{
		types::Code type = types::parseCodeName(i->first);
		if ((size_t)type >= summary::itemMsoMapSize || summary::itemMsoMap[(size_t)type] == -1)
			throw wibble::exception::Consistency("parsing summary item", "found element of unsupported type " + types::formatCode(type));
		itemmd[summary::itemMsoMap[(size_t)type]] = types::decodeString(type, i->second);
	}
	return itemmd;
}

void Summary::readFile(const std::string& fname)
{
	// Read all the metadata
	std::ifstream in;
	in.open(fname.c_str(), ios::in);
	if (!in.is_open() || in.fail())
		throw wibble::exception::File(fname, "opening file for reading");

	read(in, fname);

	in.close();
}

bool Summary::readYaml(std::istream& in, const std::string& filename)
{
	using namespace summary;
	using namespace str;

	summary::buildItemMsoMap();

	m_filename = filename;

	vector< UItem<> > itemmd;

	YamlStream yamlStream;
	for (YamlStream::const_iterator i = yamlStream.begin(in);
			i != yamlStream.end(); ++i)
	{
		types::Code type = types::parseCodeName(i->first);
		switch (type)
		{
			case types::TYPE_SUMMARYITEM: itemmd = decodeItem(i->second); break;
			case types::TYPE_SUMMARYSTATS:
			{
				if (root.ptr())
					root->add(itemmd, Stats::decodeString(i->second));
				else
					root = new Node(itemmd, Stats::decodeString(i->second));
				break;
			}
			default:
				throw wibble::exception::Consistency("parsing file " + filename,
					"cannot handle element " + fmt(type));
		}
	}
	return !in.eof();
}

void Summary::add(const Metadata& md)
{
	if (root.ptr())
		root->add(md);
	else
		root = new summary::Node(md);
}

void Summary::add(const Metadata& md, const refcounted::Pointer<summary::Stats>& s)
{
	if (root.ptr())
		root->add(md, s);
	else
		root = new summary::Node(md, s);
}

namespace summary {
struct SummaryMerger : public Visitor
{
	refcounted::Pointer<summary::Node>& root;

	SummaryMerger(refcounted::Pointer<summary::Node>& root) : root(root) {}
	virtual bool operator()(const std::vector< UItem<> >& md, const refcounted::Pointer<Stats>& stats)
	{
		if (!root.ptr())
			root = new Node(md, stats);
		else
			root->add(md, stats);
		return true;
	}
};
}

void Summary::add(const Summary& s)
{
	if (s.root.ptr())
	{
		summary::SummaryMerger merger(root);
		vector< UItem<> > visitmd(summary::msoSize, UItem<>());
		s.root->visit(merger, visitmd);
	}
}

namespace summary {
struct MatchVisitor : public Visitor
{
	bool res;
	MatchVisitor() : res(false) {}
	virtual bool operator()(const std::vector< UItem<> >& md, const refcounted::Pointer<Stats>& stats)
	{
		res = true;
		// Stop iteration
		return false;
	}
};
}
bool Summary::match(const Matcher& matcher) const
{
	if (!root.ptr() && matcher.m_impl) return false;

	summary::MatchVisitor visitor;
	vector< UItem<> > visitmd(summary::msoSize, UItem<>());
	root->visitFiltered(matcher, visitor, visitmd);
	return visitor.res;
}

void Summary::filter(const Matcher& matcher, Summary& result) const
{
	if (root.ptr())
	{
		summary::SummaryMerger merger(result.root);
		vector< UItem<> > visitmd(summary::msoSize, UItem<>());
		root->visitFiltered(matcher, merger, visitmd);
	}
}

Summary Summary::filter(const Matcher& matcher) const
{
	Summary res;
	filter(matcher, res);
	return res;
}

std::ostream& operator<<(std::ostream& o, const Summary& s)
{
	s.writeYaml(o);
	return o;
}


}

// vim:set ts=4 sw=4:
