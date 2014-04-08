/*
 * metadata - Handle arkimet metadata
 *
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/types/value.h>
#include <arki/formatter.h>
#include <arki/utils/codec.h>
#include <arki/utils/compress.h>
#include <arki/utils/fd.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include <arki/iotrace.h>
#include <arki/scan/any.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <unistd.h>
#include <fstream>
#include <cstdlib>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {

namespace metadata {

ReadContext::ReadContext() {}

ReadContext::ReadContext(const std::string& pathname)
    : basedir(str::dirname(pathname)), pathname(pathname)
{
}

ReadContext::ReadContext(const std::string& pathname, const std::string& basedir)
    : basedir(basedir), pathname(pathname)
{
}

}

static inline void ensureSize(size_t len, size_t req, const char* what)
{
	using namespace str;
	if (len < req)
		throw wibble::exception::Consistency(string("parsing ") + what, "size is " + fmt(len) + " but we need at least "+fmt(req)+" for the "+what+" style");
}

template<typename ITER>
static std::string encodeItemList(const ITER& begin, const ITER& end)
{
	string res;
	for (ITER i = begin; i != end; ++i)
		res += i->encode();
	return res;
}



Metadata::~Metadata()
{
}

std::vector< Item<types::Note> > Metadata::notes() const
{
	std::vector< Item<types::Note> > res;
	const unsigned char* buf = (const unsigned char*)m_notes.data();
	const unsigned char* end = buf + m_notes.size();
	for (const unsigned char* cur = buf; cur < end; )
	{
		const unsigned char* el_start = cur;
		size_t el_len = end - cur;
		// Fixme: replace with types::decode when fully available
		types::Code el_type = types::decodeEnvelope(el_start, el_len);

		if (el_type != types::TYPE_NOTE)
			throw wibble::exception::Consistency(
					"decoding binary encoded notes",
					"item type is not a note");
		res.push_back(types::Note::decode(el_start, el_len));
		cur = el_start + el_len;
	}
	return res;
}

const std::string& Metadata::notes_encoded() const
{
	return m_notes;
}

void Metadata::set_notes(const std::vector< Item<types::Note> >& notes)
{
	m_notes.clear();
	for (std::vector< Item<types::Note> >::const_iterator i = notes.begin(); i != notes.end(); ++i)
		add_note(*i);
}

void Metadata::set_notes_encoded(const std::string& notes)
{
	m_notes = notes;
}

void Metadata::add_note(const Item<types::Note>& note)
{
	m_notes += note.encode();
}

void Metadata::reset()
{
    m_vals.clear();
    m_notes.clear();
    source.clear();
}

bool Metadata::operator==(const Metadata& m) const
{
    if (!ItemSet::operator==(m)) return false;

    if (m_notes != m.m_notes) return false;
    if (source != m.source) return false;
    return true;
}

int Metadata::compare(const Metadata& m) const
{
	if (int res = ItemSet::compare(m)) return res;
	
	// TODO: replace with source.compare() when ready
	//if (source < m.source) return -1;
	//if (source > m.source) return 1;
	//if (notes < m.notes) return -1;
	//if (notes > m.notes) return 1;
	return 0;
}

void Metadata::create()
{
    reset();
}

bool Metadata::read(istream& in, const std::string& filename, bool readInline)
{
	wibble::sys::Buffer buf;
	string signature;
	unsigned version;
	if (!types::readBundle(in, filename, buf, signature, version))
		return false;

	// Ensure first 2 bytes are MD or !D
	if (signature != "MD")
		throw wibble::exception::Consistency("parsing file " + filename, "metadata entry does not start with 'MD'");

	read(buf, version, filename);

	// If the source is inline, then the data follows the metadata
	if (readInline && source->style() == types::Source::INLINE)
		readInlineData(in, filename);

	return true;
}

bool Metadata::read(const unsigned char*& buf, size_t& len, const metadata::ReadContext& rc)
{
	const unsigned char* obuf;
	size_t olen;
	string signature;
	unsigned version;

	if (!types::readBundle(buf, len, rc.pathname, obuf, olen, signature, version))
		return false;

	// Ensure the signature is MD or !D
	if (signature != "MD")
		throw wibble::exception::Consistency("parsing file " + rc.pathname, "metadata entry does not start with 'MD'");
	
	read(obuf, olen, version, rc);

	return true;
}

void Metadata::read(const wibble::sys::Buffer& buf, unsigned version, const metadata::ReadContext& rc)
{
	read((const unsigned char*)buf.data(), buf.size(), version, rc);
}

void Metadata::read(const unsigned char* buf, size_t len, unsigned version, const metadata::ReadContext& rc)
{
    reset();

    // Check version and ensure we can decode
    if (version != 0)
        throw wibble::exception::Consistency("parsing file " + rc.pathname, "version of the file is " + str::fmt(version) + " but I can only decode version 0");
	
	// Parse the various elements
	const unsigned char* end = buf + len;
	for (const unsigned char* cur = buf; cur < end; )
	{
		const unsigned char* el_start = cur;
		size_t el_len = end - cur;
		// Fixme: replace with types::decode when fully available
		types::Code el_type = types::decodeEnvelope(el_start, el_len);

		switch (el_type)
		{
			case types::TYPE_NOTE: m_notes += string((const char*)cur, (el_start + el_len) - cur); break;
			case types::TYPE_SOURCE: source = types::Source::decodeRelative(el_start, el_len, rc.basedir); break;
			default:
				m_vals.insert(make_pair(el_type, types::decodeInner(el_type, el_start, el_len)));
				break;
		}

		cur = el_start + el_len;
	}
}

void Metadata::readInlineData(std::istream& in, const std::string& filename)
{
    // If the source is inline, then the data follows the metadata
    if (source->style() == types::Source::INLINE)
    {
        size_t size = source.upcast<types::source::Inline>()->size;
        wibble::sys::Buffer buf(size);

        iotrace::trace_file(filename, 0, size, "read inline data");

        // Read the inline data
        in.read((char*)buf.data(), size);
        if (in.fail())
            throw wibble::exception::File(filename, "reading "+str::fmt(size)+" bytes");

        source->setCachedData(buf);
    }
}

bool Metadata::readYaml(std::istream& in, const std::string& filename)
{
	using namespace str;
	reset();

	YamlStream yamlStream;
	for (YamlStream::const_iterator i = yamlStream.begin(in);
			i != yamlStream.end(); ++i)
	{
		types::Code type = types::parseCodeName(i->first);
		string val = trim(i->second);
		switch (type)
		{
			case types::TYPE_NOTE: add_note(types::Note::decodeString(val)); break;
			case types::TYPE_SOURCE: source = types::Source::decodeString(val); break;
			default:
				m_vals.insert(make_pair(type, types::decodeString(type, val)));
		}
	}
	return !in.eof();
}

void Metadata::write(std::ostream& out, const std::string& filename) const
{
	// Prepare the encoded data
	string encoded = encode();

	// Write out
	out.write(encoded.data(), encoded.size());
	if (out.fail())
		throw wibble::exception::File(filename, "writing " + str::fmt(encoded.size()) + " bytes to the file");

    // If the source is inline, then the data follows the metadata
    if (source->style() == types::Source::INLINE)
    {
        wibble::sys::Buffer buf = source->getData();
        out.write((const char*)buf.data(), buf.size());
        if (out.fail())
            throw wibble::exception::File(filename, "writing " + str::fmt(buf.size()) + " bytes to the file");
    }
}

void Metadata::write(int outfd, const std::string& filename) const
{
    // Prepare the encoded data
    string encoded = encode();

    // Write out
    utils::fd::write_all(outfd, encoded);

    // If the source is inline, then the data follows the metadata
    if (source->style() == types::Source::INLINE)
    {
        wibble::sys::Buffer buf = source->getData();
        utils::fd::write_all(outfd, buf.data(), buf.size());
    }
}

template<typename LIST>
static void writeYamlList(std::ostream& out, const std::string& name, const LIST& l, const Formatter* formatter)
{
	if (l.empty()) return;
	out << name << ": ";
	if (l.size() == 1)
		if (formatter)
			out << *l.begin() << "\t# " << (*formatter)(*l.begin()) << endl;
		else
			out << *l.begin() << endl;
	else
	{
		out << endl;
		for (typename LIST::const_iterator i = l.begin(); i != l.end(); ++i)
			if (formatter)
				out << " " << *i << "\t# " << (*formatter)(*i) << endl;
			else
				out << " " << *i << endl;
	}
}
template<typename LIST>
static void writeYamlList(std::ostream& out, const std::string& name, const LIST& l)
{
	if (l.empty()) return;
	out << name << ": ";
	if (l.size() == 1)
		out << *l.begin() << endl;
	else
	{
		out << endl;
		for (typename LIST::const_iterator i = l.begin(); i != l.end(); ++i)
			out << " " << *i << endl;
	}
}
void Metadata::writeYaml(std::ostream& out, const Formatter* formatter) const
{
	if (source.defined())
		out << "Source: " << source << endl;
	for (const_iterator i = begin(); i != end(); ++i)
	{
		out << str::ucfirst(i->second->tag()) << ": ";
		i->second->writeToOstream(out);
		if (formatter)
			out << "\t# " << (*formatter)(i->second);
		out << endl;
	}

	writeYamlList(out, "Note", notes());
}

void Metadata::serialise(Emitter& e, const Formatter* f) const
{
    e.start_mapping();
    e.add("i");
    e.start_list();
    if (source.defined())
        source->serialise(e, f);
    for (const_iterator i = begin(); i != end(); ++i)
        i->second->serialise(e, f);
    e.end_list();
    e.add("n");
    e.start_list();
    std::vector< Item<types::Note> > n = notes();
    for (std::vector< Item<types::Note> >::const_iterator i = n.begin();
            i != n.end(); ++i)
        (*i)->serialise(e, f);
    e.end_list();
    e.end_mapping();

    e.add_break();

    // If the source is inline, then the data follows the metadata
    if (source->style() == types::Source::INLINE)
    {
        wibble::sys::Buffer buf = source->getData();
        e.add_raw(buf);
    }
}

void Metadata::read(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    // Parse items
    const List& items = val["i"].want_list("parsing metadata item list");
    for (std::vector<const Node*>::const_iterator i = items.val.begin(); i != items.val.end(); ++i)
    {
        Item<> item = types::decodeMapping((*i)->want_mapping("parsing metadata item"));
        if (item->serialisationCode() == types::TYPE_SOURCE)
            source = item.upcast<types::Source>();
        else
            set(item);
    }

    // Parse notes
    const List& notes = val["n"].want_list("parsing metadata notes list");
    for (std::vector<const Node*>::const_iterator i = notes.val.begin(); i != notes.val.end(); ++i)
    {
        Item<> item = types::decodeMapping((*i)->want_mapping("parsing metadata item"));
        if (item->serialisationCode() == types::TYPE_NOTE)
            add_note(item.upcast<types::Note>());
    }
}

string Metadata::encode() const
{
	using namespace utils::codec;
	// Encode the various information
	string encoded;
	for (const_iterator i = begin(); i != end(); ++i)
		encoded += i->second.encode();
	encoded += m_notes;
	if (source.defined())
		encoded += source.encode();

	string res;
	Encoder enc(res);
	// Prepend header
	enc.addString("MD");
	enc.addUInt(0, 2);
	enc.addUInt(encoded.size(), 4);
	enc.addString(encoded);
	return res;
}

bool Metadata::hasData() const
{
    return source.defined() && source->hasData();
}

wibble::sys::Buffer Metadata::getData() const
{
    if (!source.defined())
        throw wibble::exception::Consistency("retrieving data", "data source is not defined");

    if (source->hasData())
        return source->getData();

    // Se if we have a value set
    UItem<types::Value> value = get<types::Value>();
    if (value.defined())
    {
        wibble::sys::Buffer buf = arki::scan::reconstruct(source->format, *this, value->buffer);
        source->setCachedData(buf);
        return buf;
    } else {
        return source->getData();
    }
}

void Metadata::dropCachedData() const
{
    if (source.defined())
        source->dropCachedData();
}

void Metadata::setCachedData(const wibble::sys::Buffer& buf)
{
    if (!source.defined())
        throw wibble::exception::Consistency("setting cached data", "data source is not defined");
    source->setCachedData(buf);
}

size_t Metadata::dataSize() const
{
	switch (source->style())
	{
		case types::Source::BLOB:
			return source.upcast<types::source::Blob>()->size;
		case types::Source::INLINE:
			return source.upcast<types::source::Inline>()->size;
		default:
			return 0;
	}
}

void Metadata::setInlineData(const std::string& format, wibble::sys::Buffer buf)
{
    source = types::source::Inline::create(format, buf);
}

void Metadata::makeInline()
{
	setInlineData(source->format, getData());
}

void Metadata::readFile(const std::string& fname, metadata::Consumer& mdc)
{
    metadata::ReadContext context(fname);
    readFile(context, mdc);
}

void Metadata::readFile(const metadata::ReadContext& file, metadata::Consumer& mdc)
{
    // Read all the metadata
    std::ifstream in;
    in.open(file.pathname.c_str(), ios::in);
    if (!in.is_open() || in.fail())
        throw wibble::exception::File(file.pathname, "opening file for reading");

    readFile(in, file, mdc);

    in.close();
}

void Metadata::readFile(std::istream& in, const metadata::ReadContext& file, metadata::Consumer& mdc)
{
    bool canceled = false;
    wibble::sys::Buffer buf;
    string signature;
    unsigned version;
    Metadata md;
    while (types::readBundle(in, file.pathname, buf, signature, version))
    {
        if (canceled) continue;

        // Ensure first 2 bytes are MD or !D
        if (signature != "MD" && signature != "!D" && signature != "MG")
            throw wibble::exception::Consistency("parsing file " + file.pathname, "metadata entry does not start with 'MD', '!D' or 'MG'");

        if (signature == "MG")
        {
            // Handle metadata group
            iotrace::trace_file(file.pathname, 0, 0, "read metadata group");
            Metadata::readGroup(buf, version, file, mdc);
        } else {
            iotrace::trace_file(file.pathname, 0, 0, "read metadata");
            md.read(buf, version, file);

            // If the source is inline, then the data follows the metadata
            if (md.source->style() == types::Source::INLINE)
                md.readInlineData(in, file.pathname);
            canceled = !mdc(md);
        }
    }
}

void Metadata::readGroup(const wibble::sys::Buffer& buf, unsigned version, const metadata::ReadContext& file, metadata::Consumer& mdc)
{
    // Handle metadata group
    if (version != 0)
        throw wibble::exception::Consistency("parsing file " + file.pathname, "can only decode metadata group version 0 (LZO compressed); found version: " + str::fmt(version));

	// Read uncompressed size
	ensureSize(buf.size(), 4, "uncompressed item size");
	uint32_t uncsize = codec::decodeUInt((const unsigned char*)buf.data(), 4);

	sys::Buffer decomp = utils::compress::unlzo((const unsigned char*)buf.data() + 4, buf.size() - 4, uncsize);
	const unsigned char* ubuf = (const unsigned char*)decomp.data();
	size_t len = decomp.size(); 
	const unsigned char* ibuf;
	size_t ilen;
	string isig;
	unsigned iver;
	bool canceled = false;
	Metadata md;
	while (!canceled && types::readBundle(ubuf, len, file.pathname, ibuf, ilen, isig, iver))
	{
		md.read(ibuf, ilen, iver, file);
		canceled = !mdc(md);
	}
}

void Metadata::flushDataReaders()
{
    types::Source::flushDataReaders();
}

#ifdef HAVE_LUA
namespace {
struct LuaIter
{
	const Metadata& s;
	Metadata::const_iterator iter;

	LuaIter(const Metadata& s) : s(s), iter(s.begin()) {}
	
	// Lua iterator for summaries
	static int iterate(lua_State* L)
	{
		LuaIter& i = **(LuaIter**)lua_touserdata(L, lua_upvalueindex(1));
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
		LuaIter* i = *(LuaIter**)lua_touserdata(L, 1);
		if (i) delete i;
		return 0;
	}
};

struct MetadataUD
{
	Metadata* md;
	bool collected;

	static MetadataUD* create(lua_State* L, Metadata* md, bool collected = false);
};

MetadataUD* MetadataUD::create(lua_State* L, Metadata* md, bool collected)
{
	MetadataUD* ud = (MetadataUD*)lua_newuserdata(L, sizeof(MetadataUD));
	ud->md = md;
	ud->collected = collected;
	return ud;
}

}

static void arkilua_metadatametatable(lua_State* L);

// Make a copy of the metadata.
// Memory management of the copy will be done by Lua
static int arkilua_copy(lua_State* L)
{
	Metadata* md = Metadata::lua_check(L, 1);
	luaL_argcheck(L, md != NULL, 1, "`arki.metadata' expected");

	// Make a new copy
	MetadataUD* ud = MetadataUD::create(L, new Metadata, true);
	*(ud->md) = *md;

	// Set the metatable for the userdata
	arkilua_metadatametatable(L);
	lua_setmetatable(L, -2);

	return 1;
}

// Make a copy of the metadata.
// Memory management of the copy will be done by Lua
static int arkilua_notes(lua_State* L)
{
	Metadata* md = Metadata::lua_check(L, 1);
	luaL_argcheck(L, md != NULL, 1, "`arki.metadata' expected");

	// Return a table with all the notes in the metadata
	std::vector< Item<types::Note> > notes = md->notes();
	lua_createtable(L, notes.size(), 0);
	// Set the array elements
	for (size_t i = 0; i < notes.size(); ++i)
	{
		notes[i]->lua_push(L);
		lua_rawseti(L, -2, i+1);
	}
	return 1;
}

static int arkilua_lookup(lua_State* L)
{
	Metadata* md = Metadata::lua_check(L, 1);
	luaL_argcheck(L, md != NULL, 1, "`arki.metadata' expected");
	const char* skey = lua_tostring(L, 2);
	luaL_argcheck(L, skey != NULL, 2, "`string' expected");
	string key = skey;

	// Get an arbitrary element by name
	types::Code code = types::checkCodeName(key);
	if (code == types::TYPE_INVALID)
	{
		// Delegate lookup to the metatable
		lua_getmetatable(L, 1);
		lua_pushlstring(L, key.data(), key.size());
		lua_gettable(L, -2);
		// utils::lua::dumpstack(L, "postlookup", cerr);
		return 1;
	} else if (code == types::TYPE_SOURCE) {
		// Return the source element
		if (md->source.defined())
			md->source->lua_push(L);
		else
			lua_pushnil(L);
		return 1;
	} else {
		// Return the metadata item
		UItem<> item = md->get(code);
		if (item.defined())
			item->lua_push(L);
		else
			lua_pushnil(L);
		return 1;
	}

#if 0
	else if (key == "iter")
	{
		// Iterate

		/* create a userdatum to store an iterator structure address */
		LuaIter**d = (LuaIter**)lua_newuserdata(L, sizeof(LuaIter*));

		// Get the metatable for the iterator
		if (luaL_newmetatable(L, "arki.metadata_iter"));
		{
			/* set its __gc field */
			lua_pushstring(L, "__gc");
			lua_pushcfunction(L, LuaIter::gc);
			lua_settable(L, -3);
		}

		// Set its metatable
		lua_setmetatable(L, -2);

		// Instantiate the iterator
		*d = new LuaIter(*md);

		// Creates and returns the iterator function (its sole upvalue, the
		// iterator userdatum, is already on the stack top
		lua_pushcclosure(L, LuaIter::iterate, 1);
		return 1;
	}
#endif
}

static int arkilua_newindex(lua_State* L)
{
	Metadata* md = Metadata::lua_check(L, 1);
	luaL_argcheck(L, md != NULL, 1, "`arki.metadata' expected");
	const char* skey = lua_tostring(L, 2);
	luaL_argcheck(L, skey != NULL, 2, "`string' expected");
	string key = skey;
	UItem<> item = types::Type::lua_check(L, 3);
	luaL_argcheck(L, item.defined(), 3, "arki.type.* expected");

	if (key == "source")
	{
		luaL_argcheck(L, item->serialisationCode() == types::TYPE_SOURCE, 3, "arki.type.source expected");
		md->source = item.upcast<types::Source>();
	}
	else if (key == "notes")
	{
		// TODO
#if 0
		//
		// Return a table with all the notes in the metadata
		std::vector< Item<types::Note> > notes = md->notes();
		lua_createtable(L, notes.size(), 0);
		// Set the array elements
		for (size_t i = 0; i < notes.size(); ++i)
		{
			notes[i]->lua_push(L);
			lua_rawseti(L, -2, i+1);
		}
		return 1;
#endif
	}
	else
	{
		// Get an arbitrary element by name
		types::Code code = types::parseCodeName(key);
		if (item->serialisationCode() != code)
		{
			string msg = "arki.type." + types::tag(code) + " expected";
			luaL_argcheck(L, false, 3, msg.c_str());
		}
		md->set(item);
	}
	return 0;
}

static int arkilua_set(lua_State* L)
{
	Metadata* md = Metadata::lua_check(L, 1);
	luaL_argcheck(L, md != NULL, 1, "`arki.metadata' expected");
	UItem<> item = types::Type::lua_check(L, 2);
	luaL_argcheck(L, item.defined(), 2, "arki.type.* expected");
	md->set(item);
	return 0;
}

static int arkilua_unset(lua_State* L)
{
	Metadata* md = Metadata::lua_check(L, 1);
	luaL_argcheck(L, md != NULL, 1, "`arki.metadata' expected");
	const char* skey = lua_tostring(L, 2);
	luaL_argcheck(L, skey != NULL, 2, "`string' expected");
	types::Code code = types::parseCodeName(skey);
	md->unset(code);
	return 0;
}

static int arkilua_data(lua_State* L)
{
    Metadata* md = Metadata::lua_check(L, 1);
    luaL_argcheck(L, md != NULL, 1, "`arki.metadata' expected");
    // Return a big table with a dump of the metadata inside
    lua_newtable(L);

    // Add notes, as a table
    lua_pushstring(L, "notes");
    lua_newtable(L);
    std::vector< Item<types::Note> > n = md->notes();
    for (size_t i = 0; i < n.size(); ++i)
    {
        n[i]->lua_push(L);
        lua_rawseti(L, -2, i+1);
    }
    lua_rawset(L, -3);

    // Add source
    if (md->source.defined())
    {
        lua_pushstring(L, "source");
        md->source->lua_push(L);
        lua_rawset(L, -3);
    }

    // Add the other items
    for (Metadata::const_iterator i = md->begin(); i != md->end(); ++i)
    {
        lua_pushstring(L, i->second->tag().c_str());
        i->second->lua_push(L);
        lua_rawset(L, -3);
    }
    return 1;
}

/*
static int arkilua_clear(lua_State* L)
{
	Metadata* md = Metadata::lua_check(L, 1);
	luaL_argcheck(L, md != NULL, 1, "`arki.metadata' expected");
	md->clear();
	return 0;
}
*/
static int arkilua_tostring(lua_State* L)
{
	Metadata* md = Metadata::lua_check(L, 1);
	luaL_argcheck(L, md != NULL, 1, "`arki.metadata' expected");
	vector<string> bits;
	for (Metadata::const_iterator i = md->begin(); i != md->end(); ++i)
		bits.push_back(str::tolower(str::fmt(i->first)) + ": " + str::fmt(i->second));
	string merged = "{" + str::join(bits.begin(), bits.end(), ", ") + "}";
	lua_pushlstring(L, merged.data(), merged.size());
	return 1;
}

static int arkilua_eq(lua_State* L)
{
    Metadata* md1 = Metadata::lua_check(L, 1);
    luaL_argcheck(L, md1 != NULL, 1, "`arki.metadata' expected");
    Metadata* md2 = Metadata::lua_check(L, 2);
    luaL_argcheck(L, md2 != NULL, 2, "`arki.metadata' expected");

    lua_pushboolean(L, *md1 == *md2);
    return 1;
}

static int arkilua_gc (lua_State *L)
{
	MetadataUD* ud = (MetadataUD*)luaL_checkudata(L, 1, "arki.metadata");
	if (ud != NULL && ud->collected)
		delete ud->md;
	return 0;
}

static const struct luaL_Reg metadatalib [] = {
	{ "__newindex", arkilua_newindex },
	{ "__gc", arkilua_gc },
    { "__eq", arkilua_eq },
	{ "copy", arkilua_copy },
	{ "set", arkilua_set },
	{ "unset", arkilua_unset },
	{ "notes", arkilua_notes },
	{ "data", arkilua_data },
	// { "clear", arkilua_unset },
	{ "__tostring", arkilua_tostring },
	{ NULL, NULL }
};

// Push the arki.metadata metatable on the stack, creating it if needed
static void arkilua_metadatametatable(lua_State* L)
{
	if (luaL_newmetatable(L, "arki.metadata"));
	{
		// If the metatable wasn't previously created, create it now
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup);
		lua_settable(L, -3);  /* metatable.__index = arkilua_lookup */

        // Load normal methods
        utils::lua::add_functions(L, metadatalib);
	}
}

void Metadata::lua_push(lua_State* L)
{
	// The 'metadata' object is a userdata that holds a pointer to this Grib structure
	MetadataUD::create(L, this, false);

	// Set the metatable for the userdata
	arkilua_metadatametatable(L);

	lua_setmetatable(L, -2);
}

Metadata* Metadata::lua_check(lua_State* L, int idx)
{
	MetadataUD* ud = (MetadataUD*)luaL_checkudata(L, idx, "arki.metadata");
	if (ud == NULL) return NULL;
	return ud->md;
}

static int arkilua_new(lua_State* L)
{
	// Create a medatata which is memory managed by Lua
	MetadataUD::create(L, new Metadata, true);

	// Set the metatable for the userdata
	arkilua_metadatametatable(L);
	lua_setmetatable(L, -2);

	return 1;
}

static const struct luaL_Reg classlib [] = {
	{ "new", arkilua_new },
	{ NULL, NULL }
};

void Metadata::lua_openlib(lua_State* L)
{
    utils::lua::add_arki_global_library(L, "metadata", classlib);
}


#endif

}

// vim:set ts=4 sw=4:
