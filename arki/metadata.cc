/*
 * metadata - Handle arkimet metadata
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

#include <arki/metadata.h>
#include <arki/formatter.h>
#include <arki/utils/codec.h>
#include <arki/utils/datareader.h>
#include <arki/utils/compress.h>
#include "config.h"

#include <wibble/exception.h>
#include <wibble/string.h>
#include <fstream>
#include <cstdlib>

#ifdef HAVE_LUA
extern "C" {
#include <lauxlib.h>
#include <lualib.h>
}
#endif

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {

// TODO: @WARNING this is NOT thread safe
static utils::DataReader dataReader;

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
	deleted = false;
	m_filename.clear();
	m_vals.clear();
	m_notes.clear();
	source.clear();
	m_inline_buf = wibble::sys::Buffer();
}

bool Metadata::operator==(const Metadata& m) const
{
	if (!ItemSet::operator==(m)) return false;

	//if (m_filename != m.m_filename) return false;
	if (m_notes != m.m_notes) return false;
	if (source != m.source) return false;
	if (m_inline_buf != m.m_inline_buf) return false;
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
	m_filename = "(in memory)";
}

bool Metadata::read(istream& in, const std::string& filename, bool readInline)
{
	wibble::sys::Buffer buf;
	string signature;
	unsigned version;
	if (!types::readBundle(in, filename, buf, signature, version))
		return false;

	// Ensure first 2 bytes are MD or !D
	if (signature != "MD" && signature != "!D")
		throw wibble::exception::Consistency("parsing file " + filename, "metadata entry does not start with 'MD' or '!D'");

	read(buf, version, filename);

	// If it starts with !D, it's a deleted metadata: take note of it
	deleted = signature[0] == '!';

	// If the source is inline, then the data follows the metadata
	if (readInline && source->style() == types::Source::INLINE)
		readInlineData(in, filename);

	return true;
}

bool Metadata::read(const unsigned char*& buf, size_t& len, const std::string& filename)
{
	const unsigned char* obuf;
	size_t olen;
	string signature;
	unsigned version;

	if (!types::readBundle(buf, len, filename, obuf, olen, signature, version))
		return false;

	// Ensure the signature is MD or !D
	if (signature != "MD" && signature != "!D")
		throw wibble::exception::Consistency("parsing file " + filename, "metadata entry does not start with 'MD' or '!D'");
	
	read(obuf, olen, version, filename);

	// If it starts with !D, it's a deleted metadata: take note of it
	deleted = signature[0] == '!';

	/*
	// If the source is inline, then the data follows the metadata
	if (readInline && source->style() == types::Source::INLINE)
	{
		size_t size = source.upcast<types::source::Inline>()->size;
		m_inline_buf = wibble::sys::Buffer(size);

		// Read the inline data
		in.read((char*)m_inline_buf.data(), size);
		if (in.fail())
			throw wibble::exception::File(filename, "reading "+str::fmt(size)+" bytes");
	}
	*/

	return true;
}

void Metadata::read(const wibble::sys::Buffer& buf, unsigned version, const std::string& filename)
{
	read((const unsigned char*)buf.data(), buf.size(), version, filename);
}

void Metadata::read(const unsigned char* buf, size_t len, unsigned version, const std::string& filename)
{
	reset();
	m_filename = filename;

	// Check version and ensure we can decode
	if (version != 0)
		throw wibble::exception::Consistency("parsing file " + m_filename, "version of the file is " + str::fmt(version) + " but I can only decode version 0");
	
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
			case types::TYPE_SOURCE: source = types::Source::decode(el_start, el_len); break;
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
		m_inline_buf = wibble::sys::Buffer(size);

		// Read the inline data
		in.read((char*)m_inline_buf.data(), size);
		if (in.fail())
			throw wibble::exception::File(filename, "reading "+str::fmt(size)+" bytes");
	}
}

bool Metadata::readYaml(std::istream& in, const std::string& filename)
{
	using namespace str;
	reset();
	m_filename = filename;

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
		out.write((const char*)m_inline_buf.data(), m_inline_buf.size());
		if (out.fail())
			throw wibble::exception::File(filename, "writing " + str::fmt(m_inline_buf.size()) + " bytes to the file");
	}
}

void Metadata::write(int outfd, const std::string& filename) const
{
	// Prepare the encoded data
	string encoded = encode();

	// Write out
	ssize_t res = ::write(outfd, encoded.data(), encoded.size());
	if (res < 0)
		throw wibble::exception::System("writing metadata to " + filename);
	if ((size_t)res != encoded.size())
		throw wibble::exception::Consistency("writing metadata to " + filename, "written only " + str::fmt(res) + " bytes out of " + str::fmt(encoded.size()));

	// If the source is inline, then the data follows the metadata
	if (source->style() == types::Source::INLINE)
	{
		ssize_t res = ::write(outfd, m_inline_buf.data(), m_inline_buf.size());
		if (res < 0)
			throw wibble::exception::System("writing data to " + filename);
		if ((size_t)res != m_inline_buf.size())
			throw wibble::exception::Consistency("writing data to " + filename, "written only " + str::fmt(res) + " bytes out of " + str::fmt(m_inline_buf.size()));
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
	enc.addString(deleted ? "!D" : "MD");
	enc.addUInt(0, 2);
	enc.addUInt(encoded.size(), 4);
	enc.addString(encoded);
	return res;
}

wibble::sys::Buffer Metadata::getData() const
{
	if (m_inline_buf.size())
		return m_inline_buf;

	switch (source->style())
	{
		case types::Source::BLOB:
		{
			Item<types::source::Blob> blob = source.upcast<types::source::Blob>();

			// Compute the input file name
			string file = blob->filename;
			if (!sys::fs::access(file.c_str(), F_OK))
				file = wibble::str::joinpath(str::dirname(m_filename), blob->filename);

			// Read the data
			m_inline_buf.resize(blob->size);
			dataReader.read(file, blob->offset, blob->size, m_inline_buf.data());

			return m_inline_buf;
		}
		case types::Source::INLINE:
			return m_inline_buf;
		default:
			throw wibble::exception::Consistency("retrieving data", "data source type is " + types::Source::formatStyle(source->style()) + " but I can only handle BLOB or INLINE");
	}
}

void Metadata::dropCachedData()
{
	if (source.defined() && source->style() == types::Source::BLOB)
		m_inline_buf = wibble::sys::Buffer();
}

void Metadata::setCachedData(const wibble::sys::Buffer& buf)
{
	m_inline_buf = buf;
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

void Metadata::setInlineData(const std::string& format, const wibble::sys::Buffer& buf)
{
	source = types::source::Inline::create(format, buf.size());
	m_inline_buf = buf;
}

void Metadata::resetInlineData()
{
	if (source->style() == types::Source::INLINE) return;
	m_inline_buf = wibble::sys::Buffer();
}

void Metadata::makeInline()
{
	setInlineData(source->format, getData());
}

void Metadata::prependPath(const std::string& path)
{
	source = source.upcast<types::source::Blob>()->prependPath(path);
}

void MetadataStream::checkMetadata()
{
	using namespace utils::codec;

	if (buffer.size() < 8) return;

	// Ensure first 2 bytes are MD
	if (buffer[0] != 'M' || buffer[1] != 'D')
		throw wibble::exception::Consistency("checking partial buffer", "buffer contains data that is not encoded metadata");

	// Get version from next 2 bytes
	unsigned int version = codec::decodeUInt((const unsigned char*)buffer.data()+2, 2);
	// Get length from next 4 bytes
	unsigned int len = codec::decodeUInt((const unsigned char*)buffer.data()+4, 4);

	// Check if we have a full metadata, in that case read it, remove it
	// from the beginning of the string, then consume it it
	if (buffer.size() < 8 + len)
		return;

	md.read((const unsigned char*)buffer.data() + 8, len, version, streamname);

	buffer = buffer.substr(len + 8);
	if (md.source->style() == types::Source::INLINE)
	{
		Item<types::source::Inline> inl = md.source.upcast<types::source::Inline>();
		dataToGet = inl->size;
		state = DATA;
		checkData();
	} else {
		consumer(md);
	}
}

void MetadataStream::checkData()
{
	if (buffer.size() >= dataToGet)
	{
		wibble::sys::Buffer buf(buffer.data(), dataToGet);
		buffer = buffer.substr(dataToGet);
		dataToGet = 0;
		state = METADATA;
		md.setInlineData(md.source->format, buf);
		consumer(md);
		checkMetadata();
	}
}

void MetadataStream::readData(const void* buf, size_t size)
{
	buffer.append((const char*)buf, size);

	switch (state)
	{
		case METADATA: checkMetadata(); break;
		case DATA: checkData(); break;
	}
}

void Metadata::readFile(const std::string& fname, MetadataConsumer& mdc)
{
	// Read all the metadata
	std::ifstream in;
	in.open(fname.c_str(), ios::in);
	if (!in.is_open() || in.fail())
		throw wibble::exception::File(fname, "opening file for reading");

	readFile(in, fname, mdc);

	in.close();
}

void Metadata::readFile(std::istream& in, const std::string& fname, MetadataConsumer& mdc)
{
	bool canceled = false;
	wibble::sys::Buffer buf;
	string signature;
	unsigned version;
	Metadata md;
	while (types::readBundle(in, fname, buf, signature, version))
	{
		if (canceled) continue;

		// Ensure first 2 bytes are MD or !D
		if (signature != "MD" && signature != "!D" && signature != "MG")
			throw wibble::exception::Consistency("parsing file " + fname, "metadata entry does not start with 'MD', '!D' or 'MG'");

		if (signature == "MG")
		{
			// Handle metadata group
			if (version != 0)
				throw wibble::exception::Consistency("parsing file " + fname, "can only decode metadata group version 0 (LZO compressed); found version: " + str::fmt(version));
			
			// Read uncompressed size
			ensureSize(buf.size(), 4, "uncompressed item size");
			uint32_t uncsize = codec::decodeUInt((const unsigned char*)buf.data(), 4);

			sys::Buffer decomp = utils::compress::unlzo((const unsigned char*)buf.data() + 4, buf.size() - 4, uncsize);
			const unsigned char* buf = (const unsigned char*)decomp.data();
			size_t len = decomp.size(); 
			const unsigned char* ibuf;
			size_t ilen;
			string isig;
			unsigned iver;
			while (!canceled && types::readBundle(buf, len, fname, ibuf, ilen, isig, iver))
			{
				md.read(ibuf, ilen, iver, fname);
				canceled = !mdc(md);
			}
		} else {
			md.read(buf, version, fname);

			// If it starts with !D, it's a deleted metadata: take note of it
			md.deleted = signature[0] == '!';

			// If the source is inline, then the data follows the metadata
			if (md.source->style() == types::Source::INLINE)
				md.readInlineData(in, fname);
			canceled = !mdc(md);
		}
	}
}

void Metadata::flushDataReaders()
{
	dataReader.flush();
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
	luaL_getmetatable(L, "arki.metadata");
	lua_setmetatable(L, -2);

	return 1;
}

static int arkilua_lookup(lua_State* L)
{
	Metadata* md = Metadata::lua_check(L, 1);
	luaL_argcheck(L, md != NULL, 1, "`arki.metadata' expected");
	const char* skey = lua_tostring(L, 2);
	luaL_argcheck(L, skey != NULL, 2, "`string' expected");
	string key = skey;

	if (key == "source")
	{
		// Return the source element
		if (md->source.defined())
			md->source->lua_push(L);
		else
			lua_pushnil(L);
		return 1;
	}
	else if (key == "notes")
	{
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
	else if (key == "deleted")
	{
		// Return true if the metadata is marked deleted, else false
		lua_pushboolean(L, md->deleted);
		return 1;
	}
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
	else if (key == "copy")
	{
		lua_pushcfunction(L, arkilua_copy);
		return 1;
	}
	else
	{
		// Get an arbitrary element by name
		types::Code code = types::parseCodeName(key);
		UItem<> item = md->get(code);
		if (item.defined())
			item->lua_push(L);
		else
			lua_pushnil(L);
		return 1;
	}
}

static int arkilua_gc (lua_State *L)
{
	MetadataUD* ud = (MetadataUD*)luaL_checkudata(L, 1, "arki.metadata");
	if (ud != NULL && ud->collected)
		delete ud->md;
	return 0;
}

void Metadata::lua_push(lua_State* L)
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	MetadataUD::create(L, this, false);

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki.metadata"));
	{
		// If the metatable wasn't previously created, create it now
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup);
		lua_settable(L, -3);  /* metatable.__index = arkilua_lookup */

		lua_pushstring(L, "__gc");
		lua_pushcfunction(L, arkilua_gc);
		lua_settable(L, -3);  /* metatable.__gc = arkilua_gc */
	}

	lua_setmetatable(L, -2);
}

Metadata* Metadata::lua_check(lua_State* L, int idx)
{
	MetadataUD* ud = (MetadataUD*)luaL_checkudata(L, idx, "arki.metadata");
	if (ud == NULL) return NULL;
	return ud->md;
}

LuaMetadataConsumer::LuaMetadataConsumer(lua_State* L, int funcid) : L(L), funcid(funcid) {}
LuaMetadataConsumer::~LuaMetadataConsumer()
{
	// Unindex the function
	luaL_unref(L, LUA_REGISTRYINDEX, funcid);
}

bool LuaMetadataConsumer::operator()(Metadata& md)
{
	// Get the function
	lua_rawgeti(L, LUA_REGISTRYINDEX, funcid);

	// Push the metadata
	md.lua_push(L);

	// Call the function
	if (lua_pcall(L, 1, 1, 0))
	{
		string error = lua_tostring(L, -1);
		lua_pop(L, 1);
		throw wibble::exception::Consistency("running metadata consumer function", error);
	}

	int res = lua_toboolean(L, -1);
	lua_pop(L, 1);
	return res;
}

auto_ptr<LuaMetadataConsumer> LuaMetadataConsumer::lua_check(lua_State* L, int idx)
{
	luaL_checktype(L, idx, LUA_TFUNCTION);

	// Ref the created function into the registry
	lua_pushvalue(L, idx);
	int funcid = luaL_ref(L, LUA_REGISTRYINDEX);

	// Create a consumer using the function
	return auto_ptr<LuaMetadataConsumer>(new LuaMetadataConsumer(L, funcid));
}

#endif


}

// vim:set ts=4 sw=4:
