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
#include <arki/utils/codec.h>
#include <arki/formatter.h>
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

namespace arki {

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

void Metadata::reset()
{
	deleted = false;
	m_filename.clear();
	m_vals.clear();
	notes.clear();
	source.clear();
	m_inline_buf = wibble::sys::Buffer();
}

UItem<> Metadata::get(types::Code code) const
{
	const_iterator i = m_vals.find(code);
	if (i == m_vals.end())
		return UItem<>();
	return i->second;
}

bool Metadata::operator==(const Metadata& m) const
{
	if (m_vals != m.m_vals) return false;

	//if (m_filename != m.m_filename) return false;
	if (notes != m.notes) return false;
	if (source != m.source) return false;
	if (m_inline_buf != m.m_inline_buf) return false;
	return true;
}

int Metadata::compare(const Metadata& m) const
{
	if (int res = utils::compareMaps(m_vals, m.m_vals)) return res;
	
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

void Metadata::set(const Item<>& i)
{
	types::Code code = i->serialisationCode();
	std::map< types::Code, Item<> >::iterator it = m_vals.find(code);
	if (it == m_vals.end())
		m_vals.insert(make_pair(i->serialisationCode(), i));
	else
		it->second = i;
}

void Metadata::unset(types::Code code)
{
	m_vals.erase(code);
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
			case types::TYPE_NOTE: notes.push_back(types::Note::decode(el_start, el_len)); break;
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
			case types::TYPE_NOTE: notes.push_back(types::Note::decodeString(val)); break;
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

	writeYamlList(out, "Note", notes);
}

string Metadata::encode() const
{
	using namespace utils::codec;
	// Encode the various information
	string encoded;
	for (const_iterator i = begin(); i != end(); ++i)
		encoded += i->second->encodeWithEnvelope();
	encoded += encodeItemList(notes.begin(), notes.end());
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

			// Open the input file
			ifstream in;
			in.open(file.c_str(), ios::in);
			if (!in.is_open() || in.fail())
				throw wibble::exception::File(file, "opening file for reading");
			in.seekg(blob->offset);
			if (in.fail())
				throw wibble::exception::File(file, "moving to position " + str::fmt(blob->offset));

			// Read the data
			m_inline_buf.resize(blob->size);
			in.read((char*)m_inline_buf.data(), blob->size);
			if (in.fail())
				throw wibble::exception::File(file, "reading " + str::fmt(blob->size) + " bytes from the file");

			return m_inline_buf;
		}
		case types::Source::INLINE:
			return m_inline_buf;
		default:
			throw wibble::exception::Consistency("retrieving data", "data source type is " + types::Source::formatStyle(source->style()) + " but I can only handle BLOB or INLINE");
	}
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

	// Get length from next 4 bytes
	unsigned int len = decodeUInt((const unsigned char*)buffer.data()+4, 4);

	// Check if we have a full metadata, in that case read it, remove it
	// from the beginning of the string, then consume it it
	if (buffer.size() < 8 + len)
		return;

	size_t pos;
	{
		// Do this in a subscope, so we destroy 'in' and release a
		// reference to the string before we modify the string
		// (hopefully, this will spare us a copy-on-write)
		stringstream in(buffer, ios_base::in);
		if (!md.read(in, streamname, false))
			throw wibble::exception::Consistency("decoding downloaded metadata", "Metadata claims it has enough data to read, then claims it hit end of file");
		pos = in.tellg();
	}
	buffer = buffer.substr(pos);
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

vector<Metadata> Metadata::readFile(const std::string& fname)
{
	// Read all the metadata
    std::ifstream in;
	in.open(fname.c_str(), ios::in);
	if (!in.is_open() || in.fail())
		throw wibble::exception::File(fname, "opening file for reading");

	// Collect all metadata in a vector
	vector<Metadata> res;
	Metadata md;
	while (md.read(in, fname))
		res.push_back(md);

	in.close();

	return res;
}

void Metadata::readFile(const std::string& fname, MetadataConsumer& mdc)
{
	// Read all the metadata
    std::ifstream in;
	in.open(fname.c_str(), ios::in);
	if (!in.is_open() || in.fail())
		throw wibble::exception::File(fname, "opening file for reading");

	// Collect all metadata in a vector
	Metadata md;
	while (md.read(in, fname))
		mdc(md);

	in.close();
}

#ifdef HAVE_LUA
namespace metadata {
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
}

int Metadata::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the Metadata reference from the userdata value
	luaL_checkudata(L, udataidx, "arki.metadata");
	void* userdata = lua_touserdata(L, udataidx);
	const Metadata& v = **(const Metadata**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "source")
	{
		// Return the source element
		if (v.source.defined())
			v.source->lua_push(L);
		else
			lua_pushnil(L);
		return 1;
	}
	else if (name == "notes")
	{
		// Return a table with all the notes in the metadata
		lua_createtable(L, v.notes.size(), 0);
		// Set the array elements
		for (size_t i = 0; i < v.notes.size(); ++i)
		{
			v.notes[i]->lua_push(L);
			lua_rawseti(L, -2, i+1);
		}
		return 1;
	}
	else if (name == "deleted")
	{
		// Return true if the metadata is marked deleted, else false
		lua_pushboolean(L, v.deleted);
		return 1;
	}
	else if (name == "iter")
	{
		// Iterate

		/* create a userdatum to store an iterator structure address */
		metadata::LuaIter**d = (metadata::LuaIter**)lua_newuserdata(L, sizeof(metadata::LuaIter*));

		// Get the metatable for the iterator
		if (luaL_newmetatable(L, "arki.metadata_iter"));
		{
			/* set its __gc field */
			lua_pushstring(L, "__gc");
			lua_pushcfunction(L, metadata::LuaIter::gc);
			lua_settable(L, -3);
		}

		// Set its metatable
		lua_setmetatable(L, -2);

		// Instantiate the iterator
		*d = new metadata::LuaIter(v);

		// Creates and returns the iterator function (its sole upvalue, the
		// iterator userdatum, is already on the stack top
		lua_pushcclosure(L, metadata::LuaIter::iterate, 1);
		return 1;
	}
	else
	{
		// Get an arbitrary element by name
		types::Code code = types::parseCodeName(name);
		UItem<> item = v.get(code);
		if (item.defined())
			item->lua_push(L);
		else
			lua_pushnil(L);
		return 1;
	}
}
static int arkilua_lookup_metadata(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, Metadata::lua_lookup, 2);
	return 1;
}
void Metadata::lua_push(lua_State* L) const
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	const Metadata** s = (const Metadata**)lua_newuserdata(L, sizeof(const Metadata*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki.metadata"));
	{
		// If the metatable wasn't previously created, create it now
		// Set the __index metamethod to the lookup function
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup_metadata);
		lua_settable(L, -3);
	}

	lua_setmetatable(L, -2);
}
Metadata* Metadata::lua_check(lua_State* L, int idx)
{
	void* ud = luaL_checkudata(L, idx, "arki.metadata");
	return *(Metadata**)ud;
}
#endif


}

// vim:set ts=4 sw=4:
