#include "metadata.h"
#include "metadata/consumer.h"
#include "types/value.h"
#include "types/source/blob.h"
#include "types/source/inline.h"
#include "formatter.h"
#include "utils/codec.h"
#include "utils/compress.h"
#include "utils/fd.h"
#include "emitter.h"
#include "emitter/memory.h"
#include "iotrace.h"
#include "scan/any.h"
#include "utils/datareader.h"
#include "utils/string.h"
#include <unistd.h>
#include <fstream>
#include <cstdlib>
#include <cerrno>
#include <stdexcept>

#ifdef HAVE_LUA
#include "utils/lua.h"
#endif

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {

namespace metadata {

// TODO: @WARNING this is NOT thread safe
arki::utils::DataReader dataReader;


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
    if (len < req)
    {
        stringstream s;
        s << "cannot parse " << what << ": size is " << len << " but we need at least " << req << " for the " << what << " style";
        throw runtime_error(s.str());
    }
}

template<typename ITER>
static std::string encodeItemList(const ITER& begin, const ITER& end)
{
	string res;
	for (ITER i = begin; i != end; ++i)
		res += i->encode();
	return res;
}

Metadata::Metadata()
    : m_source(0)
{
}
Metadata::Metadata(const Metadata& o)
    : ItemSet(o), m_notes(o.m_notes), m_source(o.m_source ? o.m_source->clone() : 0), m_data(o.m_data)
{
}
Metadata::~Metadata()
{
    delete m_source;
}
Metadata& Metadata::operator=(const Metadata& o)
{
    if (this == &o) return *this;
    ItemSet::operator=(o);
    delete m_source;
    m_source = o.m_source ? o.m_source->clone() : 0;
    m_notes = o.m_notes;
    m_data = o.m_data;
    return *this;
}

Metadata* Metadata::clone() const
{
    return new Metadata(*this);
}

unique_ptr<Metadata> Metadata::create_empty()
{
    return unique_ptr<Metadata>(new Metadata);
}

unique_ptr<Metadata> Metadata::create_copy(const Metadata& md)
{
    return unique_ptr<Metadata>(md.clone());
}

unique_ptr<Metadata> Metadata::create_from_yaml(std::istream& in, const std::string& filename)
{
    unique_ptr<Metadata> res(create_empty());
    res->readYaml(in, filename);
    return res;
}

const Source& Metadata::source() const
{
    if (!m_source)
        throw wibble::exception::Consistency("metadata has no source");
    return *m_source;
}

const types::source::Blob* Metadata::has_source_blob() const
{
    if (!m_source) return 0;
    return dynamic_cast<source::Blob*>(m_source);
}

const source::Blob& Metadata::sourceBlob() const
{
    if (!m_source) throw wibble::exception::Consistency("metadata has no source");
    const source::Blob* res = dynamic_cast<source::Blob*>(m_source);
    if (!res) throw wibble::exception::Consistency("metadata source is not a Blob source");
    return *res;
}

void Metadata::set_source(std::unique_ptr<types::Source>&& s)
{
    delete m_source;
    m_source = s.release();
}

void Metadata::set_source_inline(const std::string& format, std::vector<uint8_t>&& buf)
{
    m_data = move(buf);
    set_source(Source::createInline(format, m_data.size()));
}

void Metadata::unset_source()
{
    delete m_source;
    m_source = 0;
}

std::vector<types::Note> Metadata::notes() const
{
	std::vector<types::Note> res;
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
		res.push_back(*types::Note::decode(el_start, el_len));
		cur = el_start + el_len;
	}
	return res;
}

const std::string& Metadata::notes_encoded() const
{
	return m_notes;
}

void Metadata::set_notes(const std::vector<types::Note>& notes)
{
	m_notes.clear();
	for (std::vector<types::Note>::const_iterator i = notes.begin(); i != notes.end(); ++i)
		add_note(*i);
}

void Metadata::set_notes_encoded(const std::string& notes)
{
	m_notes = notes;
}

void Metadata::add_note(const types::Note& note)
{
    m_notes += note.encodeBinary();
}

void Metadata::add_note(const std::string& note)
{
    m_notes += Note(note).encodeBinary();
}

void Metadata::clear()
{
    ItemSet::clear();
    m_vals.clear();
    m_notes.clear();
    delete m_source;
    m_source = 0;
}

bool Metadata::operator==(const Metadata& m) const
{
    if (!ItemSet::operator==(m)) return false;
    if (m_notes != m.m_notes) return false;
    return Type::nullable_equals(m_source, m.m_source);
}

int Metadata::compare(const Metadata& m) const
{
    if (int res = ItemSet::compare(m)) return res;
    return Type::nullable_compare(m_source, m.m_source);
}

bool Metadata::read(istream& in, const std::string& filename, bool readInline)
{
    vector<uint8_t> buf;
    string signature;
    unsigned version;
    if (!types::readBundle(in, filename, buf, signature, version))
        return false;

	// Ensure first 2 bytes are MD or !D
	if (signature != "MD")
		throw wibble::exception::Consistency("parsing file " + filename, "metadata entry does not start with 'MD'");

	read(buf, version, filename);

    // If the source is inline, then the data follows the metadata
    if (readInline && source().style() == types::Source::INLINE)
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

void Metadata::read(const std::vector<uint8_t>& buf, unsigned version, const metadata::ReadContext& rc)
{
    read(buf.data(), buf.size(), version, rc);
}

void Metadata::read(const unsigned char* buf, size_t len, unsigned version, const metadata::ReadContext& rc)
{
    clear();

    // Check version and ensure we can decode
    if (version != 0)
    {
        stringstream s;
        s << "cannot parse file " << rc.pathname << ": version of the file is " << version << " but I can only decode version 0";
        throw runtime_error(s.str());
    }

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
            case types::TYPE_SOURCE: set_source(types::Source::decodeRelative(el_start, el_len, rc.basedir)); break;
            default:
                m_vals.insert(make_pair(el_type, types::decodeInner(el_type, el_start, el_len).release()));
                break;
        }

		cur = el_start + el_len;
	}
}

void Metadata::readInlineData(std::istream& in, const std::string& filename)
{
    // If the source is inline, then the data follows the metadata
    if (source().style() == types::Source::INLINE)
    {
        source::Inline* s = dynamic_cast<source::Inline*>(m_source);
        vector<uint8_t> buf;
        buf.resize(s->size);

        iotrace::trace_file(filename, 0, s->size, "read inline data");

        // Read the inline data
        in.read((char*)buf.data(), s->size);
        if (in.fail())
        {
            stringstream ss;
            ss << "cannot read " << s->size << " bytes from " << filename;
            throw std::system_error(errno, std::system_category(), ss.str());
        }

        m_data = move(buf);
    }
}

bool Metadata::readYaml(std::istream& in, const std::string& filename)
{
    clear();

    wibble::str::YamlStream yamlStream;
    for (wibble::str::YamlStream::const_iterator i = yamlStream.begin(in);
            i != yamlStream.end(); ++i)
    {
        types::Code type = types::parseCodeName(i->first);
        string val = str::strip(i->second);
        switch (type)
        {
            case types::TYPE_NOTE: add_note(*types::Note::decodeString(val)); break;
            case types::TYPE_SOURCE: set_source(types::Source::decodeString(val)); break;
            default:
                m_vals.insert(make_pair(type, types::decodeString(type, val).release()));
        }
    }
    return !in.eof();
}

void Metadata::write(std::ostream& out, const std::string& filename) const
{
    // Prepare the encoded data
    string encoded = encodeBinary();

    // Write out
    out.write(encoded.data(), encoded.size());
    if (out.fail())
    {
        stringstream ss;
        ss << "cannot write " << encoded.size() << " bytes to file " << filename;
        throw std::system_error(errno, std::system_category(), ss.str());
    }

    // If the source is inline, then the data follows the metadata
    if (const source::Inline* s = dynamic_cast<const source::Inline*>(m_source))
    {
        if (s->size != m_data.size())
        {
            stringstream ss;
            ss << "cannot write metadata to file " << filename << ": source size " << s->size << " does not match the data size " << m_data.size();
            throw runtime_error(ss.str());
        }
        out.write((const char*)m_data.data(), m_data.size());
        if (out.fail())
        {
            stringstream ss;
            ss << "cannot write " << m_data.size() << " bytes to file " << filename;
            throw std::system_error(errno, std::system_category(), ss.str());
        }
    }
}

void Metadata::write(int outfd, const std::string& filename) const
{
    // Prepare the encoded data
    string encoded = encodeBinary();

    // Write out
    utils::fd::write_all(outfd, encoded);

    // If the source is inline, then the data follows the metadata
    if (const source::Inline* s = dynamic_cast<const source::Inline*>(m_source))
    {
        if (s->size != m_data.size())
        {
            stringstream ss;
            ss << "cannot write metadata to file " << filename << ": metadata size " << s->size << " does not match the data size " << m_data.size();
            throw runtime_error(ss.str());
        }
        utils::fd::write_all(outfd, m_data.data(), m_data.size());
    }
}

void Metadata::writeYaml(std::ostream& out, const Formatter* formatter) const
{
    if (m_source) out << "Source: " << *m_source << endl;
    for (map<types::Code, types::Type*>::const_iterator i = m_vals.begin(); i != m_vals.end(); ++i)
    {
        string uc = str::lower(i->second->tag());
        uc[0] = toupper(uc[0]);
        out << uc << ": ";
        i->second->writeToOstream(out);
        if (formatter)
            out << "\t# " << (*formatter)(*i->second);
        out << endl;
    }

    vector<Note> l(notes());
    if (l.empty()) return;
    out << "Note: ";
    if (l.size() == 1)
        out << *l.begin() << endl;
    else
    {
        out << endl;
        for (vector<Note>::const_iterator i = l.begin(); i != l.end(); ++i)
            out << " " << *i << endl;
    }
}

void Metadata::serialise(Emitter& e, const Formatter* f) const
{
    e.start_mapping();
    e.add("i");
    e.start_list();
    if (m_source) m_source->serialise(e, f);
    for (map<types::Code, types::Type*>::const_iterator i = m_vals.begin(); i != m_vals.end(); ++i)
        i->second->serialise(e, f);
    e.end_list();
    e.add("n");
    e.start_list();
    std::vector<types::Note> n = notes();
    for (std::vector<types::Note>::const_iterator i = n.begin(); i != n.end(); ++i)
        i->serialise(e, f);
    e.end_list();
    e.end_mapping();

    e.add_break();

    // If the source is inline, then the data follows the metadata
    if (const source::Inline* s = dynamic_cast<const source::Inline*>(m_source))
    {
        if (s->size != m_data.size())
        {
            stringstream ss;
            ss << "cannot write metadata to JSON: metadata source size " << s->size << " does not match the data size " << m_data.size();
            throw runtime_error(ss.str());
        }
        e.add_raw(m_data);
    }
}

void Metadata::read(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    // Parse items
    const List& items = val["i"].want_list("parsing metadata item list");
    for (std::vector<const Node*>::const_iterator i = items.val.begin(); i != items.val.end(); ++i)
    {
        unique_ptr<types::Type> item = types::decodeMapping((*i)->want_mapping("parsing metadata item"));
        if (item->type_code() == types::TYPE_SOURCE)
            set_source(move(downcast<types::Source>(move(item))));
        else
            set(move(item));
    }

    // Parse notes
    const List& notes = val["n"].want_list("parsing metadata notes list");
    for (std::vector<const Node*>::const_iterator i = notes.val.begin(); i != notes.val.end(); ++i)
    {
        unique_ptr<types::Type> item = types::decodeMapping((*i)->want_mapping("parsing metadata item"));
        if (item->type_code() == types::TYPE_NOTE)
            add_note(*downcast<types::Note>(move(item)));
    }
}

string Metadata::encodeBinary() const
{
    using namespace utils::codec;
    // Encode the various information
    string encoded;
    for (map<types::Code, types::Type*>::const_iterator i = m_vals.begin(); i != m_vals.end(); ++i)
        encoded += i->second->encodeBinary();
    encoded += m_notes;
    if (m_source) encoded += m_source->encodeBinary();

	string res;
	Encoder enc(res);
	// Prepend header
	enc.addString("MD");
	enc.addUInt(0, 2);
	enc.addUInt(encoded.size(), 4);
	enc.addString(encoded);
	return res;
}


const vector<uint8_t>& Metadata::getData()
{
    // First thing, try and return it from cache
    if (!m_data.empty()) return m_data;

    // If we don't have it in cache, try reconstructing it from the Value metadata
    if (const Value* value = get<types::Value>())
        m_data = arki::scan::reconstruct(m_source->format, *this, value->buffer);
    if (!m_data.empty()) return m_data;

    // If we don't have it in cache and we don't have a source, we cannot know
    // how to load it: give up
    if (!m_source) throw wibble::exception::Consistency("retrieving data", "data source is not defined");

    // Load it according to source
    switch (m_source->style())
    {
        case Source::INLINE:
        case Source::URL:
            throw wibble::exception::Consistency("retrieving data", "data is not accessible");
        case Source::BLOB:
        {
            // Do not directly use m_data so that if dataReader.read throws an
            // exception, m_data remains empty.
            const source::Blob& s = sourceBlob();
            vector<uint8_t> buf;
            buf.resize(s.size);
            metadata::dataReader.read(s.absolutePathname(), s.offset, s.size, buf.data());
            m_data = move(buf);
            return m_data;
        }
        default:
            throw wibble::exception::Consistency("retrieving data", "unsupported source style");
    }
}

void Metadata::drop_cached_data()
{
    if (const source::Blob* blob = dynamic_cast<const source::Blob*>(m_source))
    {
        m_data.clear();
        m_data.shrink_to_fit();
    }
}

void Metadata::set_cached_data(std::vector<uint8_t>&& buf)
{
    m_data = move(buf);
}

void Metadata::makeInline()
{
    if (!m_source) throw wibble::exception::Consistency("making source inline", "data source is not defined");
    getData();
    set_source(Source::createInline(m_source->format, m_data.size()));
}

void Metadata::make_absolute()
{
    if (has_source())
        if (const source::Blob* blob = dynamic_cast<const source::Blob*>(m_source))
            set_source(blob->makeAbsolute());
}

size_t Metadata::data_size() const
{
    if (!m_data.empty()) return m_data.size();
    if (!m_source) return 0;

    // Query according to source
    switch (m_source->style())
    {
        case Source::INLINE:
            return dynamic_cast<const source::Inline*>(m_source)->size;
        case Source::URL:
            // URL does not know about sizes
            return 0;
        case Source::BLOB:
            return dynamic_cast<const source::Blob*>(m_source)->size;
        default:
            // An unsupported source type should make more noise than a 0
            // return type
            throw wibble::exception::Consistency("retrieving data", "unsupported source style");
    }
}

void Metadata::flushDataReaders()
{
    metadata::dataReader.flush();
}


void Metadata::readFile(const std::string& fname, metadata::Eater& mdc)
{
    metadata::ReadContext context(fname);
    readFile(context, mdc);
}

void Metadata::read_file(const std::string& fname, metadata_dest_func dest)
{
    metadata::ReadContext context(fname);
    read_file(context, dest);
}

void Metadata::readFile(const metadata::ReadContext& file, metadata::Eater& mdc)
{
    // Read all the metadata
    std::ifstream in;
    in.open(file.pathname.c_str(), ios::in);
    if (!in.is_open() || in.fail())
        throw wibble::exception::File(file.pathname, "opening file for reading");

    readFile(in, file, mdc);

    in.close();
}

void Metadata::read_file(const metadata::ReadContext& file, metadata_dest_func dest)
{
    // Read all the metadata
    std::ifstream in;
    in.open(file.pathname.c_str(), ios::in);
    if (!in.is_open() || in.fail())
        throw wibble::exception::File(file.pathname, "opening file for reading");

    read_file(in, file, dest);

    in.close();
}

void Metadata::readFile(std::istream& in, const metadata::ReadContext& file, metadata::Eater& mdc)
{
    read_file(in, file, [&](unique_ptr<Metadata> md) { return mdc.eat(move(md)); });
}

void Metadata::read_file(std::istream& in, const metadata::ReadContext& file, metadata_dest_func dest)
{
    bool canceled = false;
    vector<uint8_t> buf;
    string signature;
    unsigned version;
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
            Metadata::read_group(buf, version, file, dest);
        } else {
            unique_ptr<Metadata> md(new Metadata);
            iotrace::trace_file(file.pathname, 0, 0, "read metadata");
            md->read(buf, version, file);

            // If the source is inline, then the data follows the metadata
            if (md->source().style() == types::Source::INLINE)
                md->readInlineData(in, file.pathname);
            canceled = !dest(move(md));
        }
    }
}

void Metadata::read_group(const std::vector<uint8_t>& buf, unsigned version, const metadata::ReadContext& file, metadata_dest_func dest)
{
    // Handle metadata group
    if (version != 0)
    {
        stringstream ss;
        ss << "cannot parse file " << file.pathname << ": found version " << version << " but only version 0 (LZO compressed) is supported";
        throw runtime_error(ss.str());
    }

    // Read uncompressed size
    ensureSize(buf.size(), 4, "uncompressed item size");
    uint32_t uncsize = codec::decodeUInt((const unsigned char*)buf.data(), 4);

    vector<uint8_t> decomp = utils::compress::unlzo((const unsigned char*)buf.data() + 4, buf.size() - 4, uncsize);
    const unsigned char* ubuf = (const unsigned char*)decomp.data();
    size_t len = decomp.size();
    const unsigned char* ibuf;
    size_t ilen;
    string isig;
    unsigned iver;
    bool canceled = false;
    while (!canceled && types::readBundle(ubuf, len, file.pathname, ibuf, ilen, isig, iver))
    {
        unique_ptr<Metadata> md(new Metadata);
        md->read(ibuf, ilen, iver, file);
        canceled = !dest(move(md));
    }
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
            lua_pushstring(L, str::lower(types::formatCode(i.iter->first)).c_str());
            i.iter->second->lua_push(L);
            ++i.iter;
            return 2;
        }
        else
            return 0;  // no more values to return
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
    std::vector<Note> notes = md->notes();
    lua_createtable(L, notes.size(), 0);
    // Set the array elements
    for (size_t i = 0; i < notes.size(); ++i)
    {
        notes[i].lua_push(L);
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
        if (md->has_source())
            md->source().lua_push(L);
        else
            lua_pushnil(L);
        return 1;
    } else {
        // Return the metadata item
        const Type* item = md->get(code);
        if (item)
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
    Type* item = types::Type::lua_check(L, 3);
    luaL_argcheck(L, item, 3, "arki.type.* expected");

    if (key == "source")
    {
        luaL_argcheck(L, item->type_code() == types::TYPE_SOURCE, 3, "arki.type.source expected");
        md->set_source(downcast<Source>(item->cloneType()));
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
        if (item->type_code() != code)
        {
            string msg = "arki.type." + types::tag(code) + " expected";
            luaL_argcheck(L, false, 3, msg.c_str());
        }
        md->set(*item);
    }
    return 0;
}

static int arkilua_set(lua_State* L)
{
    Metadata* md = Metadata::lua_check(L, 1);
    luaL_argcheck(L, md != NULL, 1, "`arki.metadata' expected");
    Type* item = types::Type::lua_check(L, 2);
    luaL_argcheck(L, item, 2, "arki.type.* expected");
    md->set(*item);
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
    std::vector<types::Note> n = md->notes();
    for (size_t i = 0; i < n.size(); ++i)
    {
        n[i].lua_push(L);
        lua_rawseti(L, -2, i+1);
    }
    lua_rawset(L, -3);

    // Add source
    if (md->has_source())
    {
        lua_pushstring(L, "source");
        md->source().lua_push(L);
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
    {
        stringstream s;
        s << str::lower(formatCode(i->first)) << ": " << i->second;
        bits.push_back(s.str());
    }
    string merged = "{" + str::join(", ", bits.begin(), bits.end()) + "}";
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
	// The 'metadata' object is a userdata that holds a pointer to this Metadata structure
	MetadataUD::create(L, this, false);

	// Set the metatable for the userdata
	arkilua_metadatametatable(L);

	lua_setmetatable(L, -2);
}

void Metadata::lua_push(lua_State* L, unique_ptr<Metadata>&& md)
{
    // The 'metadata' object is a userdata that holds a pointer to this Metadata structure
    MetadataUD::create(L, md.release(), true);

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
