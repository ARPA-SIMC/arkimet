#include "metadata.h"
#include "metadata/data.h"
#include "metadata/consumer.h"
#include "core/file.h"
#include "exceptions.h"
#include "types/value.h"
#include "types/source/blob.h"
#include "types/source/inline.h"
#include "formatter.h"
#include "binary.h"
#include "utils/compress.h"
#include "emitter.h"
#include "emitter/memory.h"
#include "iotrace.h"
#include "scan.h"
#include "utils/string.h"
#include "utils/yaml.h"
#include <unistd.h>
#include <cstdlib>
#include <cerrno>
#include <stdexcept>
#include <fcntl.h>
#include <sys/uio.h>

#ifdef HAVE_LUA
#include "utils/lua.h"
#endif

using namespace std;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::core;

namespace arki {

namespace metadata {

ReadContext::ReadContext() {}

ReadContext::ReadContext(const std::string& pathname)
    : basedir(str::dirname(sys::abspath(pathname))), pathname(pathname)
{
}

ReadContext::ReadContext(const std::string& pathname, const std::string& basedir)
    : basedir(sys::abspath(basedir)), pathname(pathname)
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
{
}

Metadata::Metadata(const Metadata& o)
    : ItemSet(o), m_notes(o.m_notes), m_source(o.m_source ? o.m_source->clone() : nullptr), m_data(o.m_data)
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

#if 0
unique_ptr<Metadata> Metadata::create_from_yaml(std::istream& in, const std::string& filename)
{
    unique_ptr<Metadata> res(create_empty());
    res->readYaml(in, filename);
    return res;
}
#endif

const Source& Metadata::source() const
{
    if (!m_source)
        throw_consistency_error("metadata has no source");
    return *m_source;
}

const types::source::Blob* Metadata::has_source_blob() const
{
    if (!m_source) return 0;
    return dynamic_cast<source::Blob*>(m_source);
}

const source::Blob& Metadata::sourceBlob() const
{
    if (!m_source) throw_consistency_error("metadata has no source");
    const source::Blob* res = dynamic_cast<const source::Blob*>(m_source);
    if (!res) throw_consistency_error("metadata source is not a Blob source");
    return *res;
}

source::Blob& Metadata::sourceBlob()
{
    if (!m_source) throw_consistency_error("metadata has no source");
    source::Blob* res = dynamic_cast<source::Blob*>(m_source);
    if (!res) throw_consistency_error("metadata source is not a Blob source");
    return *res;
}

void Metadata::set_source(std::unique_ptr<types::Source>&& s)
{
    delete m_source;
    m_source = s.release();
}

void Metadata::set_source_inline(const std::string& format, std::shared_ptr<metadata::Data> data)
{
    m_data = data;
    set_source(Source::createInline(format, m_data->size()));
}

void Metadata::unset_source()
{
    delete m_source;
    m_source = 0;
}

std::vector<types::Note> Metadata::notes() const
{
    std::vector<types::Note> res;
    BinaryDecoder dec(m_notes);
    while (dec)
    {
        types::Code el_type;
        BinaryDecoder inner = dec.pop_type_envelope(el_type);

        if (el_type != TYPE_NOTE)
            throw std::runtime_error("cannot decode note: item type is not a note");

        res.push_back(*types::Note::decode(inner));
    }
    return res;
}

const std::vector<uint8_t>& Metadata::notes_encoded() const
{
    return m_notes;
}

void Metadata::set_notes(const std::vector<types::Note>& notes)
{
    m_notes.clear();
    for (std::vector<types::Note>::const_iterator i = notes.begin(); i != notes.end(); ++i)
        add_note(*i);
}

void Metadata::set_notes_encoded(const std::vector<uint8_t>& notes)
{
    m_notes = notes;
}

void Metadata::add_note(const types::Note& note)
{
    BinaryEncoder enc(m_notes);
    note.encodeBinary(enc);
}

void Metadata::add_note(const std::string& note)
{
    BinaryEncoder enc(m_notes);
    Note(note).encodeBinary(enc);
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

int Metadata::compare_items(const Metadata& m) const
{
    // Compare skipping VALUE items
    const_iterator a = begin();
    const_iterator b = m.begin();
    if (a != end() && a->first == TYPE_VALUE) ++a;
    if (b != m.end() && b->first == TYPE_VALUE) ++b;
    auto incr_a = [&] {
        ++a;
        if (a != end() && a->first == TYPE_VALUE) ++a;
    };
    auto incr_b = [&] {
        ++b;
        if (b != m.end() && b->first == TYPE_VALUE) ++b;
    };

    for ( ; a != end() && b != m.end(); incr_a(), incr_b())
    {
        if (a->first == TYPE_VALUE) ++a;
        if (b->first == TYPE_VALUE) ++b;
        if (a->first < b->first)
            return -1;
        if (a->first > b->first)
            return 1;
        if (int res = a->second->compare(*b->second)) return res;
    }
    if (a != end() && a->first == TYPE_VALUE) ++a;
    if (b != m.end() && b->first == TYPE_VALUE) ++b;

    if (a == end() && b == m.end())
        return 0;
    if (a == end())
        return -1;
    return 1;
}

bool Metadata::read(int in, const metadata::ReadContext& filename, bool readInline)
{
    types::Bundle bundle;
    NamedFileDescriptor f(in, filename.pathname);
    if (!bundle.read_header(f))
        return false;

    // Ensure first 2 bytes are MD or !D
    if (bundle.signature != "MD")
        throw_consistency_error("parsing file " + filename.pathname, "metadata entry does not start with 'MD'");

    if (!bundle.read_data(f))
        return false;

    BinaryDecoder inner(bundle.data);

    read_inner(inner, bundle.version, filename);

    // If the source is inline, then the data follows the metadata
    if (readInline && source().style() == types::Source::INLINE)
        read_inline_data(f);

    return true;
}

bool Metadata::read(BinaryDecoder& dec, const metadata::ReadContext& filename, bool readInline)
{
    if (!dec) return false;

    string signature;
    unsigned version;
    BinaryDecoder inner = dec.pop_metadata_bundle(signature, version);

    // Ensure first 2 bytes are MD or !D
    if (signature != "MD")
        throw std::runtime_error("cannot parse " + filename.pathname + ": metadata entry does not start with 'MD'");

    read_inner(inner, version, filename);

    // If the source is inline, then the data follows the metadata
    if (readInline && source().style() == types::Source::INLINE)
        readInlineData(dec, filename.pathname);

    return true;
}

void Metadata::read_inner(BinaryDecoder& dec, unsigned version, const metadata::ReadContext& rc)
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
    while (dec)
    {
        const uint8_t* encoded_start = dec.buf;
        TypeCode el_type;
        BinaryDecoder inner = dec.pop_type_envelope(el_type);
        const uint8_t* encoded_end = dec.buf;

        switch (el_type)
        {
            case TYPE_NOTE:
                m_notes.insert(m_notes.end(), encoded_start, encoded_end);
                break;
            case TYPE_SOURCE:
                set_source(types::Source::decodeRelative(inner, rc.basedir));
                break;
            default:
                m_vals.insert(make_pair(el_type, types::decodeInner(el_type, inner).release()));
                break;
        }
    }
}

void Metadata::read_inline_data(NamedFileDescriptor& fd)
{
    // If the source is inline, then the data follows the metadata
    if (source().style() != types::Source::INLINE) return;

    source::Inline* s = dynamic_cast<source::Inline*>(m_source);
    vector<uint8_t> buf;
    buf.resize(s->size);

    iotrace::trace_file(fd, 0, s->size, "read inline data");

    // Read the inline data
    fd.read_all_or_throw(buf.data(), s->size);
    m_data = metadata::DataManager::get().to_data(m_source->format, move(buf));
}

void Metadata::read_inline_data(core::AbstractInputFile& fd)
{
    // If the source is inline, then the data follows the metadata
    if (source().style() != types::Source::INLINE) return;

    source::Inline* s = dynamic_cast<source::Inline*>(m_source);
    vector<uint8_t> buf;
    buf.resize(s->size);

    iotrace::trace_file(fd, 0, s->size, "read inline data");

    // Read the inline data
    fd.read(buf.data(), s->size);
    m_data = metadata::DataManager::get().to_data(m_source->format, move(buf));
}

void Metadata::readInlineData(BinaryDecoder& dec, const std::string& filename)
{
    // If the source is inline, then the data follows the metadata
    if (source().style() != types::Source::INLINE) return;

    source::Inline* s = dynamic_cast<source::Inline*>(m_source);

    BinaryDecoder data = dec.pop_data(s->size, "inline data");
    m_data = metadata::DataManager::get().to_data(m_source->format, std::vector<uint8_t>(data.buf, data.buf + s->size));
}

bool Metadata::readYaml(LineReader& in, const std::string& filename)
{
    clear();

    YamlStream yamlStream;
    for (YamlStream::const_iterator i = yamlStream.begin(in);
            i != yamlStream.end(); ++i)
    {
        types::Code type = types::parseCodeName(i->first);
        string val = str::strip(i->second);
        switch (type)
        {
            case TYPE_NOTE: add_note(*types::Note::decodeString(val)); break;
            case TYPE_SOURCE: set_source(types::Source::decodeString(val)); break;
            default:
                m_vals.insert(make_pair(type, types::decodeString(type, val).release()));
        }
    }
    return !in.eof();
}

void Metadata::write(NamedFileDescriptor& out) const
{
    // Prepare the encoded data
    vector<uint8_t> encoded = encodeBinary();

    // Write out
    out.write_all_or_retry(encoded.data(), encoded.size());

    // If the source is inline, then the data follows the metadata
    if (const source::Inline* s = dynamic_cast<const source::Inline*>(m_source))
    {
        if (s->size != m_data->size())
        {
            stringstream ss;
            ss << "cannot write metadata to file " << out.name() << ": metadata size " << s->size << " does not match the data size " << m_data->size();
            throw runtime_error(ss.str());
        }
        m_data->write_inline(out);
    }
}

void Metadata::write(AbstractOutputFile& out) const
{
    // Prepare the encoded data
    vector<uint8_t> encoded = encodeBinary();

    // Write out
    out.write(encoded.data(), encoded.size());

    // If the source is inline, then the data follows the metadata
    if (const source::Inline* s = dynamic_cast<const source::Inline*>(m_source))
    {
        if (s->size != m_data->size())
        {
            stringstream ss;
            ss << "cannot write metadata to file " << out.name() << ": metadata size " << s->size << " does not match the data size " << m_data->size();
            throw runtime_error(ss.str());
        }
        m_data->write_inline(out);
    }
}

std::string Metadata::to_yaml(const Formatter* formatter) const
{
    std::stringstream buf;
    if (m_source) buf << "Source: " << *m_source << endl;
    for (const auto& i: m_vals)
    {
        string uc = str::lower(i.second->tag());
        uc[0] = toupper(uc[0]);
        buf << uc << ": ";
        i.second->writeToOstream(buf);
        if (formatter)
            buf << "\t# " << (*formatter)(*i.second);
        buf << endl;
    }

    vector<Note> l(notes());
    if (l.empty())
        return buf.str();
    buf << "Note: ";
    if (l.size() == 1)
        buf << *l.begin() << endl;
    else
    {
        buf << endl;
        for (const auto& note: l)
            buf << " " << note << endl;
    }

    return buf.str();
}

void Metadata::write_yaml(core::NamedFileDescriptor& out, const Formatter* formatter) const
{
    std::string yaml = to_yaml(formatter);
    out.write_all_or_retry(yaml.data(), yaml.size());
}

void Metadata::write_yaml(core::AbstractOutputFile& out, const Formatter* formatter) const
{
    std::string yaml = to_yaml(formatter);
    out.write(yaml.data(), yaml.size());
}

void Metadata::serialise(Emitter& e, const emitter::Keys& keys, const Formatter* f) const
{
    e.start_mapping();
    e.add("i");
    e.start_list();
    if (m_source) m_source->serialise(e, keys, f);
    for (const auto& val: m_vals)
        val.second->serialise(e, keys, f);
    e.end_list();
    e.add("n");
    e.start_list();
    std::vector<types::Note> n = notes();
    for (const auto& note: n)
        note.serialise(e, keys, f);
    e.end_list();
    e.end_mapping();

    e.add_break();

    // If the source is inline, then the data follows the metadata
    if (const source::Inline* s = dynamic_cast<const source::Inline*>(m_source))
    {
        if (s->size != m_data->size())
        {
            stringstream ss;
            ss << "cannot write metadata to JSON: metadata source size " << s->size << " does not match the data size " << m_data->size();
            throw runtime_error(ss.str());
        }
        m_data->emit(e);
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
        if (item->type_code() == TYPE_SOURCE)
            set_source(move(downcast<types::Source>(move(item))));
        else
            set(move(item));
    }

    // Parse notes
    const List& notes = val["n"].want_list("parsing metadata notes list");
    for (std::vector<const Node*>::const_iterator i = notes.val.begin(); i != notes.val.end(); ++i)
    {
        unique_ptr<types::Type> item = types::decodeMapping((*i)->want_mapping("parsing metadata item"));
        if (item->type_code() == TYPE_NOTE)
            add_note(*downcast<types::Note>(move(item)));
    }
}

std::vector<uint8_t> Metadata::encodeBinary() const
{
    vector<uint8_t> res;
    BinaryEncoder enc(res);
    encodeBinary(enc);
    return res;
}

void Metadata::encodeBinary(BinaryEncoder& enc) const
{
    // Encode the various information
    vector<uint8_t> encoded;
    BinaryEncoder subenc(encoded);
    for (map<types::Code, types::Type*>::const_iterator i = m_vals.begin(); i != m_vals.end(); ++i)
        i->second->encodeBinary(subenc);
    subenc.add_raw(m_notes);
    if (m_source)
       m_source->encodeBinary(subenc);

    // Prepend header
    enc.add_string("MD");
    enc.add_unsigned(0u, 2);
    // TODO Make it a one pass only job, by writing zeroes here the first time round, then rewriting it with the actual length
    enc.add_unsigned(encoded.size(), 4);
    enc.add_raw(encoded);
}


const metadata::Data& Metadata::get_data()
{
    // First thing, try and return it from cache
    if (m_data) return *m_data;

    // If we don't have it in cache, try reconstructing it from the Value metadata
    if (const Value* value = get<types::Value>())
        m_data = metadata::DataManager::get().to_data(m_source->format, scan::Scanner::reconstruct(m_source->format, *this, value->buffer));
    if (m_data) return *m_data;

    // If we don't have it in cache and we don't have a source, we cannot know
    // how to load it: give up
    if (!m_source) throw_consistency_error("retrieving data", "data source is not defined");

    // Load it according to source
    switch (m_source->style())
    {
        case Source::INLINE:
            throw runtime_error("cannot retrieve data: data is not found on INLINE metadata");
        case Source::URL:
            throw runtime_error("cannot retrieve data: data is not accessible for URL metadata");
        case Source::BLOB:
        {
            // Do not directly use m_data so that if dataReader.read throws an
            // exception, m_data remains empty.
            source::Blob& s = sourceBlob();
            if (!s.reader)
                throw runtime_error("cannot retrieve data: BLOB source has no reader associated");
            m_data = metadata::DataManager::get().to_data(m_source->format, s.read_data());
            return *m_data;
        }
        default:
            throw_consistency_error("retrieving data", "unsupported source style");
    }
}

size_t Metadata::stream_data(NamedFileDescriptor& out)
{
    // First thing, try and return it from cache
    if (m_data) return m_data->write(out);

    // If we don't have it in cache, try reconstructing it from the Value metadata
    if (const Value* value = get<types::Value>())
        m_data = metadata::DataManager::get().to_data(m_source->format, scan::Scanner::reconstruct(m_source->format, *this, value->buffer));
    if (m_data) return m_data->write(out);

    // If we don't have it in cache and we don't have a source, we cannot know
    // how to load it: give up
    if (!m_source) throw_consistency_error("retrieving data", "data source is not defined");

    // Load it according to source
    switch (m_source->style())
    {
        case Source::INLINE:
            throw runtime_error("cannot retrieve data: data is not found on INLINE metadata");
        case Source::URL:
            throw runtime_error("cannot retrieve data: data is not accessible for URL metadata");
        case Source::BLOB:
        {
            // Do not directly use m_data so that if dataReader.read throws an
            // exception, m_data remains empty.
            source::Blob& s = sourceBlob();
            if (!s.reader)
                throw runtime_error("cannot retrieve data: BLOB source has no reader associated");
            return s.stream_data(out);
        }
        default:
            throw_consistency_error("retrieving data", "unsupported source style");
    }
}

size_t Metadata::stream_data(AbstractOutputFile& out)
{
    // First thing, try and return it from cache
    if (m_data) return m_data->write(out);

    // If we don't have it in cache, try reconstructing it from the Value metadata
    if (const Value* value = get<types::Value>())
        m_data = metadata::DataManager::get().to_data(m_source->format, scan::Scanner::reconstruct(m_source->format, *this, value->buffer));
    if (m_data) return m_data->write(out);

    // If we don't have it in cache and we don't have a source, we cannot know
    // how to load it: give up
    if (!m_source) throw_consistency_error("retrieving data", "data source is not defined");

    // Load it according to source
    switch (m_source->style())
    {
        case Source::INLINE:
            throw runtime_error("cannot retrieve data: data is not found on INLINE metadata");
        case Source::URL:
            throw runtime_error("cannot retrieve data: data is not accessible for URL metadata");
        case Source::BLOB:
        {
            // Do not directly use m_data so that if dataReader.read throws an
            // exception, m_data remains empty.
            source::Blob& s = sourceBlob();
            if (!s.reader)
                throw runtime_error("cannot retrieve data: BLOB source has no reader associated");
            return s.stream_data(out);
        }
        default:
            throw_consistency_error("retrieving data", "unsupported source style");
    }
}

void Metadata::drop_cached_data()
{
    if (/*const source::Blob* blob =*/ dynamic_cast<const source::Blob*>(m_source))
        m_data.reset();
}

bool Metadata::has_cached_data() const
{
    return (bool)m_data;
}

void Metadata::set_cached_data(std::shared_ptr<metadata::Data> data)
{
    m_data = data;
}

void Metadata::makeInline()
{
    if (!m_source) throw_consistency_error("making source inline", "data source is not defined");
    get_data();
    set_source(Source::createInline(m_source->format, m_data->size()));
}

void Metadata::make_absolute()
{
    if (has_source())
        if (const source::Blob* blob = dynamic_cast<const source::Blob*>(m_source))
            set_source(blob->makeAbsolute());
}

size_t Metadata::data_size() const
{
    if (m_data) return m_data->size();
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
            throw_consistency_error("retrieving data", "unsupported source style");
    }
}

bool Metadata::read_buffer(const uint8_t* buf, std::size_t size, const metadata::ReadContext& file, metadata_dest_func dest)
{
    BinaryDecoder dec(buf, size);
    return read_buffer(dec, file, dest);
}

bool Metadata::read_buffer(const std::vector<uint8_t>& buf, const metadata::ReadContext& file, metadata_dest_func dest)
{
    BinaryDecoder dec(buf);
    return read_buffer(dec, file, dest);
}

bool Metadata::read_buffer(BinaryDecoder& dec, const metadata::ReadContext& file, metadata_dest_func dest)
{
    bool canceled = false;
    string signature;
    unsigned version;
    while (dec)
    {
        if (canceled) continue;
        BinaryDecoder inner = dec.pop_metadata_bundle(signature, version);

        // Ensure first 2 bytes are MD or !D
        if (signature != "MD" && signature != "!D" && signature != "MG")
            throw std::runtime_error("cannot parse file " + file.pathname + ": metadata entry does not start with 'MD', '!D' or 'MG'");

        if (signature == "MG")
        {
            // Handle metadata group
            iotrace::trace_file(file.pathname, 0, 0, "read metadata group");
            Metadata::read_group(inner, version, file, dest);
        } else {
            unique_ptr<Metadata> md(new Metadata);
            iotrace::trace_file(file.pathname, 0, 0, "read metadata");
            md->read_inner(inner, version, file);

            // If the source is inline, then the data follows the metadata
            if (md->source().style() == types::Source::INLINE)
                md->readInlineData(dec, file.pathname);
            canceled = !dest(move(md));
        }
    }

    return !canceled;
}

bool Metadata::read_file(const std::string& fname, metadata_dest_func dest)
{
    metadata::ReadContext context(fname);
    return read_file(context, dest);
}

bool Metadata::read_file(const metadata::ReadContext& file, metadata_dest_func dest)
{
    // Read all the metadata
    sys::File in(file.pathname, O_RDONLY);
    bool res = read_file(in, file, dest);
    in.close();
    return res;
}

bool Metadata::read_file(int in, const metadata::ReadContext& file, metadata_dest_func dest)
{
    bool canceled = false;
    NamedFileDescriptor f(in, file.pathname);;
    types::Bundle bundle;
    while (bundle.read_header(f))
    {
        // Ensure first 2 bytes are MD or !D
        if (bundle.signature != "MD" && bundle.signature != "!D" && bundle.signature != "MG")
            throw_consistency_error("parsing file " + file.pathname, "metadata entry does not start with 'MD', '!D' or 'MG'");

        if (!bundle.read_data(f)) break;

        if (canceled) continue;

        if (bundle.signature == "MG")
        {
            // Handle metadata group
            iotrace::trace_file(file.pathname, 0, 0, "read metadata group");
            BinaryDecoder dec(bundle.data);
            Metadata::read_group(dec, bundle.version, file, dest);
        } else {
            unique_ptr<Metadata> md(new Metadata);
            iotrace::trace_file(file.pathname, 0, 0, "read metadata");
            BinaryDecoder dec(bundle.data);
            md->read_inner(dec, bundle.version, file);

            // If the source is inline, then the data follows the metadata
            if (md->source().style() == types::Source::INLINE)
                md->read_inline_data(f);
            canceled = !dest(move(md));
        }
    }
    return !canceled;
}

bool Metadata::read_file(NamedFileDescriptor& fd, metadata_dest_func mdc)
{
    return read_file(fd, fd.name(), mdc);
}

bool Metadata::read_file(core::AbstractInputFile& fd, const metadata::ReadContext& file, metadata_dest_func dest)
{
    bool canceled = false;
    types::Bundle bundle;
    while (bundle.read_header(fd))
    {
        // Ensure first 2 bytes are MD or !D
        if (bundle.signature != "MD" && bundle.signature != "!D" && bundle.signature != "MG")
            throw_consistency_error("parsing file " + file.pathname, "metadata entry does not start with 'MD', '!D' or 'MG'");

        if (!bundle.read_data(fd)) break;

        if (canceled) continue;

        if (bundle.signature == "MG")
        {
            // Handle metadata group
            iotrace::trace_file(file.pathname, 0, 0, "read metadata group");
            BinaryDecoder dec(bundle.data);
            Metadata::read_group(dec, bundle.version, file, dest);
        } else {
            unique_ptr<Metadata> md(new Metadata);
            iotrace::trace_file(file.pathname, 0, 0, "read metadata");
            BinaryDecoder dec(bundle.data);
            md->read_inner(dec, bundle.version, file);

            // If the source is inline, then the data follows the metadata
            if (md->source().style() == types::Source::INLINE)
                md->read_inline_data(fd);
            canceled = !dest(move(md));
        }
    }
    return !canceled;
}

bool Metadata::read_group(BinaryDecoder& dec, unsigned version, const metadata::ReadContext& file, metadata_dest_func dest)
{
    // Handle metadata group
    if (version != 0)
    {
        stringstream ss;
        ss << "cannot parse file " << file.pathname << ": found version " << version << " but only version 0 (LZO compressed) is supported";
        throw runtime_error(ss.str());
    }

    // Read uncompressed size
    uint32_t uncsize = dec.pop_uint(4, "uncompressed item size");

    vector<uint8_t> decomp = utils::compress::unlzo(dec.buf, dec.size, uncsize);
    dec.buf += dec.size;
    dec.size = 0;

    BinaryDecoder unenc(decomp);

    string isig;
    unsigned iver;
    bool canceled = false;
    while (!canceled && unenc)
    {
        BinaryDecoder inner = unenc.pop_metadata_bundle(isig, iver);
        unique_ptr<Metadata> md(new Metadata);
        md->read_inner(inner, iver, file);
        canceled = !dest(move(md));
    }

    return !canceled;
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
    if (code == TYPE_INVALID)
    {
        // Delegate lookup to the metatable
        lua_getmetatable(L, 1);
        lua_pushlstring(L, key.data(), key.size());
        lua_gettable(L, -2);
        // utils::lua::dumpstack(L, "postlookup", cerr);
        return 1;
    } else if (code == TYPE_SOURCE) {
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
        luaL_argcheck(L, item->type_code() == TYPE_SOURCE, 3, "arki.type.source expected");
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
    if (luaL_newmetatable(L, "arki.metadata"))
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
