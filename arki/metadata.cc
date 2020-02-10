#include "metadata.h"
#include "metadata/data.h"
#include "core/file.h"
#include "core/binary.h"
#include "exceptions.h"
#include "types/bundle.h"
#include "types/value.h"
#include "types/source/blob.h"
#include "types/source/inline.h"
#include "formatter.h"
#include "utils/compress.h"
#include "structured/emitter.h"
#include "structured/memory.h"
#include "structured/keys.h"
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
    core::BinaryDecoder dec(m_notes);
    while (dec)
    {
        types::Code el_type;
        core::BinaryDecoder inner = dec.pop_type_envelope(el_type);

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
    core::BinaryEncoder enc(m_notes);
    note.encodeBinary(enc);
}

void Metadata::add_note(const std::string& note)
{
    core::BinaryEncoder enc(m_notes);
    Note(note).encodeBinary(enc);
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

    core::BinaryDecoder inner(bundle.data);

    read_inner(inner, bundle.version, filename);

    // If the source is inline, then the data follows the metadata
    if (readInline && source().style() == types::Source::Style::INLINE)
        read_inline_data(f);

    return true;
}

bool Metadata::read(core::BinaryDecoder& dec, const metadata::ReadContext& filename, bool readInline)
{
    if (!dec) return false;

    string signature;
    unsigned version;
    core::BinaryDecoder inner = dec.pop_metadata_bundle(signature, version);

    // Ensure first 2 bytes are MD or !D
    if (signature != "MD")
        throw std::runtime_error("cannot parse " + filename.pathname + ": metadata entry does not start with 'MD'");

    read_inner(inner, version, filename);

    // If the source is inline, then the data follows the metadata
    if (readInline && source().style() == types::Source::Style::INLINE)
        readInlineData(dec, filename.pathname);

    return true;
}

void Metadata::read_inner(core::BinaryDecoder& dec, unsigned version, const metadata::ReadContext& rc)
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
        core::BinaryDecoder inner = dec.pop_type_envelope(el_type);
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
    if (source().style() != types::Source::Style::INLINE) return;

    source::Inline* s = dynamic_cast<source::Inline*>(m_source);
    vector<uint8_t> buf;
    buf.resize(s->size);

    iotrace::trace_file(fd, 0, s->size, "read inline data");

    // Read the inline data
    if (!fd.read_all_or_retry(buf.data(), s->size))
        fd.throw_runtime_error("inline data not found after arkimet metadata");
    m_data = metadata::DataManager::get().to_data(m_source->format, move(buf));
}

void Metadata::read_inline_data(core::AbstractInputFile& fd)
{
    // If the source is inline, then the data follows the metadata
    if (source().style() != types::Source::Style::INLINE) return;

    source::Inline* s = dynamic_cast<source::Inline*>(m_source);
    vector<uint8_t> buf;
    buf.resize(s->size);

    iotrace::trace_file(fd, 0, s->size, "read inline data");

    // Read the inline data
    fd.read(buf.data(), s->size);
    m_data = metadata::DataManager::get().to_data(m_source->format, move(buf));
}

void Metadata::readInlineData(core::BinaryDecoder& dec, const std::string& filename)
{
    // If the source is inline, then the data follows the metadata
    if (source().style() != types::Source::Style::INLINE) return;

    source::Inline* s = dynamic_cast<source::Inline*>(m_source);

    core::BinaryDecoder data = dec.pop_data(s->size, "inline data");
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
            buf << "\t# " << formatter->format(*i.second);
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

void Metadata::serialise(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.start_mapping();
    e.add(keys.metadata_items);
    e.start_list();
    if (m_source) m_source->serialise(e, keys, f);
    for (const auto& val: m_vals)
        val.second->serialise(e, keys, f);
    e.end_list();
    e.add(keys.metadata_notes);
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

void Metadata::read(const structured::Keys& keys, const structured::Reader& val)
{
    // Parse items
    val.sub(keys.metadata_items, "metadata items", [&](const structured::Reader& items) {
        unsigned size = items.list_size("metadata items");
        for (unsigned idx = 0; idx < size; ++idx)
        {
            unique_ptr<types::Type> item = items.as_type(idx, "metadata item", keys);
            if (item->type_code() == TYPE_SOURCE)
                set_source(move(downcast<types::Source>(move(item))));
            else
                set(move(item));
        }
    });

    // Parse notes
    val.sub(keys.metadata_notes, "metadata notes", [&](const structured::Reader& notes) {
        unsigned size = notes.list_size("metadata notes");
        for (unsigned idx = 0; idx < size; ++idx)
        {
            unique_ptr<types::Type> item = notes.as_type(idx, "metadata note", keys);
            if (item->type_code() == TYPE_NOTE)
                add_note(*downcast<types::Note>(move(item)));
        }
    });
}

std::vector<uint8_t> Metadata::encodeBinary() const
{
    vector<uint8_t> res;
    core::BinaryEncoder enc(res);
    encodeBinary(enc);
    return res;
}

void Metadata::encodeBinary(core::BinaryEncoder& enc) const
{
    // Encode the various information
    vector<uint8_t> encoded;
    core::BinaryEncoder subenc(encoded);
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
        case Source::Style::INLINE:
            throw runtime_error("cannot retrieve data: data is not found on INLINE metadata");
        case Source::Style::URL:
            throw runtime_error("cannot retrieve data: data is not accessible for URL metadata");
        case Source::Style::BLOB:
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
        case Source::Style::INLINE:
            throw runtime_error("cannot retrieve data: data is not found on INLINE metadata");
        case Source::Style::URL:
            throw runtime_error("cannot retrieve data: data is not accessible for URL metadata");
        case Source::Style::BLOB:
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
        case Source::Style::INLINE:
            throw runtime_error("cannot retrieve data: data is not found on INLINE metadata");
        case Source::Style::URL:
            throw runtime_error("cannot retrieve data: data is not accessible for URL metadata");
        case Source::Style::BLOB:
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
        case Source::Style::INLINE:
            return dynamic_cast<const source::Inline*>(m_source)->size;
        case Source::Style::URL:
            // URL does not know about sizes
            return 0;
        case Source::Style::BLOB:
            return dynamic_cast<const source::Blob*>(m_source)->size;
        default:
            // An unsupported source type should make more noise than a 0
            // return type
            throw_consistency_error("retrieving data", "unsupported source style");
    }
}

bool Metadata::read_buffer(const uint8_t* buf, std::size_t size, const metadata::ReadContext& file, metadata_dest_func dest)
{
    core::BinaryDecoder dec(buf, size);
    return read_buffer(dec, file, dest);
}

bool Metadata::read_buffer(const std::vector<uint8_t>& buf, const metadata::ReadContext& file, metadata_dest_func dest)
{
    core::BinaryDecoder dec(buf);
    return read_buffer(dec, file, dest);
}

bool Metadata::read_buffer(core::BinaryDecoder& dec, const metadata::ReadContext& file, metadata_dest_func dest)
{
    bool canceled = false;
    string signature;
    unsigned version;
    while (dec)
    {
        if (canceled) continue;
        core::BinaryDecoder inner = dec.pop_metadata_bundle(signature, version);

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
            if (md->source().style() == types::Source::Style::INLINE)
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
            core::BinaryDecoder dec(bundle.data);
            Metadata::read_group(dec, bundle.version, file, dest);
        } else {
            unique_ptr<Metadata> md(new Metadata);
            iotrace::trace_file(file.pathname, 0, 0, "read metadata");
            core::BinaryDecoder dec(bundle.data);
            md->read_inner(dec, bundle.version, file);

            // If the source is inline, then the data follows the metadata
            if (md->source().style() == types::Source::Style::INLINE)
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
            core::BinaryDecoder dec(bundle.data);
            Metadata::read_group(dec, bundle.version, file, dest);
        } else {
            unique_ptr<Metadata> md(new Metadata);
            iotrace::trace_file(file.pathname, 0, 0, "read metadata");
            core::BinaryDecoder dec(bundle.data);
            md->read_inner(dec, bundle.version, file);

            // If the source is inline, then the data follows the metadata
            if (md->source().style() == types::Source::Style::INLINE)
                md->read_inline_data(fd);
            canceled = !dest(move(md));
        }
    }
    return !canceled;
}

bool Metadata::read_group(core::BinaryDecoder& dec, unsigned version, const metadata::ReadContext& file, metadata_dest_func dest)
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

    core::BinaryDecoder unenc(decomp);

    string isig;
    unsigned iver;
    bool canceled = false;
    while (!canceled && unenc)
    {
        core::BinaryDecoder inner = unenc.pop_metadata_bundle(isig, iver);
        unique_ptr<Metadata> md(new Metadata);
        md->read_inner(inner, iver, file);
        canceled = !dest(move(md));
    }

    return !canceled;
}

}
