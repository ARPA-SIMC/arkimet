#include "metadata.h"
#include "metadata/data.h"
#include "core/file.h"
#include "core/binary.h"
#include "exceptions.h"
#include "types/bundle.h"
#include "types/value.h"
#include "types/source/blob.h"
#include "types/source/inline.h"
#include "types/note.h"
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
#include <cstring>
#include <cerrno>
#include <cassert>
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


/*
 * Index
 */

Index::~Index()
{
    for (auto& i: items)
        delete i;
}

void Index::raw_append(std::unique_ptr<types::Type> t)
{
    items.emplace_back(t.release());
}

Index::const_iterator Index::values_end() const
{
    const_iterator i = items.begin();
    for ( ; i != items.end(); ++i)
    {
        auto c = (*i)->type_code();
        if (c == TYPE_NOTE || c == TYPE_SOURCE)
            break;
    }
    return i;
}

bool Index::has(types::Code code) const
{
    for (const auto& i: items)
        if (i->type_code() == code)
            return true;
    return false;
}

const Type* Index::get(Code code) const
{
    for (const auto& i: items)
        if (i->type_code() == code)
            return i;
    return nullptr;
}

const types::Source* Index::get_source() const
{
    if (items.empty())
        return nullptr;

    if (items.back()->type_code() != TYPE_SOURCE)
        return nullptr;

    // We checked the type_code, so we can reinterpret cast
    return reinterpret_cast<types::Source*>(items.back());
}

types::Source* Index::get_source()
{
    if (items.empty())
        return nullptr;

    if (items.back()->type_code() != TYPE_SOURCE)
        return nullptr;

    // We checked the type_code, so we can reinterpret cast
    return reinterpret_cast<types::Source*>(items.back());
}

void Index::set_source(std::unique_ptr<types::Source> s)
{
    if (items.empty() || items.back()->type_code() != TYPE_SOURCE)
        items.emplace_back(s.release());
    else
    {
        delete items.back();
        items.back() = s.release();
    }
}

void Index::unset_source()
{
    if (items.empty() || items.back()->type_code() != TYPE_SOURCE)
        return;

    delete items.back();
    items.pop_back();
}

std::pair<Index::const_iterator, Index::const_iterator> Index::notes() const
{
    // TODO: is this faster if we iterate forward?
    auto b = items.rbegin();
    if (b != items.rend() && (*b)->type_code() == TYPE_SOURCE)
        ++b;

    auto e = b;
    while (e != items.rend() && (*e)->type_code() == TYPE_NOTE)
        ++e;

    return std::make_pair(e.base(), b.base());
}

void Index::clear_notes()
{
    auto b = items.rbegin();
    if (b != items.rend() && (*b)->type_code() == TYPE_SOURCE)
        ++b;

    auto e = b;
    while (e != items.rend() && (*e)->type_code() == TYPE_NOTE)
    {
        delete *e;
        ++e;
    }

    items.erase(e.base(), b.base());
}

void Index::append_note(std::unique_ptr<types::Note> n)
{
    auto pos = items.end();
    if (!items.empty() && items.back()->type_code() == TYPE_SOURCE)
        --pos;
    items.emplace(pos, n.release());
}

const types::Note* Index::get_last_note() const
{
    auto n = items.rbegin();
    if (n != items.rend() && (*n)->type_code() == TYPE_SOURCE)
        ++n;
    if ((*n)->type_code() == TYPE_NOTE)
        return reinterpret_cast<const types::Note*>(*n);
    return nullptr;
}

void Index::clone_fill(const Index& o)
{
    assert(items.empty());
    for (const auto& i: o.items)
        items.emplace_back(i->clone());
}

void Index::set_value(std::unique_ptr<types::Type> item)
{
    auto code = item->type_code();
    assert(code != TYPE_SOURCE);
    assert(code != TYPE_NOTE);

    auto end = values_end();

    // TODO: in theory, this could be rewritten with rbegin/rend to optimize
    // for the insertion of sorted data. In practice, after trying, it caused a
    // decrease in performance, so abandoning that for now
    for (auto i = items.begin(); i != end; ++i)
    {
        auto icode = (*i)->type_code();
        if (icode == code)
        {
            delete *i;
            *i = item.release();
            return;
        } else if (icode > code) {
            items.emplace(i, item.release());
            return;
        }
    }

    items.emplace(end, item.release());
}

void Index::unset_value(types::Code code)
{
    auto i = items.begin();
    for ( ; i != items.end(); ++i)
    {
        auto c = (*i)->type_code();
        if (c == TYPE_NOTE || c == TYPE_SOURCE)
            break;

        if (c == code)
        {
            items.erase(i);
            break;
        }
    }
}

}


/*
 * Metadata
 */

Metadata::Metadata()
{
}

Metadata::Metadata(const uint8_t* encoded, unsigned size)
    : m_encoded_size(size)
{
    uint8_t* tdata = new uint8_t[size];
    memcpy(tdata, encoded, size);
    m_encoded = tdata;
}

Metadata::~Metadata()
{
    delete[] m_encoded;
}

std::shared_ptr<Metadata> Metadata::clone() const
{
    auto res = std::make_shared<Metadata>();
    res->m_index.clone_fill(m_index);
    res->m_data = m_data;
    return res;
}

void Metadata::set(const types::Type& item)
{
    m_index.set_value(std::unique_ptr<types::Type>(item.clone()));
}

void Metadata::test_set(const types::Type& item)
{
    std::unique_ptr<types::Type> clone(item.clone());
    m_index.set_value(std::move(clone));
}

void Metadata::test_set(std::unique_ptr<types::Type> item)
{
    m_index.set_value(std::move(item));
}

void Metadata::test_set(const std::string& type, const std::string& val)
{
    test_set(types::decodeString(types::parseCodeName(type.c_str()), val));
}

void Metadata::merge(const Metadata& md)
{
    // TODO: this is currently only used in the mock BUFR scanner.
    // If it gets used in more performance critical code, it should get
    // rewritten with performance in mind
    auto end = md.m_index.values_end();
    for (auto i = md.m_index.begin(); i != end; ++i)
    {
        std::unique_ptr<Type> newval((*i)->clone());
        m_index.set_value(std::move(newval));
    }
}

void Metadata::diff_items(const Metadata& o, std::function<void(types::Code code, const types::Type* first, const types::Type* second)> dest) const
{
    auto values_end = m_index.values_end();
    auto ovalues_end = o.m_index.values_end();

    // Compare skipping VALUE items
    auto a = m_index.begin();
    auto b = o.m_index.begin();
    while (a != values_end || b != ovalues_end)
    {
        if (a == values_end)
        {
            auto code = (*b)->type_code();
            if (code != TYPE_VALUE)
                dest(code, nullptr, *b);
            ++b;
            continue;
        }
        if (b == ovalues_end)
        {
            auto code = (*a)->type_code();
            if (code != TYPE_VALUE)
                dest(code, *a, nullptr);
            ++a;
            continue;
        }

        auto acode = (*a)->type_code();
        auto bcode = (*b)->type_code();

        if (acode < bcode)
        {
            if (acode != TYPE_VALUE)
                dest(acode, *a, nullptr);
            ++a;
            continue;
        }

        if (acode > bcode)
        {
            if (bcode != TYPE_VALUE)
                dest(bcode, nullptr, *b);
            ++b;
            continue;
        }

        if (**a != **b)
            if (acode != TYPE_VALUE)
                dest(acode, *a, *b);
        ++a;
        ++b;
    }
}

const Source& Metadata::source() const
{
    const types::Source* s = m_index.get_source();
    if (!s)
        throw_consistency_error("metadata has no source");
    return *s;
}

const types::source::Blob* Metadata::has_source_blob() const
{
    const types::Source* s = m_index.get_source();
    if (!s) return nullptr;
    if (s->style() != types::Source::Style::BLOB) return nullptr;
    return reinterpret_cast<const source::Blob*>(s);
}

const source::Blob& Metadata::sourceBlob() const
{
    const types::Source* s = m_index.get_source();
    if (!s) throw_consistency_error("metadata has no source");
    if (s->style() != types::Source::Style::BLOB)
        throw_consistency_error("metadata source is not a Blob source");
    return *reinterpret_cast<const source::Blob*>(s);
}

source::Blob& Metadata::sourceBlob()
{
    types::Source* s = m_index.get_source();
    if (!s) throw_consistency_error("metadata has no source");
    if (s->style() != types::Source::Style::BLOB)
        throw_consistency_error("metadata source is not a Blob source");
    return *reinterpret_cast<source::Blob*>(s);
}

void Metadata::set_source(std::unique_ptr<types::Source> s)
{
    m_index.set_source(std::move(s));
}

void Metadata::set_source_inline(const std::string& format, std::shared_ptr<metadata::Data> data)
{
    m_data = data;
    set_source(Source::createInline(format, m_data->size()));
}

void Metadata::unset_source()
{
    m_index.unset_source();
}

void Metadata::clear_notes()
{
    m_index.clear_notes();
}

void Metadata::encode_notes(core::BinaryEncoder& enc) const
{
    metadata::Index::const_iterator i, end;
    std::tie(i, end) = m_index.notes();
    for ( ; i != end; ++i)
        (*i)->encodeBinary(enc);
}

void Metadata::set_notes_encoded(const std::vector<uint8_t>& notes)
{
    // TODO: this could be optimizez by replacing existing notes instead of
    // removing and readding.
    // It is only called by iseg and ondisk2 indices while recomposing the
    // metadata from index bits. That whole process could be refactored, and
    // made to efficiently compose a metadata in the right order. That would
    // make this method unnecessary.
    clear_notes();
    core::BinaryDecoder dec(notes);
    while (dec)
    {
        TypeCode el_type;
        core::BinaryDecoder inner = dec.pop_type_envelope(el_type);

        auto n = types::Type::decode_inner(el_type, inner, false);
        std::unique_ptr<Note> nn(reinterpret_cast<types::Note*>(n.release()));
        m_index.append_note(std::move(nn));
    }
}

void Metadata::add_note(const types::Note& note)
{
    m_index.append_note(std::unique_ptr<types::Note>(note.clone()));
}

void Metadata::add_note(const std::string& note)
{
    m_index.append_note(Note::create(note));
}

const types::Note& Metadata::get_last_note() const
{
    auto n = m_index.get_last_note();
    if (!n)
        throw std::runtime_error("no notes found");
    return *n;
}


bool Metadata::operator==(const Metadata& m) const
{
    if (m_index.size() != m.m_index.size())
        return false;
    auto a = m_index.begin();
    auto b = m.m_index.begin();
    for ( ; a != m_index.end(); ++a, ++b)
        if (**a != **b)
            return false;
    return true;
}

bool Metadata::items_equal(const Metadata& o) const
{
    bool res = true;
    diff_items(o, [&](types::Code code, const types::Type* first, const types::Type* second) { res = false; });
    return res;
}

std::shared_ptr<Metadata> Metadata::read_binary(int in, const metadata::ReadContext& filename, bool readInline)
{
    types::Bundle bundle;
    NamedFileDescriptor f(in, filename.pathname);
    if (!bundle.read_header(f))
        return std::shared_ptr<Metadata>();

    // Ensure first 2 bytes are MD or !D
    if (bundle.signature != "MD")
        throw_consistency_error("parsing file " + filename.pathname, "metadata entry does not start with 'MD'");

    if (!bundle.read_data(f))
        return std::shared_ptr<Metadata>();

    core::BinaryDecoder inner(bundle.data);

    auto res = read_binary_inner(inner, bundle.version, filename);

    // If the source is inline, then the data follows the metadata
    if (readInline && res->source().style() == types::Source::Style::INLINE)
        res ->read_inline_data(f);

    return res;
}

std::shared_ptr<Metadata> Metadata::read_binary(core::BinaryDecoder& dec, const metadata::ReadContext& filename, bool readInline)
{
    if (!dec) return std::shared_ptr<Metadata>();

    string signature;
    unsigned version;
    core::BinaryDecoder inner = dec.pop_metadata_bundle(signature, version);

    // Ensure first 2 bytes are MD or !D
    if (signature != "MD")
        throw std::runtime_error("cannot parse " + filename.pathname + ": metadata entry does not start with 'MD'");

    auto res = read_binary_inner(inner, version, filename);

    // If the source is inline, then the data follows the metadata
    if (readInline && res->source().style() == types::Source::Style::INLINE)
        res->readInlineData(dec, filename.pathname);

    return res;
}

std::shared_ptr<Metadata> Metadata::read_binary_inner(core::BinaryDecoder& dec, unsigned version, const metadata::ReadContext& rc)
{
    // Check version and ensure we can decode
    if (version != 0)
    {
        std::stringstream s;
        s << "cannot parse file " << rc.pathname << ": version of the file is " << version << " but I can only decode version 0";
        throw std::runtime_error(s.str());
    }

    // Parse the various elements
    auto res = std::make_shared<Metadata>(dec.buf, dec.size);
    core::BinaryDecoder mddec(res->m_encoded, res->m_encoded_size);
    TypeCode last_code = TYPE_INVALID;
    while (mddec)
    {
        TypeCode el_type;
        core::BinaryDecoder inner = mddec.pop_type_envelope(el_type);

        switch (el_type)
        {
            case TYPE_NOTE:
                if (last_code == TYPE_SOURCE)
                {
                    std::stringstream s;
                    s << "cannot parse file " << rc.pathname << ": element of type " << types::formatCode(el_type) << " should not follow one of type SOURCE";
                    throw std::runtime_error(s.str());
                }
                res->m_index.raw_append(types::Type::decode_inner(el_type, inner, true));
                break;
            case TYPE_SOURCE:
                res->m_index.raw_append(types::Source::decodeRelative(inner, rc.basedir));
                break;
            default:
                if (last_code == TYPE_SOURCE || last_code == TYPE_NOTE)
                {
                    std::stringstream s;
                    s << "cannot parse file " << rc.pathname << ": element of type " << types::formatCode(el_type) << " should not follow one of type " << types::formatCode(last_code);
                    throw std::runtime_error(s.str());
                }
                res->m_index.raw_append(types::Type::decode_inner(el_type, inner, true));
                break;
        }

        last_code = el_type;
    }

    return res;
}

void Metadata::read_inline_data(NamedFileDescriptor& fd)
{
    // If the source is inline, then the data follows the metadata
    const Source& s = source();
    if (s.style() != types::Source::Style::INLINE) return;
    const source::Inline* si = reinterpret_cast<const source::Inline*>(&s);
    vector<uint8_t> buf;
    buf.resize(si->size);

    iotrace::trace_file(fd, 0, si->size, "read inline data");

    // Read the inline data
    if (!fd.read_all_or_retry(buf.data(), si->size))
        fd.throw_runtime_error("inline data not found after arkimet metadata");
    m_data = metadata::DataManager::get().to_data(s.format, move(buf));
}

void Metadata::read_inline_data(core::AbstractInputFile& fd)
{
    // If the source is inline, then the data follows the metadata
    const Source& s = source();
    if (s.style() != types::Source::Style::INLINE) return;
    const source::Inline* si = reinterpret_cast<const source::Inline*>(&s);
    vector<uint8_t> buf;
    buf.resize(si->size);

    iotrace::trace_file(fd, 0, si->size, "read inline data");

    // Read the inline data
    fd.read(buf.data(), si->size);
    m_data = metadata::DataManager::get().to_data(s.format, move(buf));
}

void Metadata::readInlineData(core::BinaryDecoder& dec, const std::string& filename)
{
    // If the source is inline, then the data follows the metadata
    const Source& s = source();
    if (s.style() != types::Source::Style::INLINE) return;
    const source::Inline* si = reinterpret_cast<const source::Inline*>(&s);
    core::BinaryDecoder data = dec.pop_data(si->size, "inline data");
    m_data = metadata::DataManager::get().to_data(s.format, std::vector<uint8_t>(data.buf, data.buf + si->size));
}

std::shared_ptr<Metadata> Metadata::read_yaml(LineReader& in, const std::string& filename)
{
    if (in.eof())
        return std::shared_ptr<Metadata>();

    std::shared_ptr<Metadata> res;

    YamlStream yamlStream;
    for (YamlStream::const_iterator i = yamlStream.begin(in);
            i != yamlStream.end(); ++i)
    {
        if (!res)
            res = std::make_shared<Metadata>();
        types::Code type = types::parseCodeName(i->first);
        string val = str::strip(i->second);
        switch (type)
        {
            case TYPE_NOTE: res->m_index.append_note(types::Note::decodeString(val)); break;
            case TYPE_SOURCE: res->m_index.set_source(types::Source::decodeString(val)); break;
            default:
                res->m_index.set_value(types::decodeString(type, val));
        }
    }

    return res;
}

void Metadata::write(NamedFileDescriptor& out) const
{
    // Prepare the encoded data
    vector<uint8_t> encoded = encodeBinary();

    // Write out
    out.write_all_or_retry(encoded.data(), encoded.size());

    // If the source is inline, then the data follows the metadata
    const Source* s = m_index.get_source();
    if (s->style() != Source::Style::INLINE)
        return;

    // Having checked the style, we can reinterpret_cast
    const source::Inline* si = reinterpret_cast<const source::Inline*>(s);
    if (si->size != m_data->size())
    {
        stringstream ss;
        ss << "cannot write metadata to file " << out.name() << ": metadata size " << si->size << " does not match the data size " << m_data->size();
        throw runtime_error(ss.str());
    }
    m_data->write_inline(out);
}

void Metadata::write(AbstractOutputFile& out) const
{
    // Prepare the encoded data
    vector<uint8_t> encoded = encodeBinary();

    // Write out
    out.write(encoded.data(), encoded.size());

    // If the source is inline, then the data follows the metadata
    const Source* s = m_index.get_source();
    if (s->style() != Source::Style::INLINE)
        return;

    // Having checked the style, we can reinterpret_cast
    const source::Inline* si = reinterpret_cast<const source::Inline*>(s);
    if (si->size != m_data->size())
    {
        stringstream ss;
        ss << "cannot write metadata to file " << out.name() << ": metadata size " << si->size << " does not match the data size " << m_data->size();
        throw runtime_error(ss.str());
    }
    m_data->write_inline(out);
}

std::string Metadata::to_yaml(const Formatter* formatter) const
{
    metadata::Index::const_iterator notes_begin, notes_end;
    std::tie(notes_begin, notes_end) = m_index.notes();

    std::stringstream buf;

    if (notes_end != m_index.end())
        buf << "Source: " << **notes_end << endl;

    auto i = m_index.begin();
    for ( ; i != notes_begin; ++i)
    {
        std::string uc = str::lower((*i)->tag());
        uc[0] = toupper(uc[0]);
        buf << uc << ": ";
        (*i)->writeToOstream(buf);
        if (formatter)
            buf << "\t# " << formatter->format(**i);
        buf << endl;
    }

    if (i != notes_end)
    {
        buf << "Note: ";
        if (i + 1 == notes_end)
            buf << **i << endl;
        else
        {
            for ( ; i != notes_end; ++i)
            {
                buf << endl;
                buf << " " << **i << endl;
            }
        }
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
    metadata::Index::const_iterator notes_begin, notes_end;
    std::tie(notes_begin, notes_end) = m_index.notes();

    e.start_mapping();
    e.add(keys.metadata_items);

    e.start_list();
    // Source
    const Source* s = nullptr;
    if (notes_end != m_index.end())
    {
        s = reinterpret_cast<const Source*>(*notes_end);
        s->serialise(e, keys, f);
    }
    // Values
    auto i = m_index.begin();
    for ( ; i != notes_begin; ++i)
        (*i)->serialise(e, keys, f);
    e.end_list();

    e.add(keys.metadata_notes);
    e.start_list();
    for ( ; i != notes_end; ++i)
        (*i)->serialise(e, keys, f);
    e.end_list();
    e.end_mapping();

    e.add_break();

    // If the source is inline, we need to include the daata
    if (s->style() != types::Source::Style::INLINE)
        return;
    const source::Inline* si = reinterpret_cast<const source::Inline*>(s);
    if (si->size != m_data->size())
    {
        stringstream ss;
        ss << "cannot write metadata to JSON: metadata source size " << si->size << " does not match the data size " << m_data->size();
        throw runtime_error(ss.str());
    }
    m_data->emit(e);
}

std::shared_ptr<Metadata> Metadata::read_structure(const structured::Keys& keys, const structured::Reader& val)
{
    auto res = std::make_shared<Metadata>();

    // Parse items
    val.sub(keys.metadata_items, "metadata items", [&](const structured::Reader& items) {
        unsigned size = items.list_size("metadata items");
        for (unsigned idx = 0; idx < size; ++idx)
        {
            std::unique_ptr<types::Type> item = items.as_type(idx, "metadata item", keys);
            switch (item->type_code())
            {
                case TYPE_SOURCE:
                {
                    std::unique_ptr<Source> s(reinterpret_cast<Source*>(item.release()));
                    res->m_index.set_source(std::move(s));
                    break;
                }
                case TYPE_NOTE:
                {
                    std::unique_ptr<Note> n(reinterpret_cast<Note*>(item.release()));
                    res->m_index.append_note(std::move(n));
                    break;
                }
                default:
                    res->m_index.set_value(std::move(item));
            }
        }
    });

    // Parse notes
    val.sub(keys.metadata_notes, "metadata notes", [&](const structured::Reader& notes) {
        unsigned size = notes.list_size("metadata notes");
        for (unsigned idx = 0; idx < size; ++idx)
        {
            unique_ptr<types::Type> item = notes.as_type(idx, "metadata note", keys);
            if (item->type_code() == TYPE_NOTE)
                res->add_note(*downcast<types::Note>(move(item)));
        }
    });

    return res;
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
    vector<uint8_t> encoded;
    core::BinaryEncoder subenc(encoded);

    for (const auto& i: m_index)
        i->encodeBinary(subenc);

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

    const Source* s = m_index.get_source();

    // If we don't have it in cache and we don't have a source, we cannot know
    // how to load it or what to reconstruct: give up
    if (!s) throw_consistency_error("cannot retrieve data: data source is not defined");

    // If we don't have it in cache, try reconstructing it from the Value metadata
    if (const Value* value = get<types::Value>())
        m_data = metadata::DataManager::get().to_data(s->format, scan::Scanner::reconstruct(s->format, *this, value->buffer));
    if (m_data) return *m_data;

    // Load it according to source
    switch (s->style())
    {
        case Source::Style::INLINE:
            throw runtime_error("cannot retrieve data: data is not found on INLINE metadata");
        case Source::Style::URL:
            throw runtime_error("cannot retrieve data: data is not accessible for URL metadata");
        case Source::Style::BLOB:
        {
            const source::Blob* blob = reinterpret_cast<const source::Blob*>(s);
            if (!blob->reader)
                throw runtime_error("cannot retrieve data: BLOB source has no reader associated");
            m_data = metadata::DataManager::get().to_data(blob->format, blob->read_data());
            return *m_data;
        }
        default:
            throw_consistency_error("cannot retrieve data: unsupported source style");
    }
}

size_t Metadata::stream_data(NamedFileDescriptor& out)
{
    // This code is a copy of get_data, except that if the data is not
    // previously cached and gets streamed, it won't be stored in m_data, to
    // prevent turning a stream operation into a memory-filling operation

    // First thing, try and return it from cache
    if (m_data) return m_data->write(out);

    const Source* s = m_index.get_source();

    // If we don't have it in cache and we don't have a source, we cannot know
    // how to load it or what to reconstruct: give up
    if (!s) throw_consistency_error("cannot stream data: data source is not defined");

    // If we don't have it in cache, try reconstructing it from the Value metadata
    if (const Value* value = get<types::Value>())
        m_data = metadata::DataManager::get().to_data(s->format, scan::Scanner::reconstruct(s->format, *this, value->buffer));
    if (m_data) return m_data->write(out);

    // Load it according to source
    switch (s->style())
    {
        case Source::Style::INLINE:
            throw runtime_error("cannot stream data: data is not found on INLINE metadata");
        case Source::Style::URL:
            throw runtime_error("cannot stream data: data is not accessible for URL metadata");
        case Source::Style::BLOB:
        {
            // Do not directly use m_data so that if dataReader.read throws an
            // exception, m_data remains empty.
            const source::Blob* blob = reinterpret_cast<const source::Blob*>(s);
            if (!blob->reader)
                throw runtime_error("cannot stream data: BLOB source has no reader associated");
            return blob->stream_data(out);
        }
        default:
            throw_consistency_error("cannot stream data: unsupported source style");
    }
}

size_t Metadata::stream_data(AbstractOutputFile& out)
{
    // This code is a copy of get_data, except that if the data is not
    // previously cached and gets streamed, it won't be stored in m_data, to
    // prevent turning a stream operation into a memory-filling operation

    // First thing, try and return it from cache
    if (m_data) return m_data->write(out);

    const Source* s = m_index.get_source();

    // If we don't have it in cache and we don't have a source, we cannot know
    // how to load it or what to reconstruct: give up
    if (!s) throw_consistency_error("cannot stream data: data source is not defined");

    // If we don't have it in cache, try reconstructing it from the Value metadata
    if (const Value* value = get<types::Value>())
        m_data = metadata::DataManager::get().to_data(s->format, scan::Scanner::reconstruct(s->format, *this, value->buffer));
    if (m_data) return m_data->write(out);

    // Load it according to source
    switch (s->style())
    {
        case Source::Style::INLINE:
            throw runtime_error("cannot stream data: data is not found on INLINE metadata");
        case Source::Style::URL:
            throw runtime_error("cannot stream data: data is not accessible for URL metadata");
        case Source::Style::BLOB:
        {
            // Do not directly use m_data so that if dataReader.read throws an
            // exception, m_data remains empty.
            const source::Blob* blob = reinterpret_cast<const source::Blob*>(s);
            if (!blob->reader)
                throw runtime_error("cannot stream data: BLOB source has no reader associated");
            return blob->stream_data(out);
        }
        default:
            throw_consistency_error("cannot stream data: unsupported source style");
    }
}

void Metadata::drop_cached_data()
{
    auto s = m_index.get_source();
    if (s && s->style() == Source::Style::BLOB)
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
    auto source = m_index.get_source();
    if (!source) throw_consistency_error("cannot inline source in metadata: data source is not defined");
    get_data();
    set_source(Source::createInline(source->format, m_data->size()));
}

void Metadata::make_absolute()
{
    const source::Blob* blob = has_source_blob();
    if (!blob)
        return;
    set_source(blob->makeAbsolute());
}

size_t Metadata::data_size() const
{
    if (m_data) return m_data->size();

    const Source* s = m_index.get_source();
    if (!s) return 0;

    // Query according to source
    switch (s->style())
    {
        case Source::Style::INLINE:
            return reinterpret_cast<const source::Inline*>(s)->size;
        case Source::Style::URL:
            // URL does not know about sizes
            return 0;
        case Source::Style::BLOB:
            return reinterpret_cast<const source::Blob*>(s)->size;
        default:
            // An unsupported source type should make more noise than a 0
            // return type
            throw_consistency_error("cannot retrieve data: unsupported source style" + Source::formatStyle(s->style()));
    }
}

void Metadata::dump_internals(FILE* out) const
{
    fprintf(out, "Metadata contents:\n");
    if (m_encoded)
        fprintf(out, "  Has encoded buffer %ub long\n", m_encoded_size);
    if (m_data)
        fprintf(out, "  Has cached data %zdb long\n", m_data->size());
    fprintf(out, "  Item index:\n");
    unsigned idx = 0;
    for (const auto& i: m_index)
        fprintf(out, "    %3u: %s: %s\n", idx++, i->tag().c_str(), i->to_string().c_str());
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
            iotrace::trace_file(file.pathname, 0, 0, "read metadata");
            auto res = read_binary_inner(inner, version, file);

            // If the source is inline, then the data follows the metadata
            if (res->source().style() == types::Source::Style::INLINE)
                res->readInlineData(dec, file.pathname);
            canceled = !dest(move(res));
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
            iotrace::trace_file(file.pathname, 0, 0, "read metadata");
            core::BinaryDecoder dec(bundle.data);
            auto md = Metadata::read_binary_inner(dec, bundle.version, file);

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
            iotrace::trace_file(file.pathname, 0, 0, "read metadata");
            core::BinaryDecoder dec(bundle.data);
            auto md = Metadata::read_binary_inner(dec, bundle.version, file);

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
        auto md = Metadata::read_binary_inner(inner, iver, file);
        canceled = !dest(move(md));
    }

    return !canceled;
}

}
