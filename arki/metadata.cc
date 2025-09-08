#include "metadata.h"
#include "core/file.h"
#include "exceptions.h"
#include "formatter.h"
#include "iotrace.h"
#include "metadata/data.h"
#include "metadata/reader.h"
#include "scan.h"
#include "stream.h"
#include "structured/emitter.h"
#include "structured/keys.h"
#include "structured/reader.h"
#include "types/bundle.h"
#include "types/note.h"
#include "types/source/blob.h"
#include "types/source/inline.h"
#include "types/value.h"
#include "utils/compress.h"
#include "utils/string.h"
#include "utils/yaml.h"
#include <cassert>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>

using namespace std;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::core;

namespace {

std::filesystem::path canonical_ifexists(const std::filesystem::path path)
{
    try
    {
        return std::filesystem::canonical(path);
    }
    catch (std::filesystem::filesystem_error&)
    {
        return path;
    }
}

} // namespace

namespace arki {
namespace metadata {

/*
 * Index
 */

Index::~Index()
{
    for (auto& i : items)
        delete i;
}

void Index::raw_append(std::unique_ptr<types::Type> t)
{
    items.emplace_back(t.release());
}

Index::const_iterator Index::values_end() const
{
    const_iterator i = items.begin();
    for (; i != items.end(); ++i)
    {
        auto c = (*i)->type_code();
        if (c == TYPE_NOTE || c == TYPE_SOURCE)
            break;
    }
    return i;
}

bool Index::has(types::Code code) const
{
    for (const auto& i : items)
        if (i->type_code() == code)
            return true;
    return false;
}

const Type* Index::get(Code code) const
{
    for (const auto& i : items)
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
    for (const auto& i : o.items)
        items.emplace_back(i->clone());
}

void Index::set_value(std::unique_ptr<types::Type> item)
{
    auto code = item->type_code();
    assert(code != TYPE_SOURCE);
    assert(code != TYPE_NOTE);

    // FIXME post Centos7: this should be 'auto', but gcc on Centos7 cannot
    // tell that const_iterator is the same as
    // std::vector<types::Type*>::const_iterator
    std::vector<types::Type*>::const_iterator end = values_end();

    // TODO: in theory, this could be rewritten with rbegin/rend to optimize
    // for the insertion of sorted data. In practice, after trying, it caused a
    // decrease in performance, so abandoning that for now
    auto i = items.begin();
    for (; i != end; ++i)
    {
        auto icode = (*i)->type_code();
        if (icode == code)
        {
            delete *i;
            *i = item.release();
            return;
        }
        else if (icode > code)
        {
            items.emplace(i, item.release());
            return;
        }
    }

    // FIXME: Centos7 workaround: despite STL documentation, gcc in Centos7
    // requires an interator on insert/emplace instead of a const_iterator
    // So we don't limit 'i' to the for scope and directly use 'end' here, but
    // we reuse 'i'
    items.emplace(i, item.release());
}

void Index::unset_value(types::Code code)
{
    for (auto i = items.begin(); i != items.end(); ++i)
    {
        auto c = (*i)->type_code();
        if (c == TYPE_NOTE || c == TYPE_SOURCE)
            break;

        if (c == code)
        {
            delete *i;
            items.erase(i);
            break;
        }
    }
}

} // namespace metadata

/*
 * Metadata
 */

Metadata::Metadata() {}

Metadata::Metadata(const uint8_t* encoded, unsigned size) : m_encoded_size(size)
{
    uint8_t* tdata = new uint8_t[size];
    memcpy(tdata, encoded, size);
    m_encoded = tdata;
}

Metadata::~Metadata() { delete[] m_encoded; }

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

void Metadata::diff_items(
    const Metadata& o,
    std::function<void(types::Code code, const types::Type* first,
                       const types::Type* second)>
        dest) const
{
    auto values_end  = m_index.values_end();
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
    if (!s)
        return nullptr;
    if (s->style() != types::Source::Style::BLOB)
        return nullptr;
    return reinterpret_cast<const source::Blob*>(s);
}

const source::Blob& Metadata::sourceBlob() const
{
    const types::Source* s = m_index.get_source();
    if (!s)
        throw_consistency_error("metadata has no source");
    if (s->style() != types::Source::Style::BLOB)
        throw_consistency_error("metadata source is not a Blob source");
    return *reinterpret_cast<const source::Blob*>(s);
}

source::Blob& Metadata::sourceBlob()
{
    types::Source* s = m_index.get_source();
    if (!s)
        throw_consistency_error("metadata has no source");
    if (s->style() != types::Source::Style::BLOB)
        throw_consistency_error("metadata source is not a Blob source");
    return *reinterpret_cast<source::Blob*>(s);
}

void Metadata::set_source(std::unique_ptr<types::Source> s)
{
    m_index.set_source(std::move(s));
}

void Metadata::set_source_inline(DataFormat format,
                                 std::shared_ptr<metadata::Data> data)
{
    m_data = data;
    set_source(Source::createInline(format, m_data->size()));
}

void Metadata::unset_source() { m_index.unset_source(); }

void Metadata::clear_notes() { m_index.clear_notes(); }

void Metadata::encode_notes(core::BinaryEncoder& enc) const
{
    metadata::Index::const_iterator i, end;
    std::tie(i, end) = m_index.notes();
    for (; i != end; ++i)
        (*i)->encodeBinary(enc);
}

void Metadata::set_notes_encoded(const uint8_t* data, unsigned size)
{
    // TODO: this could be optimized by replacing existing notes instead of
    // removing and readding.
    // It is only called by iseg indices while recomposing the
    // metadata from index bits. That whole process could be refactored, and
    // made to efficiently compose a metadata in the right order. That would
    // make this method unnecessary.
    clear_notes();
    core::BinaryDecoder dec(data, size);
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

void Metadata::add_note_scanned_from(const std::filesystem::path& source)
{
    stringstream note;
    note << "Scanned from " << source.filename().native();
    m_index.append_note(Note::create(note.str()));
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
    for (; a != m_index.end(); ++a, ++b)
        if (**a != **b)
            return false;
    return true;
}

bool Metadata::items_equal(const Metadata& o) const
{
    bool res = true;
    diff_items(o, [&](types::Code code, const types::Type* first,
                      const types::Type* second) noexcept { res = false; });
    return res;
}

std::shared_ptr<Metadata>
Metadata::read_binary_inner(core::BinaryDecoder& dec, unsigned version,
                            const std::filesystem::path& path,
                            const std::filesystem::path& basedir)
{
    // Check version and ensure we can decode
    if (version != 0)
    {
        std::stringstream s;
        s << path.native() << ": version of the file is " << version
          << " but I can only decode version 0";
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
                    s << path << ": element of type "
                      << types::formatCode(el_type)
                      << " should not follow one of type SOURCE";
                    throw std::runtime_error(s.str());
                }
                res->m_index.raw_append(
                    types::Type::decode_inner(el_type, inner, true));
                break;
            case TYPE_SOURCE:
                res->m_index.raw_append(
                    types::Source::decodeRelative(inner, basedir));
                break;
            default:
                if (last_code == TYPE_SOURCE || last_code == TYPE_NOTE)
                {
                    std::stringstream s;
                    s << path << ": element of type "
                      << types::formatCode(el_type)
                      << " should not follow one of type "
                      << types::formatCode(last_code);
                    throw std::runtime_error(s.str());
                }
                res->m_index.raw_append(
                    types::Type::decode_inner(el_type, inner, true));
                break;
        }

        last_code = el_type;
    }

    return res;
}

size_t Metadata::read_inline_data(NamedFileDescriptor& fd)
{
    // If the source is inline, then the data follows the metadata
    const Source& s = source();
    if (s.style() != types::Source::Style::INLINE)
        return 0;
    const source::Inline* si = reinterpret_cast<const source::Inline*>(&s);
    vector<uint8_t> buf;
    buf.resize(si->size);

    iotrace::trace_file(fd, 0, si->size, "read inline data");

    // Read the inline data
    if (!fd.read_all_or_retry(buf.data(), si->size))
        fd.throw_runtime_error("inline data not found after arkimet metadata");
    m_data = metadata::DataManager::get().to_data(s.format, std::move(buf));

    return si->size;
}

size_t Metadata::read_inline_data(core::AbstractInputFile& fd)
{
    // If the source is inline, then the data follows the metadata
    const Source& s = source();
    if (s.style() != types::Source::Style::INLINE)
        return 0;
    const source::Inline* si = reinterpret_cast<const source::Inline*>(&s);
    vector<uint8_t> buf;
    buf.resize(si->size);

    iotrace::trace_file(fd, 0, si->size, "read inline data");

    // Read the inline data
    fd.read(buf.data(), si->size);
    m_data = metadata::DataManager::get().to_data(s.format, std::move(buf));

    return si->size;
}

void Metadata::readInlineData(core::BinaryDecoder& dec,
                              const std::filesystem::path& filename)
{
    // If the source is inline, then the data follows the metadata
    const Source& s = source();
    if (s.style() != types::Source::Style::INLINE)
        return;
    const source::Inline* si = reinterpret_cast<const source::Inline*>(&s);
    core::BinaryDecoder data = dec.pop_data(si->size, "inline data");
    m_data                   = metadata::DataManager::get().to_data(
        s.format, std::vector<uint8_t>(data.buf, data.buf + si->size));
}

std::shared_ptr<Metadata>
Metadata::read_yaml(LineReader& in, const std::filesystem::path& filename)
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
        string val       = str::strip(i->second);
        switch (type)
        {
            case TYPE_NOTE:
                res->m_index.append_note(types::Note::decodeString(val));
                break;
            case TYPE_SOURCE:
                res->m_index.set_source(types::Source::decodeString(val));
                break;
            default: res->m_index.set_value(types::decodeString(type, val));
        }
    }

    return res;
}

void Metadata::write(NamedFileDescriptor& out, bool skip_data) const
{
    // Prepare the encoded data
    std::vector<uint8_t> encoded = encodeBinary();

    // Write out
    out.write_all_or_retry(encoded.data(), encoded.size());

    if (skip_data)
        return;

    // If the source is inline, then the data follows the metadata
    const Source* s = m_index.get_source();
    if (s->style() != Source::Style::INLINE)
        return;

    // Having checked the style, we can reinterpret_cast
    const source::Inline* si = reinterpret_cast<const source::Inline*>(s);
    if (si->size != m_data->size())
        throw_runtime_error("cannot write metadata to file ", out.path(),
                            ": metadata size ", si->size,
                            " does not match the data size ", m_data->size());
    m_data->write_inline(out);
}

void Metadata::write(StreamOutput& out, bool skip_data) const
{
    // Prepare the encoded data
    std::vector<uint8_t> encoded = encodeBinary();

    // Write out
    out.send_buffer(encoded.data(), encoded.size());

    if (skip_data)
        return;

    // If the source is inline, then the data follows the metadata
    const Source* s = m_index.get_source();
    if (!s || s->style() != Source::Style::INLINE)
        return;

    // Having checked the style, we can reinterpret_cast
    const source::Inline* si = reinterpret_cast<const source::Inline*>(s);
    if (si->size != m_data->size())
        throw_runtime_error("cannot write metadata to file ", out.path(),
                            ": metadata size ", si->size,
                            " does not match the data size ", m_data->size());
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
    for (; i != notes_begin; ++i)
    {
        std::string uc = str::lower((*i)->tag());
        uc[0]          = toupper(uc[0]);
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
            for (; i != notes_end; ++i)
            {
                buf << endl;
                buf << " " << **i << endl;
            }
        }
    }

    return buf.str();
}

void Metadata::serialise(structured::Emitter& e, const structured::Keys& keys,
                         const Formatter* f) const
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
    for (; i != notes_begin; ++i)
        (*i)->serialise(e, keys, f);
    e.end_list();

    e.add(keys.metadata_notes);
    e.start_list();
    for (; i != notes_end; ++i)
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
        ss << "cannot write metadata to JSON: metadata source size " << si->size
           << " does not match the data size " << m_data->size();
        throw runtime_error(ss.str());
    }
    m_data->emit(e);
}

std::shared_ptr<Metadata>
Metadata::read_structure(const structured::Keys& keys,
                         const structured::Reader& val)
{
    auto res = std::make_shared<Metadata>();

    // Parse items
    val.sub(keys.metadata_items, "metadata items",
            [&](const structured::Reader& items) {
                unsigned size = items.list_size("metadata items");
                for (unsigned idx = 0; idx < size; ++idx)
                {
                    std::unique_ptr<types::Type> item =
                        items.as_type(idx, "metadata item", keys);
                    switch (item->type_code())
                    {
                        case TYPE_SOURCE: {
                            std::unique_ptr<Source> s(
                                reinterpret_cast<Source*>(item.release()));
                            res->m_index.set_source(std::move(s));
                            break;
                        }
                        case TYPE_NOTE: {
                            std::unique_ptr<Note> n(
                                reinterpret_cast<Note*>(item.release()));
                            res->m_index.append_note(std::move(n));
                            break;
                        }
                        default: res->m_index.set_value(std::move(item));
                    }
                }
            });

    // Parse notes
    val.sub(keys.metadata_notes, "metadata notes",
            [&](const structured::Reader& notes) {
                unsigned size = notes.list_size("metadata notes");
                for (unsigned idx = 0; idx < size; ++idx)
                {
                    unique_ptr<types::Type> item =
                        notes.as_type(idx, "metadata note", keys);
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

    for (const auto& i : m_index)
        i->encodeBinary(subenc);

    // Prepend header
    enc.add_string("MD");
    enc.add_unsigned(0u, 2);
    // TODO Make it a one pass only job, by writing zeroes here the first time
    // round, then rewriting it with the actual length
    enc.add_unsigned(encoded.size(), 4);
    enc.add_raw(encoded);
}

const metadata::Data& Metadata::get_data()
{
    // First thing, try and return it from cache
    if (m_data)
        return *m_data;

    const Source* s = m_index.get_source();

    // If we don't have it in cache and we don't have a source, we cannot know
    // how to load it or what to reconstruct: give up
    if (!s)
        throw_consistency_error(
            "cannot retrieve data: data source is not defined");

    // If we don't have it in cache, try reconstructing it from the Value
    // metadata
    if (const Value* value = get<types::Value>())
        m_data = metadata::DataManager::get().to_data(
            s->format,
            scan::Scanner::reconstruct(s->format, *this, value->buffer));
    if (m_data)
        return *m_data;

    // Load it according to source
    switch (s->style())
    {
        case Source::Style::INLINE:
            throw runtime_error(
                "cannot retrieve data: data is not found on INLINE metadata");
        case Source::Style::URL:
            throw runtime_error("cannot retrieve data: data is not accessible "
                                "for URL metadata");
        case Source::Style::BLOB: {
            const source::Blob* blob = reinterpret_cast<const source::Blob*>(s);
            if (!blob->reader)
                throw runtime_error("cannot retrieve data: BLOB source has no "
                                    "reader associated");
            m_data = metadata::DataManager::get().to_data(blob->format,
                                                          blob->read_data());
            return *m_data;
        }
        default:
            throw_consistency_error(
                "cannot retrieve data: unsupported source style");
    }
}

stream::SendResult Metadata::stream_data(StreamOutput& out)
{
    // This code is a copy of get_data, except that if the data is not
    // previously cached and gets streamed, it won't be stored in m_data, to
    // prevent turning a stream operation into a memory-filling operation

    // First thing, try and return it from cache
    if (m_data)
        return m_data->write(out);

    const Source* s = m_index.get_source();

    // If we don't have it in cache and we don't have a source, we cannot know
    // how to load it or what to reconstruct: give up
    if (!s)
        throw_consistency_error(
            "cannot stream data: data source is not defined");

    // If we don't have it in cache, try reconstructing it from the Value
    // metadata
    if (const Value* value = get<types::Value>())
        m_data = metadata::DataManager::get().to_data(
            s->format,
            scan::Scanner::reconstruct(s->format, *this, value->buffer));
    if (m_data)
        return m_data->write(out);

    // Load it according to source
    switch (s->style())
    {
        case Source::Style::INLINE:
            throw runtime_error(
                "cannot stream data: data is not found on INLINE metadata");
        case Source::Style::URL:
            throw runtime_error(
                "cannot stream data: data is not accessible for URL metadata");
        case Source::Style::BLOB: {
            // Do not directly use m_data so that if dataReader.read throws an
            // exception, m_data remains empty.
            const source::Blob* blob = reinterpret_cast<const source::Blob*>(s);
            if (!blob->reader)
                throw runtime_error(
                    "cannot stream data: BLOB source has no reader associated");
            return blob->stream_data(out);
        }
        default:
            throw_consistency_error(
                "cannot stream data: unsupported source style");
    }
}

void Metadata::drop_cached_data()
{
    auto s = m_index.get_source();
    if (s && s->style() == Source::Style::BLOB)
        m_data.reset();
}

bool Metadata::has_cached_data() const { return (bool)m_data; }

void Metadata::set_cached_data(std::shared_ptr<metadata::Data> data)
{
    m_data = data;
}

void Metadata::makeInline()
{
    auto source = m_index.get_source();
    if (!source)
        throw_consistency_error(
            "cannot inline source in metadata: data source is not defined");
    get_data();
    set_source(Source::createInline(source->format, m_data->size()));
}

void Metadata::make_absolute()
{
    if (const source::Blob* blob = has_source_blob())
        set_source(blob->makeAbsolute());
}

void Metadata::prepare_for_segment_metadata()
{
    if (const source::Blob* blob = has_source_blob())
        set_source(blob->for_segment_metadata());
    else
        throw std::runtime_error("metadata intended for segment metadata does "
                                 "not have a blob source");
}

size_t Metadata::data_size() const
{
    if (m_data)
        return m_data->size();

    const Source* s = m_index.get_source();
    if (!s)
        return 0;

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
            throw_consistency_error(
                "cannot retrieve data: unsupported source style" +
                Source::formatStyle(s->style()));
    }
}

void Metadata::dump_internals(FILE* out) const
{
    fprintf(out, "Metadata contents:\n");
    if (m_encoded)
        fprintf(out, "  Has encoded buffer %ub long\n", m_encoded_size);
    if (m_data)
        fprintf(out, "  Has cached data %zub long\n", m_data->size());
    fprintf(out, "  Item index:\n");
    unsigned idx = 0;
    for (const auto& i : m_index)
        fprintf(out, "    %3u: %s: %s\n", idx++, i->tag().c_str(),
                i->to_string().c_str());
}

} // namespace arki
