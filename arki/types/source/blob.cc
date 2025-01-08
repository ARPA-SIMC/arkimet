#include "blob.h"
#include "arki/segment/data.h"
#include "arki/core/binary.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/structured/emitter.h"
#include "arki/structured/keys.h"
#include "arki/structured/reader.h"
#include "arki/exceptions.h"

using namespace std;
using namespace arki::utils;
using namespace arki::core;

namespace arki {
namespace types {
namespace source {

Source::Style Blob::style() const { return source::Style::BLOB; }

void Blob::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Source::encodeWithoutEnvelope(enc);
    enc.add_varint(filename.native().size());
    enc.add_raw(filename.native());
    enc.add_varint(offset);
    enc.add_varint(size);
}

std::ostream& Blob::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
             << format << "," << (basedir / filename).native() << ":" << offset << "+" << size
             << ")";
}

void Blob::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Source::serialise_local(e, keys, f);
    e.add(keys.source_basedir, basedir);
    e.add(keys.source_file, filename);
    e.add(keys.source_offset, offset);
    e.add(keys.source_size, size);
}

std::unique_ptr<Blob> Blob::decode_structure(const structured::Keys& keys, const structured::Reader& reader)
{
    std::filesystem::path basedir;
    if (reader.has_key(keys.source_basedir, structured::NodeType::STRING))
        basedir = reader.as_string(keys.source_basedir, "source base directory");

    return Blob::create_unlocked(
            reader.as_string(keys.source_format, "source format"),
            basedir,
            reader.as_string(keys.source_file, "source file name"),
            reader.as_int(keys.source_offset, "source offset"),
            reader.as_int(keys.source_size, "source size"));
}

int Blob::compare_local(const Source& o) const
{
    if (int res = Source::compare_local(o)) return res;
    // We should be the same kind, so upcast
    const Blob* v = dynamic_cast<const Blob*>(&o);
    if (!v)
        throw_consistency_error(
            "comparing metadata types",
            "second element claims to be a Blob Source, but is a "s + typeid(&o).name() + " instead");

    if (filename < v->filename) return -1;
    if (filename > v->filename) return 1;
    if (int res = offset - v->offset) return res;
    return size - v->size;
}

bool Blob::equals(const Type& o) const
{
    const Blob* v = dynamic_cast<const Blob*>(&o);
    if (!v) return false;
    return format == v->format && filename == v->filename && offset == v->offset && size == v->size;
}

Blob* Blob::clone() const
{
    return new Blob(*this);
}

std::unique_ptr<Blob> Blob::create(std::shared_ptr<segment::data::Reader> reader, uint64_t offset, uint64_t size)
{
    auto res = create_unlocked(reader->segment().format, reader->segment().root, reader->segment().relpath, offset, size);
    res->lock(reader);
    return res;
}

std::unique_ptr<Blob> Blob::create(const std::string& format, const std::filesystem::path& basedir, const std::filesystem::path& filename, uint64_t offset, uint64_t size, std::shared_ptr<segment::data::Reader> reader)
{
    auto res = create_unlocked(format, basedir, filename, offset, size);
    res->lock(reader);
    return res;
}

std::unique_ptr<Blob> Blob::create_unlocked(const std::string& format, const std::filesystem::path& basedir, const std::filesystem::path& filename, uint64_t offset, uint64_t size)
{
    unique_ptr<Blob> res(new Blob);
    res->format = format;
    res->basedir = basedir;
    res->filename = filename;
    res->offset = offset;
    res->size = size;
    return res;
}


std::unique_ptr<Blob> Blob::fileOnly() const
{
    std::filesystem::path pathname = absolutePathname();
    std::unique_ptr<Blob> res = Blob::create_unlocked(format, pathname.parent_path(), pathname.filename(), offset, size);
    res->reader = reader;
    return res;
}

std::unique_ptr<Blob> Blob::makeAbsolute() const
{
    std::filesystem::path pathname = absolutePathname();
    std::unique_ptr<Blob> res = Blob::create_unlocked(format, "", pathname, offset, size);
    res->reader = reader;
    return res;
}

std::unique_ptr<Blob> Blob::makeRelativeTo(const std::filesystem::path& path) const
{
    auto pathname = absolutePathname();
    auto relpath = pathname.lexically_relative(path);
    if (!relpath.empty() && *relpath.begin() == "..")
        throw std::runtime_error(pathname.native() + " is not contained inside " + path.native());
    std::unique_ptr<Blob> res = Blob::create_unlocked(format, path, relpath, offset, size);
    res->reader = reader;
    return res;
}

std::filesystem::path Blob::absolutePathname() const
{
    if (!filename.empty() && filename.is_absolute())
        return filename;
    return basedir / filename;
}

void Blob::lock(std::shared_ptr<segment::data::Reader> reader)
{
    this->reader = reader;
}

void Blob::unlock()
{
    reader.reset();
}

vector<uint8_t> Blob::read_data(NamedFileDescriptor& fd, bool rlock) const
{
    if (rlock)
        throw std::runtime_error("cannot retrieve data: read locking in this method is not yet implemented");
    vector<uint8_t> buf;
    buf.resize(size);
    if (fd.pread(buf.data(), size, offset) != size)
        throw runtime_error("cannot retrieve data: only partial data has been read");
    return buf;
}

std::vector<uint8_t> Blob::read_data() const
{
    if (!reader) throw std::runtime_error("readData() called on an unlocked source");
    return reader->read(*this);
}

stream::SendResult Blob::stream_data(StreamOutput& out) const
{
    if (!reader) throw std::runtime_error("readData() called on an unlocked source");
    return reader->stream(*this, out);
}

}
}
}
