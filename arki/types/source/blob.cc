#include "blob.h"
#include "arki/binary.h"
#include "arki/utils/lua.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/emitter.h"
#include "arki/emitter/memory.h"
#include "arki/exceptions.h"
#include "arki/reader.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {
namespace source {

Source::Style Blob::style() const { return Source::BLOB; }

void Blob::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Source::encodeWithoutEnvelope(enc);
    enc.add_varint(filename.size());
    enc.add_raw(filename);
    enc.add_varint(offset);
    enc.add_varint(size);
}

std::ostream& Blob::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
             << format << "," << str::joinpath(basedir, filename) << ":" << offset << "+" << size
             << ")";
}

void Blob::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Source::serialiseLocal(e, f);
    e.add("b", basedir);
    e.add("file", filename);
    e.add("ofs", offset);
    e.add("sz", size);
}

std::unique_ptr<Blob> Blob::decodeMapping(const emitter::memory::Mapping& val)
{
    const arki::emitter::memory::Node& rd = val["b"];
    string basedir;
    if (rd.is_string())
        basedir = rd.get_string();

    return Blob::create_unlocked(
            val["f"].want_string("parsing blob source format"),
            basedir,
            val["file"].want_string("parsing blob source filename"),
            val["ofs"].want_int("parsing blob source offset"),
            val["sz"].want_int("parsing blob source size"));
}

const char* Blob::lua_type_name() const { return "arki.types.source.blob"; }

#ifdef HAVE_LUA
bool Blob::lua_lookup(lua_State* L, const std::string& name) const
{
    if (name == "file")
        lua_pushlstring(L, filename.data(), filename.size());
    else if (name == "offset")
        lua_pushnumber(L, offset);
    else if (name == "size")
        lua_pushnumber(L, size);
    else
        return Source::lua_lookup(L, name);
    return true;
}
#endif

int Blob::compare_local(const Source& o) const
{
    if (int res = Source::compare_local(o)) return res;
    // We should be the same kind, so upcast
    const Blob* v = dynamic_cast<const Blob*>(&o);
    if (!v)
        throw_consistency_error(
            "comparing metadata types",
            string("second element claims to be a Blob Source, but is a ") + typeid(&o).name() + " instead");

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

std::unique_ptr<Blob> Blob::create(const std::string& format, const std::string& basedir, const std::string& filename, uint64_t offset, uint64_t size)
{
    auto res = create_unlocked(format, basedir, filename, offset, size);
    res->lock();
    return res;
}

std::unique_ptr<Blob> Blob::create_unlocked(const std::string& format, const std::string& basedir, const std::string& filename, uint64_t offset, uint64_t size)
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
    string pathname = absolutePathname();
    std::unique_ptr<Blob> res = Blob::create_unlocked(format, str::dirname(pathname), str::basename(filename), offset, size);
    res->reader = reader;
    return res;
}

std::unique_ptr<Blob> Blob::makeAbsolute() const
{
    string pathname = absolutePathname();
    std::unique_ptr<Blob> res = Blob::create_unlocked(format, "", pathname, offset, size);
    res->reader = reader;
    return res;
}

std::unique_ptr<Blob> Blob::makeRelativeTo(const std::string& path) const
{
    string pathname = absolutePathname();
    if (not str::startswith(pathname, path))
        throw std::runtime_error(pathname + " is not contained inside " + path);
    size_t cut = path.size();
    while (pathname[cut] == '/')
        ++cut;
    std::unique_ptr<Blob> res = Blob::create_unlocked(format, path, pathname.substr(cut), offset, size);
    res->reader = reader;
    return res;
}

std::string Blob::absolutePathname() const
{
    if (!filename.empty() && filename[0] == '/')
        return filename;
    return str::joinpath(basedir, filename);
}

void Blob::lock()
{
    if (reader) return;
    reader = reader::Registry::get().reader(absolutePathname());
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

size_t Blob::stream_data(NamedFileDescriptor& out) const
{
    if (!reader) throw std::runtime_error("readData() called on an unlocked source");
    return reader->stream(*this, out);
}

}
}
}
