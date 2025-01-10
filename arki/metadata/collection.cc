#include "collection.h"
#include "arki/exceptions.h"
#include "arki/core/file.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include "arki/types/reftime.h"
#include "arki/utils/compress.h"
#include "arki/utils/files.h"
#include "arki/core/binary.h"
#include "arki/segment/data.h"
#include "arki/scan.h"
#include "arki/utils/sys.h"
#include "arki/stream.h"
#include "arki/summary.h"
#include "arki/metadata/sort.h"
#include "arki/dataset.h"
#include <algorithm>
#include <memory>
#include <fcntl.h>

using namespace std;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::types;

namespace {

std::filesystem::path path_tmp(const std::filesystem::path& path)
{
    auto res(path);
    res += ".tmp";
    return res;
}

}


namespace arki {
namespace metadata {

/**
 * Write metadata to a file, atomically.
 *
 * The file will be created with a temporary name, and then renamed to its
 * final name.
 *
 * Note: the temporary file name will NOT be created securely.
 */
struct AtomicWriter
{
    std::filesystem::path destfname;
    sys::File out;

    AtomicWriter(const std::filesystem::path& fname)
        : destfname(fname), out(path_tmp(fname), O_WRONLY | O_TRUNC | O_CREAT | O_EXCL, 0666)
    {
    }

    ~AtomicWriter()
    {
        rollback();
    }

    void commit()
    {
        if (!out) return;
        out.close();
        std::filesystem::rename(out.path(), destfname);
    }

    void rollback()
    {
        if (!out) return;
        out.close();
        ::unlink(out.path().c_str());
    }
};


static void compressAndWrite(const std::vector<uint8_t>& buf, NamedFileDescriptor& out)
{
    auto obuf = compress::lzo(buf.data(), buf.size());
    if (obuf.size() + 8 < buf.size())
    {
        // Write a metadata group
        vector<uint8_t> tmp;
        core::BinaryEncoder enc(tmp);
        enc.add_string("MG");
        enc.add_unsigned(0u, 2);	// Version 0: LZO compressed
        enc.add_unsigned(obuf.size() + 4, 4); // Compressed len
        enc.add_unsigned(buf.size(), 4); // Uncompressed len
        out.write(tmp.data(), tmp.size());
        out.write((const char*)obuf.data(), obuf.size());
    } else
        // Write the plain metadata
        out.write(buf.data(), buf.size());
}

static stream::SendResult compressAndWrite(const std::vector<uint8_t>& buf, StreamOutput& out)
{
    auto obuf = compress::lzo(buf.data(), buf.size());
    if (obuf.size() + 8 < buf.size())
    {
        // Write a metadata group
        vector<uint8_t> tmp;
        core::BinaryEncoder enc(tmp);
        enc.add_string("MG");
        enc.add_unsigned(0u, 2);	// Version 0: LZO compressed
        enc.add_unsigned(obuf.size() + 4, 4); // Compressed len
        enc.add_unsigned(buf.size(), 4); // Uncompressed len
        auto res = out.send_buffer(tmp.data(), tmp.size());
        res += out.send_buffer((const char*)obuf.data(), obuf.size());
        return res;
    } else
        // Write the plain metadata
        return out.send_buffer(buf.data(), buf.size());
}

Collection::Collection(dataset::Dataset& ds, const query::Data& q)
{
    add(ds, q);
}

Collection::Collection(dataset::Dataset& ds, const std::string& q)
{
    add(ds, q);
}

Collection::Collection(dataset::Reader& ds, const query::Data& q)
{
    add(ds, q);
}

Collection::Collection(dataset::Reader& ds, const std::string& q)
{
    add(ds, q);
}

Collection::~Collection()
{
}

bool Collection::operator==(const Collection& o) const
{
    if (vals.size() != o.vals.size()) return false;
    auto i = vals.begin();
    auto oi = o.vals.begin();
    for ( ; i != vals.end() && oi != o.vals.end(); ++i, ++oi)
        if (**i != **oi) return false;
    return true;
}

Collection Collection::clone() const
{
    Collection res;
    res.vals.reserve(size());
    for (const auto& md: vals)
        res.vals.push_back(std::shared_ptr<Metadata>(md->clone()));
    return res;
}

dataset::WriterBatch Collection::make_import_batch() const
{
    dataset::WriterBatch batch;
    for (auto& md: vals)
        batch.emplace_back(make_shared<dataset::WriterBatchElement>(*md));
    return batch;
}

metadata_dest_func Collection::inserter_func()
{
    return [=](std::shared_ptr<Metadata> md) { acquire(md); return true; };
}

void Collection::add(dataset::Dataset& ds, const query::Data& q)
{
    ds.create_reader()->query_data(q, inserter_func());
}

void Collection::add(dataset::Dataset& ds, const std::string& q)
{
    ds.create_reader()->query_data(q, inserter_func());
}

void Collection::add(dataset::Reader& reader, const query::Data& q)
{
    reader.query_data(q, inserter_func());
}

void Collection::add(dataset::Reader& reader, const std::string& q)
{
    reader.query_data(q, inserter_func());
}

void Collection::push_back(const Metadata& md)
{
    acquire(md.clone());
}

void Collection::acquire(std::shared_ptr<Metadata> md, bool with_data)
{
    if (!with_data) md->drop_cached_data();
    vals.push_back(md);
}

void Collection::write_to(NamedFileDescriptor& out) const
{
    static const size_t blocksize = 256;

    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    for (size_t i = 0; i < vals.size(); ++i)
    {
        if (i > 0 && (i % blocksize) == 0)
        {
            compressAndWrite(buf, out);
            buf.clear();
        }
        vals[i]->encodeBinary(enc);
    }
    if (!buf.empty())
        compressAndWrite(buf, out);
}

stream::SendResult Collection::write_to(StreamOutput& out) const
{
    static const size_t blocksize = 256;

    stream::SendResult res;
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    for (size_t i = 0; i < vals.size(); ++i)
    {
        if (i > 0 && (i % blocksize) == 0)
        {
            res += compressAndWrite(buf, out);
            buf.clear();
        }
        vals[i]->encodeBinary(enc);
    }
    if (!buf.empty())
        res += compressAndWrite(buf, out);
    return res;
}

void Collection::read_from_file(const metadata::ReadContext& rc)
{
    Metadata::read_file(rc, inserter_func());
}

void Collection::read_from_file(const std::filesystem::path& pathname)
{
    Metadata::read_file(pathname, inserter_func());
}

void Collection::read_from_file(NamedFileDescriptor& fd)
{
    Metadata::read_file(fd, inserter_func());
}

void Collection::writeAtomically(const std::filesystem::path& fname) const
{
    AtomicWriter writer(fname);
    write_to(writer.out);
    writer.commit();
}

void Collection::appendTo(const std::filesystem::path& fname) const
{
    sys::File out(fname, O_APPEND | O_CREAT, 0666);
    write_to(out);
    out.close();
}

std::filesystem::path Collection::ensureContiguousData(const std::string& source) const
{
    // Check that the metadata completely cover the data file
    if (empty()) return std::string();

    std::filesystem::path fname;
    off_t last_end = 0;
    for (auto i = vals.begin(); i != vals.end(); ++i)
    {
        const source::Blob& s = (*i)->sourceBlob();
        if (s.offset != (size_t)last_end)
            throw std::runtime_error("cannot validate "s + source +
                    ": metadata element points to data that does not start at the end of the previous element");
        if (i == vals.begin())
        {
            fname = s.absolutePathname();
        } else {
            if (fname != s.absolutePathname())
                throw std::runtime_error("cannot validate "s + source +
                        ": metadata element points at another data file (previous: " + fname.native() + ", this: " + s.absolutePathname().native() + ")");
        }
        last_end += s.size;
    }
    std::unique_ptr<struct stat> st = sys::stat(fname);
    if (st.get() == NULL)
        throw_file_error(fname, "cannot validate data described in " + source);
    if (st->st_size != last_end)
        throw std::runtime_error("validating " + source + ": metadata do not cover the entire data file " + fname.native());
    return fname;
}

void Collection::add_to_summary(Summary& out) const
{
    for (const_iterator i = vals.begin(); i != vals.end(); ++i)
        out.add(**i);
}

namespace {
struct ClearOnEnd
{
    vector<Metadata*>& vals;
    ClearOnEnd(vector<Metadata*>& vals) : vals(vals) {}
    ~ClearOnEnd()
    {
        for (vector<Metadata*>::iterator i = vals.begin(); i != vals.end(); ++i)
            delete *i;
        vals.clear();
    }
};
}

bool Collection::move_to(metadata_dest_func dest)
{
    bool success = true;
    for (auto& md: vals)
    {
        if (success && !dest(md))
            success = false;
        // Release as soon as we passed it on
        md.reset();
    }
    clear();
    return success;
}

void Collection::strip_source_paths()
{
    for (auto i = vals.begin(); i != vals.end(); ++i)
    {
        const source::Blob& source = (*i)->sourceBlob();
        (*i)->set_source(upcast<Source>(source.fileOnly()));
    }
}

void Collection::sort(const sort::Compare& cmp)
{
    std::stable_sort(vals.begin(), vals.end(), sort::STLCompare(cmp));
}

void Collection::sort(const std::string& cmp)
{
    unique_ptr<sort::Compare> c = sort::Compare::parse(cmp);
    sort(*c);
}

void Collection::sort()
{
    sort("");
}

bool Collection::expand_date_range(core::Interval& interval) const
{
    for (const auto& md: *this)
    {
        auto rt = md->get<types::Reftime>();
        if (!rt) return false;
        rt->expand_date_range(interval);
    }
    return true;
}

void Collection::drop_cached_data()
{
    for (auto& md: *this) md->drop_cached_data();
}

TestCollection::TestCollection(const std::filesystem::path& pathname, bool with_data)
{
    scan_from_file(pathname, with_data);
}

void TestCollection::scan_from_file(const std::filesystem::path& pathname, bool with_data)
{
    auto segment = Segment::from_isolated_file(pathname);
    auto reader = segment->detect_data_reader(std::make_shared<core::lock::Null>());
    reader->scan([&](std::shared_ptr<Metadata> md) { acquire(md, with_data); return true; });
}

void TestCollection::scan_from_file(const std::filesystem::path& pathname, DataFormat format, bool with_data)
{
    auto segment = Segment::from_isolated_file(pathname, format);
    auto reader = segment->detect_data_reader(std::make_shared<core::lock::Null>());
    reader->scan([&](std::shared_ptr<Metadata> md) { acquire(md, with_data); return true; });
}


}
}
