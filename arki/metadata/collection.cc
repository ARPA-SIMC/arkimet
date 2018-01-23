#include "collection.h"
#include "arki/exceptions.h"
#include "arki/core/file.h"
#include "arki/types/source/blob.h"
#include "arki/types/reftime.h"
#include "arki/utils/compress.h"
#include "arki/binary.h"
#include "arki/utils/sys.h"
#include "arki/utils.h"
#include "arki/summary.h"
#include "arki/sort.h"
#include "arki/postprocess.h"
#include "arki/dataset.h"
#include "arki/scan/any.h"
#include <algorithm>
#include <memory>
#include <fcntl.h>
#include <arpa/inet.h>
#include <utime.h>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/report.h>
#endif

using namespace std;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::types;

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
    std::string destfname;
    sys::File out;

    AtomicWriter(const std::string& fname)
        : destfname(fname), out(fname + ".tmp", O_WRONLY | O_TRUNC | O_CREAT | O_EXCL, 0666)
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
        if (::rename(out.name().c_str(), destfname.c_str()) < 0)
            throw_system_error("cannot rename " + out.name() + " to " + destfname);
    }

    void rollback()
    {
        if (!out) return;
        out.close();
        ::unlink(out.name().c_str());
    }
};


static void compressAndWrite(const std::vector<uint8_t>& buf, NamedFileDescriptor& out)
{
    auto obuf = compress::lzo(buf.data(), buf.size());
    if (obuf.size() + 8 < buf.size())
    {
        // Write a metadata group
        vector<uint8_t> tmp;
        BinaryEncoder enc(tmp);
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

Collection::Collection() {}

Collection::Collection(const Collection& o)
{
    for (const auto& i: o.vals)
        vals.push_back(i->clone());
}

Collection& Collection::operator=(const Collection& o)
{
    if (this == &o) return *this;

    clear();
    for (const auto& i: o.vals)
        vals.push_back(i->clone());

    return *this;
}

Collection::Collection(dataset::Reader& ds, const dataset::DataQuery& q)
{
    add(ds, q);
}

Collection::Collection(dataset::Reader& ds, const std::string& q)
{
    add(ds, dataset::DataQuery(q));
}

Collection::~Collection()
{
    for (vector<Metadata*>::iterator i = vals.begin(); i != vals.end(); ++i)
        delete *i;
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

void Collection::clear()
{
    for (vector<Metadata*>::iterator i = vals.begin(); i != vals.end(); ++i)
        delete *i;
    vals.clear();
}

void Collection::pop_back()
{
    if (empty()) return;
    delete vals.back();
    vals.pop_back();
}

std::vector<std::shared_ptr<dataset::WriterBatchElement>> Collection::make_import_batch() const
{
    std::vector<std::shared_ptr<dataset::WriterBatchElement>> batch;
    for (auto& md: *this)
        batch.emplace_back(make_shared<dataset::WriterBatchElement>(*md));
    return batch;
}

metadata_dest_func Collection::inserter_func()
{
    return [=](unique_ptr<Metadata> md) { acquire(move(md)); return true; };
}

void Collection::add(dataset::Reader& ds, const dataset::DataQuery& q)
{
    ds.query_data(q, inserter_func());
}

void Collection::push_back(const Metadata& md)
{
    acquire(Metadata::create_copy(md));
}

void Collection::acquire(unique_ptr<Metadata>&& md, bool with_data)
{
    if (!with_data) md->drop_cached_data();
    vals.push_back(md.release());
}

void Collection::write_to(NamedFileDescriptor& out) const
{
    static const size_t blocksize = 256;

    vector<uint8_t> buf;
    BinaryEncoder enc(buf);
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

void Collection::read_from_file(const metadata::ReadContext& rc)
{
    Metadata::read_file(rc, inserter_func());
}

void Collection::read_from_file(const std::string& pathname)
{
    Metadata::read_file(pathname, inserter_func());
}

void Collection::read_from_file(NamedFileDescriptor& fd)
{
    Metadata::read_file(fd, inserter_func());
}

void Collection::writeAtomically(const std::string& fname) const
{
    AtomicWriter writer(fname);
    write_to(writer.out);
    writer.commit();
}

void Collection::appendTo(const std::string& fname) const
{
    sys::File out(fname, O_APPEND | O_CREAT, 0666);
    write_to(out);
    out.close();
}

std::string Collection::ensureContiguousData(const std::string& source) const
{
    // Check that the metadata completely cover the data file
    if (empty()) return std::string();

    string fname;
    off_t last_end = 0;
    for (vector<Metadata*>::const_iterator i = vals.begin(); i != vals.end(); ++i)
    {
        const source::Blob& s = (*i)->sourceBlob();
        if (s.offset != (size_t)last_end)
            throw std::runtime_error("cannot validate " + source +
                    ": metadata element points to data that does not start at the end of the previous element");
        if (i == vals.begin())
        {
            fname = s.absolutePathname();
        } else {
            if (fname != s.absolutePathname())
                throw std::runtime_error("cannot validate " + source +
                        ": metadata element points at another data file (previous: " + fname + ", this: " + s.absolutePathname() + ")");
        }
        last_end += s.size;
    }
    std::unique_ptr<struct stat> st = sys::stat(fname);
    if (st.get() == NULL)
        throw_file_error(fname, "cannot validate data described in " + source);
    if (st->st_size != last_end)
        throw std::runtime_error("validating " + source + ": metadata do not cover the entire data file " + fname);
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
    // Ensure that at the end of this method we clear vals, deallocating all
    // leftovers
    ClearOnEnd coe(vals);

    for (vector<Metadata*>::iterator i = vals.begin(); i != vals.end(); ++i)
    {
        // Move the pointer to an unique_ptr
        unique_ptr<Metadata> md(*i);
        *i = 0;
        // Pass it on to the eater
        if (!dest(move(md)))
            return false;
    }
    return true;
}

void Collection::strip_source_paths()
{
    for (vector<Metadata*>::iterator i = vals.begin(); i != vals.end(); ++i)
    {
        const source::Blob& source = (*i)->sourceBlob();
        (*i)->set_source(upcast<Source>(source.fileOnly()));
    }
}

void Collection::sort(const sort::Compare& cmp)
{
    std::sort(vals.begin(), vals.end(), sort::STLCompare(cmp));
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

bool Collection::expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& until) const
{
    for (const auto& md: *this)
    {
        auto rt = md->get<types::Reftime>();
        if (!rt) return false;
        rt->expand_date_range(begin, until);
    }
    return true;
}

void Collection::drop_cached_data()
{
    for (auto& md: *this) md->drop_cached_data();
}

TestCollection::TestCollection(const std::string& pathname, bool with_data)
{
    scan_from_file(pathname, with_data);
}

bool TestCollection::scan_from_file(const std::string& pathname, bool with_data)
{
    return scan::scan(pathname, std::make_shared<core::lock::Null>(), [&](unique_ptr<Metadata> md) { acquire(move(md), with_data); return true; });
}

bool TestCollection::scan_from_file(const std::string& pathname, const std::string& format, bool with_data)
{
    return scan::scan(pathname, std::make_shared<core::lock::Null>(), format, [&](unique_ptr<Metadata> md) { acquire(move(md), with_data); return true; });
}


}
}
