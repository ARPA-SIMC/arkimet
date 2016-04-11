#include "dir.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/scan/any.h"
#include "arki/utils/string.h"
#include "arki/dataset/reporter.h"
#include <cerrno>
#include <cstring>
#include <cstdint>
#include <set>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <fcntl.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace segment {
namespace dir {

namespace {

struct FdLock
{
    const std::string& pathname;
    int fd;
    struct flock lock;

    FdLock(const std::string& pathname, int fd) : pathname(pathname), fd(fd)
    {
        lock.l_type = F_WRLCK;
        lock.l_whence = SEEK_SET;
        lock.l_start = 0;
        lock.l_len = 0;
        // Use SETLKW, so that if it is already locked, we just wait
        if (fcntl(fd, F_SETLKW, &lock) == -1)
            throw_file_error(pathname, "cannot lock the file for writing");
    }

    ~FdLock()
    {
        lock.l_type = F_UNLCK;
        fcntl(fd, F_SETLK, &lock);
    }
};

struct Append : public Transaction
{
    const dir::Segment& segment;
    bool fired;
    Metadata& md;
    size_t pos;
    size_t size;

    Append(const dir::Segment& segment, Metadata& md, size_t pos, size_t size)
        : segment(segment), fired(false), md(md), pos(pos), size(size)
    {
    }

    virtual ~Append()
    {
        if (!fired) rollback();
    }

    virtual void commit()
    {
        if (fired) return;

        // Set the source information that we are writing in the metadata
        md.set_source(Source::createBlob(md.source().format, "", segment.absname, pos, size));

        fired = true;
    }

    virtual void rollback()
    {
        if (fired) return;

        // If we had a problem, remove the file that we have created. The
        // sequence will have skipped one number, but we do not need to
        // guarantee consecutive numbers, so it is just a cosmetic issue. This
        // case should be rare enough that even the cosmetic issue should
        // rarely be noticeable.
        string target_name = str::joinpath(segment.absname, SequenceFile::data_fname(pos, segment.format));
        unlink(target_name.c_str());

        fired = true;
    }
};

}

SequenceFile::SequenceFile(const std::string& dirname)
    : dirname(dirname), pathname(str::joinpath(dirname, ".sequence")), fd(-1)
{
}

SequenceFile::~SequenceFile()
{
    close();
}

void SequenceFile::open()
{
    fd = ::open(pathname.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOATIME | O_NOFOLLOW, 0666);
    if (fd == -1)
        throw_file_error(pathname, "cannot open sequence file");
}

void SequenceFile::close()
{
    if (fd != -1)
        ::close(fd);
    fd = -1;
}

std::pair<std::string, size_t> SequenceFile::next(const std::string& format)
{
    FdLock lock(pathname, fd);
    uint64_t cur;

    // Read the value in the sequence file
    ssize_t count = pread(fd, &cur, sizeof(cur), 0);
    if (count < 0)
        throw_file_error(pathname, "cannot read sequence file");

    if ((size_t)count < sizeof(cur))
        cur = 0;
    else
        ++cur;

    // Write it out
    count = pwrite(fd, &cur, sizeof(cur), 0);
    if (count < 0)
        throw_file_error(pathname, "cannot write sequence file");

    return make_pair(str::joinpath(dirname, data_fname(cur, format)), (size_t)cur);
}

File SequenceFile::open_next(const std::string& format, std::string& absname, size_t& pos)
{
    while (true)
    {
        pair<string, size_t> dest = next(format);

        File fd(dest.first);
        if (fd.open_ifexists(O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL, 0666))
        {
            absname = dest.first;
            pos = dest.second;
            return fd;
        }
    }
}

std::string SequenceFile::data_fname(size_t pos, const std::string& format)
{
    char buf[32];
    snprintf(buf, 32, "%06zd.%s", pos, format.c_str());
    return buf;
}

Segment::Segment(const std::string& format, const std::string& relname, const std::string& absname)
    : dataset::Segment(relname, absname), format(format), seqfile(absname)
{
}

Segment::~Segment()
{
}

void Segment::open()
{
    if (seqfile.fd != -1) return;

    // Ensure that the directory 'absname' exists
    sys::makedirs(absname);

    seqfile.open();
}

void Segment::close()
{
    seqfile.close();
}

size_t Segment::write_file(Metadata& md, File& fd)
{
    try {
        const std::vector<uint8_t>& buf = md.getData();

        size_t count = fd.pwrite(buf.data(), buf.size(), 0);
        if (count != buf.size())
            throw std::runtime_error(fd.name() + ": written only " + std::to_string(count) + "/" + std::to_string(buf.size()) + " bytes");

        if (fdatasync(fd) < 0)
            fd.throw_error("cannot flush write");

        return buf.size();
    } catch (...) {
        unlink(absname.c_str());
        throw;
    }
}

off_t Segment::append(Metadata& md)
{
    open();

    string dest;
    size_t pos;
    File fd = seqfile.open_next(format, dest, pos);
    /*size_t size =*/ write_file(md, fd);
    fd.close();

    // Set the source information that we are writing in the metadata
    // md.set_source(Source::createBlob(md.source().format, "", absname, pos, size));
    return pos;
}

off_t Segment::append(const std::vector<uint8_t>& buf)
{
    throw std::runtime_error("dir::Segment::append not implemented");
}

Pending Segment::append(Metadata& md, off_t* ofs)
{
    open();

    string dest;
    size_t pos;
    File fd = seqfile.open_next(format, dest, pos);
    size_t size = write_file(md, fd);
    fd.close();
    *ofs = pos;
    return new Append(*this, md, pos, size);
}

off_t Segment::link(const std::string& srcabsname)
{
    open();

    size_t pos;
    while (true)
    {
        pair<string, size_t> dest = seqfile.next(format);
        if (::link(srcabsname.c_str(), dest.first.c_str()) == 0)
        {
            pos = dest.second;
            break;
        }
        if (errno != EEXIST)
            throw_system_error("cannot link " + srcabsname + " as " + dest.first);
    }
    return pos;
}

State Segment::check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick)
{
    close();

    size_t next_sequence_expected(0);
    const scan::Validator* validator(0);
    bool out_of_order(false);
    std::string format = utils::require_format(absname);

    if (!quick)
        validator = &scan::Validator::by_filename(absname.c_str());

    // Deal with segments that just do not exist
    if (!sys::exists(absname))
        return SEGMENT_UNALIGNED;

    // List the dir elements we know about
    set<size_t> expected;
    sys::Path dir(absname);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (!i.isreg()) continue;
        if (!str::endswith(i->d_name, format)) continue;
        expected.insert((size_t)strtoul(i->d_name, 0, 10));
    }

    // Check the list of elements we expect for sort order, gaps, and match
    // with what is in the file system
    for (const auto& i: mds)
    {
        if (validator)
        {
            try {
                validate(*i, *validator);
            } catch (std::exception& e) {
                stringstream out;
                out << "validation failed at " << i->source() << ": " << e.what();
                reporter.segment_info(ds, relname, out.str());
                return SEGMENT_UNALIGNED;
            }
        }

        const source::Blob& source = i->sourceBlob();

        if (source.offset != next_sequence_expected)
            out_of_order = true;

        set<size_t>::const_iterator ei = expected.find(source.offset);
        if (ei == expected.end())
        {
            stringstream ss;
            ss << "expected file " << source.offset << " not found in the file system";
            reporter.segment_info(ds, relname, ss.str());
            return SEGMENT_UNALIGNED;
        } else
            expected.erase(ei);

        ++next_sequence_expected;
    }

    if (!expected.empty())
    {
        stringstream ss;
        ss << "found " << expected.size() << " file(s) that the index does now know about";
        reporter.segment_info(ds, relname, ss.str());
        return SEGMENT_DIRTY;
    }

    // Take note of files with holes
    if (out_of_order)
    {
        reporter.segment_info(ds, relname, "contains deleted data or data to be reordered");
        return SEGMENT_DIRTY;
    } else {
        return SEGMENT_OK;
    }
}

void Segment::validate(Metadata& md, const scan::Validator& v)
{
    if (const types::source::Blob* blob = md.has_source_blob()) {
        if (blob->filename != relname)
            throw std::runtime_error("metadata to validate does not appear to be from this segment");

        string fname = str::joinpath(absname, seqfile.data_fname(blob->offset, blob->format));
        sys::File fd(fname, O_RDONLY);
        v.validate_file(fd, blob->offset, blob->size);
        return;
    }
    const auto& buf = md.getData();
    v.validate_buf(buf.data(), buf.size());
}

void Segment::foreach_datafile(std::function<void(const char*)> f)
{
    sys::Path dir(absname);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (!i.isreg()) continue;
        if (strcmp(i->d_name, ".sequence") != 0 && !str::endswith(i->d_name, format)) continue;
        f(i->d_name);
    }
}

size_t Segment::remove()
{
    close();

    std::string format = utils::require_format(absname);

    size_t size = 0;
    foreach_datafile([&](const char* name) {
        string pathname = str::joinpath(absname, name);
        size += sys::size(pathname);
        sys::unlink(pathname);
    });
    // Also remove the directory if it is empty
    rmdir(absname.c_str());
    return size;
}

void Segment::truncate(size_t offset)
{
    close();

    utils::files::PreserveFileTimes pft(absname);

    // Truncate dir segment
    string format = utils::require_format(absname);
    foreach_datafile([&](const char* name) {
        if (strtoul(name, 0, 10) >= offset)
        {
            //cerr << "UNLINK " << absname << " -- " << *i << endl;
            sys::unlink(str::joinpath(absname, name));
        }
    });
}

Pending Segment::repack(const std::string& rootdir, metadata::Collection& mds)
{
    close();

    struct Rename : public Transaction
    {
        std::string tmpabsname;
        std::string absname;
        std::string tmppos;
        bool fired;

        Rename(const std::string& tmpabsname, const std::string& absname)
            : tmpabsname(tmpabsname), absname(absname), tmppos(absname + ".pre-repack"), fired(false)
        {
        }

        virtual ~Rename()
        {
            if (!fired) rollback();
        }

        virtual void commit()
        {
            if (fired) return;

            // It is impossible to make this atomic, so we just try to make it as quick as possible

            // Move the old directory inside the emp dir, to free the old directory name
            if (rename(absname.c_str(), tmppos.c_str()))
                throw_system_error("cannot rename " + absname + " to " + tmppos);

            // Rename the data file to its final name
            if (rename(tmpabsname.c_str(), absname.c_str()) < 0)
                throw_system_error("cannot rename " + tmpabsname + " to " + absname
                    + " (ATTENTION: please check if you need to rename " + tmppos + " to " + absname
                    + " manually to restore the dataset as it was before the repack)");

            // Remove the old data
            sys::rmtree(tmppos);

            fired = true;
        }

        virtual void rollback()
        {
            if (fired) return;

            try
            {
                sys::rmtree(tmpabsname);
            } catch (std::exception& e) {
                nag::warning("Failed to remove %s while recovering from a failed repack: %s", tmpabsname.c_str(), e.what());
            }

            rename(tmppos.c_str(), absname.c_str());

            fired = true;
        }
    };

    string format = utils::require_format(relname);
    string absname = str::joinpath(rootdir, relname);
    string tmprelname = relname + ".repack";
    string tmpabsname = absname + ".repack";

    // Create a writer for the temp dir
    unique_ptr<dir::Segment> writer(make_segment(format, tmprelname, tmpabsname));

    // Fill the temp file with all the data in the right order
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        const source::Blob& source = (*i)->sourceBlob();

        // Make a hardlink in the target directory for the file pointed by *i
        off_t pos = writer->link(str::joinpath(source.absolutePathname(), SequenceFile::data_fname(source.offset, source.format)));

        // Update the source information in the metadata
        (*i)->set_source(Source::createBlob(source.format, rootdir, relname, pos, source.size));
    }

    // Close the temp writer
    writer.release();

    return new Rename(tmpabsname, absname);
}

unique_ptr<dir::Segment> Segment::make_segment(const std::string& format, const std::string& relname, const std::string& absname)
{
    if (sys::exists(absname)) sys::rmtree(absname);
    unique_ptr<dir::Segment> res(new dir::Segment(format, relname, absname));
    return res;
}

State HoleSegment::check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick)
{
    // Force quick, since the file contents are fake
    return Segment::check(reporter, ds, mds, true);
}

unique_ptr<dir::Segment> HoleSegment::make_segment(const std::string& format, const std::string& relname, const std::string& absname)
{
    if (sys::exists(absname)) sys::rmtree(absname);
    unique_ptr<dir::Segment> res(new dir::HoleSegment(format, relname, absname));
    return res;
}

OstreamWriter::OstreamWriter()
{
    throw std::runtime_error("dir::OstreamWriter not implemented");
}

OstreamWriter::~OstreamWriter()
{
}

size_t OstreamWriter::stream(Metadata& md, NamedFileDescriptor& out) const
{
    throw std::runtime_error("dir::OstreamWriter::stream not implemented");
}

HoleSegment::HoleSegment(const std::string& format, const std::string& relname, const std::string& absname)
    : Segment(format, relname, absname)
{
}

size_t HoleSegment::write_file(Metadata& md, File& fd)
{
    try {
        if (ftruncate(fd, md.data_size()) == -1)
            fd.throw_error("cannot set file size");

        return md.data_size();
    } catch (...) {
        unlink(absname.c_str());
        throw;
    }
}

}
}
}
}
