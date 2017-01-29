#include "lines.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <arki/wibble/sys/signal.h>
#include <system_error>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace segment {
namespace lines {

namespace {

struct Append : public Transaction
{
    Segment& w;
    Metadata& md;
    bool fired = false;
    const std::vector<uint8_t>& buf;
    off_t pos;

    Append(Segment& w, Metadata& md) : w(w), md(md), buf(md.getData())
    {
        // Lock the file so that we are the only ones writing to it
        w.lock();

        // Insertion offset
        pos = w.wrpos();
    }

    virtual ~Append()
    {
        if (!fired) rollback();
    }

    virtual void commit()
    {
        if (fired) return;

        // Append the data
        w.write(buf);

        w.unlock();

        // Set the source information that we are writing in the metadata
        md.set_source(Source::createBlob(md.source().format, "", w.absname, pos, buf.size()));

        fired = true;
    }

    virtual void rollback()
    {
        if (fired) return;

        // If we had a problem, attempt to truncate the file to the original size
        w.fdtruncate(pos);

        w.unlock();
        fired = true;
    }
};

}



Segment::Segment(const std::string& relname, const std::string& absname)
    : fd::Segment(relname, absname)
{
}

void Segment::test_add_padding(unsigned size)
{
    open();
    for (unsigned i = 0; i < size; ++i)
        fd.write("\n", 1);
}

void Segment::write(const std::vector<uint8_t>& buf)
{
    struct iovec todo[2] = {
        { (void*)buf.data(), buf.size() },
        { (void*)"\n", 1 },
    };

    // Prevent caching (ignore function result)
    //(void)posix_fadvise(df.fd, pos, buf.size(), POSIX_FADV_DONTNEED);

    // Append the data
    ssize_t res = ::writev(fd, todo, 2);
    if (res < 0 || (unsigned)res != buf.size() + 1)
    {
        stringstream ss;
        ss << "cannot write " << (buf.size() + 1) << " bytes to " << absname;
        throw std::system_error(errno, std::system_category(), ss.str());
    }

    if (fdatasync(fd) < 0)
        throw_file_error(absname, "cannot flush write");
}

off_t Segment::append(Metadata& md)
{
    open();

    // Get the data blob to append
    const std::vector<uint8_t>& buf = md.getData();

    // Lock the file so that we are the only ones writing to it
    lock();

    // Get the write position in the data file
    off_t pos = wrpos();

    try {
        // Append the data
        write(buf);
    } catch (...) {
        // If we had a problem, attempt to truncate the file to the original size
        fdtruncate(pos);

        unlock();

        throw;
    }

    unlock();

    // Set the source information that we are writing in the metadata
    //md.set_source(Source::createBlob(md.source().format, "", absname, pos, buf.size()));
    return pos;
}

off_t Segment::append(const std::vector<uint8_t>& buf)
{
    open();

    off_t pos = wrpos();
    write(buf);
    return pos;
}

Pending Segment::append(Metadata& md, off_t* ofs)
{
    open();

    Append* res = new Append(*this, md);
    *ofs = res->pos;
    return res;
}

State Segment::check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick)
{
    return check_fd(reporter, ds, mds, 2, quick);
}

static fd::Segment* make_repack_segment(const std::string& relname, const std::string& absname)
{
    unique_ptr<lines::Segment> res(new lines::Segment(relname, absname));
    res->truncate_and_open();
    return res.release();
}

Pending Segment::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    close();
    return fd::Segment::repack(rootdir, relname, mds, make_repack_segment, false, test_flags);
}

OstreamWriter::OstreamWriter()
{
    sigemptyset(&blocked);
    sigaddset(&blocked, SIGINT);
    sigaddset(&blocked, SIGTERM);
}

OstreamWriter::~OstreamWriter()
{
}

size_t OstreamWriter::stream(Metadata& md, NamedFileDescriptor& out) const
{
    const std::vector<uint8_t>& buf = md.getData();
    struct iovec todo[2] = {
        { (void*)buf.data(), buf.size() },
        { (void*)"\n", 1 },
    };
    ssize_t res = ::writev(out, todo, 2);
    if (res < 0 || (unsigned)res != buf.size() + 1)
        throw_system_error("cannot write " + to_string(buf.size() + 1) + " bytes to " + out.name());
    return buf.size() + 1;
}

}
}
}
}
