#include "lines.h"
#include "arki/metadata.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <arki/wibble/sys/signal.h>
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
namespace data {
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
        throw wibble::exception::File(absname, "flushing write");
}

void Segment::append(Metadata& md)
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
    md.set_source(Source::createBlob(md.source().format, "", absname, pos, buf.size()));
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

FileState Segment::check(const metadata::Collection& mds, bool quick)
{
    return fd::Segment::check(mds, 2, quick);
}

static data::Segment* make_repack_segment(const std::string& relname, const std::string& absname)
{
    unique_ptr<lines::Segment> res(new lines::Segment(relname, absname));
    res->truncate_and_open();
    return res.release();
}

Pending Segment::repack(const std::string& rootdir, metadata::Collection& mds)
{
    close();
    return fd::Segment::repack(rootdir, relname, mds, make_repack_segment);
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

size_t OstreamWriter::stream(Metadata& md, std::ostream& out) const
{
    const std::vector<uint8_t>& buf = md.getData();
    wibble::sys::sig::ProcMask pm(blocked);
    out.write((const char*)buf.data(), buf.size());
    // Cannot use endl since we don't know how long it is, and we would risk
    // returning the wrong number of bytes written
    out << "\n";
    out.flush();
    return buf.size() + 1;
}

size_t OstreamWriter::stream(Metadata& md, int out) const
{
    const std::vector<uint8_t>& buf = md.getData();
    wibble::sys::sig::ProcMask pm(blocked);

    ssize_t res = ::write(out, buf.data(), buf.size());
    if (res < 0 || (unsigned)res != buf.size())
    {
        stringstream ss;
        ss << "cannot write " << buf.size() << " bytes";
        throw std::system_error(errno, std::system_category(), ss.str());
    }

    res = ::write(out, "\n", 1);
    if (res < 0 || (unsigned)res != 1)
        throw wibble::exception::System("writing newline");

    return buf.size() + 1;
}


}
}
}
}
