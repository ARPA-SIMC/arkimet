#include "concat.h"
#include "arki/metadata.h"
#include "arki/nag.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <arki/wibble/sys/signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace data {
namespace concat {

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
    // md.set_source(Source::createBlob(md.source().format, "", absname, pos, buf.size()));
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

void HoleSegment::write(const std::vector<uint8_t>& buf)
{
    // Get the current file size
    off_t pos = ::lseek(fd, 0, SEEK_END);
    if (pos == (off_t)-1)
        throw wibble::exception::File(absname, "getting file size");

    // Enlarge its apparent size to include the size of buf
    int res = ftruncate(fd, pos + buf.size());
    if (res < 0)
    {
        stringstream ss;
        ss << "cannot add " << buf.size() << " bytes to the apparent file size of " << absname;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
}

FileState Segment::check(const metadata::Collection& mds, bool quick)
{
    return fd::Segment::check(mds, 0, quick);
}

static data::Segment* make_repack_segment(const std::string& relname, const std::string& absname)
{
    unique_ptr<concat::Segment> res(new concat::Segment(relname, absname));
    res->truncate_and_open();
    return res.release();
}
Pending Segment::repack(const std::string& rootdir, metadata::Collection& mds)
{
    return fd::Segment::repack(rootdir, relname, mds, make_repack_segment);
}

static data::Segment* make_repack_hole_segment(const std::string& relname, const std::string& absname)
{
    unique_ptr<concat::Segment> res(new concat::HoleSegment(relname, absname));
    res->truncate_and_open();
    return res.release();
}
Pending HoleSegment::repack(const std::string& rootdir, metadata::Collection& mds)
{
    close();
    return fd::Segment::repack(rootdir, relname, mds, make_repack_hole_segment, true);
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
    out.flush();
    return buf.size();
}

size_t OstreamWriter::stream(Metadata& md, int out) const
{
    const std::vector<uint8_t>& buf = md.getData();
    wibble::sys::sig::ProcMask pm(blocked);

    ssize_t res = ::write(out, buf.data(), buf.size());
    if (res < 0 || (unsigned)res != buf.size())
    {
        stringstream ss;
        ss << "cannot write buf.size() bytes";
        throw std::system_error(errno, std::system_category(), ss.str());
    }

    return buf.size();
}

}
}
}
}
