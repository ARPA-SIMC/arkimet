#include "concat.h"
#include "arki/exceptions.h"
#include "arki/nag.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include <fcntl.h>
#include <unistd.h>
#include <sstream>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace segment {
namespace concat {

namespace {

struct File : public fd::File
{
    using fd::File::File;

    size_t write_data(const std::vector<uint8_t>& buf) override
    {
        // Prevent caching (ignore function result)
        //(void)posix_fadvise(df.fd, pos, buf.size(), POSIX_FADV_DONTNEED);

        // Append the data
        write_all_or_throw(buf.data(), buf.size());
        return buf.size();
    }
    void test_add_padding(size_t size) override
    {
        for (unsigned i = 0; i < size; ++i)
            write("", 1);
    }
};


struct HoleFile : public fd::File
{
    using fd::File::File;

    size_t write_data(const std::vector<uint8_t>& buf) override
    {
        // Get the current file size
        off_t size = lseek(0, SEEK_END);

        // Enlarge its apparent size to include the size of buf
        ftruncate(size + buf.size());

        return buf.size();
    }
    void test_add_padding(size_t size) override
    {
        throw std::runtime_error("HoleFile::test_add_padding not implemented");
    }
};

}

const char* Segment::type() const { return "concat"; }
bool Segment::single_file() const { return true; }
std::shared_ptr<segment::Reader> Segment::reader(std::shared_ptr<core::Lock> lock) const
{
    return make_shared<Reader>(format, root, relpath, abspath, lock);
}
std::shared_ptr<segment::Checker> Segment::checker() const
{
    return make_shared<Checker>(format, root, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::make_checker(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath)
{
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds)
{
    fd::Creator creator(rootdir, relpath, mds, std::unique_ptr<fd::File>(new File(abspath)));
    creator.create();
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
bool Segment::can_store(const std::string& format)
{
    return format == "grib" || format == "bufr";
}


const char* HoleSegment::type() const { return "hole_concat"; }
bool HoleSegment::single_file() const { return true; }
std::shared_ptr<segment::Reader> HoleSegment::reader(std::shared_ptr<core::Lock> lock) const
{
    return make_shared<Reader>(format, root, relpath, abspath, lock);
}
std::shared_ptr<segment::Checker> HoleSegment::checker() const
{
    return make_shared<HoleChecker>(format, root, relpath, abspath);
}
std::shared_ptr<segment::Checker> HoleSegment::make_checker(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath)
{
    return make_shared<HoleChecker>(format, root, relpath, abspath);
}


Reader::Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock)
    : fd::Reader(abspath, lock), m_segment(format, root, relpath, abspath)
{
}

const Segment& Reader::segment() const { return m_segment; }


Writer::Writer(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, int mode)
    : fd::Writer(open_file(abspath, O_WRONLY | O_CREAT | mode, 0666)), m_segment(format, root, relpath, abspath)
{
}

const Segment& Writer::segment() const { return m_segment; }

std::unique_ptr<fd::File> Writer::open_file(const std::string& pathname, int flags, mode_t mode)
{
    return unique_ptr<fd::File>(new File(pathname, flags, mode));
}

HoleWriter::HoleWriter(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, int mode)
    : fd::Writer(open_file(abspath, O_WRONLY | O_CREAT | mode, 0666)), m_segment(format, root, relpath, abspath)
{
}

const Segment& HoleWriter::segment() const { return m_segment; }

std::unique_ptr<fd::File> HoleWriter::open_file(const std::string& pathname, int flags, mode_t mode)
{
    return unique_ptr<fd::File>(new HoleFile(pathname, flags, mode));
}


Checker::Checker(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath)
    : m_segment(format, root, relpath, abspath)
{
}

const Segment& Checker::segment() const { return m_segment; }

std::unique_ptr<fd::File> Checker::open_file(const std::string& pathname, int flags, mode_t mode)
{
    return unique_ptr<fd::File>(new File(pathname, flags, mode));
}

std::unique_ptr<fd::File> Checker::open(const std::string& pathname)
{
    return std::unique_ptr<fd::File>(new File(pathname, O_RDWR, 0666));
}

State Checker::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    fd::CheckBackend checker(segment().abspath, segment().relpath, reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}


HoleChecker::HoleChecker(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath)
    : m_segment(format, root, relpath, abspath)
{
}

const Segment& HoleChecker::segment() const { return m_segment; }

std::unique_ptr<fd::File> HoleChecker::open_file(const std::string& pathname, int flags, mode_t mode)
{
    return unique_ptr<fd::File>(new HoleFile(pathname, flags, mode));
}

std::unique_ptr<fd::File> HoleChecker::open(const std::string& pathname)
{
    return std::unique_ptr<fd::File>(new HoleFile(pathname, O_RDWR, 0666));
}

State HoleChecker::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    fd::CheckBackend checker(segment().abspath, segment().relpath, reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

Pending HoleChecker::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    string tmpabspath = segment().abspath + ".repack";

    Pending p(new files::RenameTransaction(tmpabspath, segment().abspath));

    fd::Creator creator(rootdir, segment().relpath, mds, open_file(tmpabspath, O_WRONLY | O_CREAT | O_TRUNC, 0666));
    // Skip validation, since all data reads as zeroes
    // creator.validator = &scan::Validator::by_filename(abspath);
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

}
}
}
