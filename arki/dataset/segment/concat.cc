#include "concat.h"
#include "arki/exceptions.h"
#include "arki/nag.h"
#include <system_error>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sstream>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace segment {
namespace concat {

namespace {

struct File : public fd::File
{
    using fd::File::File;

    void write_data(off_t wrpos, const std::vector<uint8_t>& buf) override;
    void test_add_padding(size_t size) override;
};


struct HoleFile : public fd::File
{
    using fd::File::File;

    void write_data(off_t wrpos, const std::vector<uint8_t>& buf) override;
    void test_add_padding(size_t size) override;
};



void File::write_data(off_t wrpos, const std::vector<uint8_t>& buf)
{
    // Prevent caching (ignore function result)
    //(void)posix_fadvise(df.fd, pos, buf.size(), POSIX_FADV_DONTNEED);

    // Append the data
    lseek(wrpos);
    write_all_or_throw(buf.data(), buf.size());

    fdatasync();
}

void File::test_add_padding(size_t size)
{
    for (unsigned i = 0; i < size; ++i)
        write("", 1);
}

void HoleFile::write_data(off_t wrpos, const std::vector<uint8_t>& buf)
{
    // Get the current file size
    off_t size = lseek(0, SEEK_END);

    if (wrpos + buf.size() <= (size_t)size)
        return;

    // Enlarge its apparent size to include the size of buf
    ftruncate(wrpos + buf.size());
}

void HoleFile::test_add_padding(size_t size)
{
    throw std::runtime_error("HoleFile::test_add_padding not implemented");
}

}


Writer::Writer(const std::string& root, const std::string& relname, const std::string& absname, int mode)
    : fd::Writer(root, relname, unique_ptr<fd::File>(new File(absname, O_WRONLY | O_CREAT | mode, 0666)))
{
}

HoleWriter::HoleWriter(const std::string& root, const std::string& relname, const std::string& absname, int mode)
    : fd::Writer(root, relname, unique_ptr<fd::File>(new HoleFile(absname, O_WRONLY | O_CREAT | mode, 0666)))
{
}


void Checker::open()
{
    if (fd) return;
    fd = new File(absname, O_RDWR, 0666);
}

State Checker::check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick)
{
    return check_fd(reporter, ds, mds, 0, quick);
}

unique_ptr<fd::Writer> Checker::make_tmp_segment(const std::string& relname, const std::string& absname)
{
    return unique_ptr<fd::Writer>(new concat::Writer(root, relname, absname, O_TRUNC));
}

Pending Checker::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    return fd::Checker::repack_impl(rootdir, mds, false, test_flags);
}

void HoleChecker::open()
{
    if (fd) return;
    fd = new HoleFile(absname, O_RDWR, 0666);
}

unique_ptr<fd::Writer> HoleChecker::make_tmp_segment(const std::string& relname, const std::string& absname)
{
    return unique_ptr<fd::Writer>(new concat::HoleWriter(root, relname, absname, O_TRUNC));
}

Pending HoleChecker::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    return fd::Checker::repack_impl(rootdir, mds, true, test_flags);
}

}
}
}
}
