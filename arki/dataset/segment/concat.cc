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
