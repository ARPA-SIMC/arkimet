#include "concat.h"
#include "arki/exceptions.h"
#include "arki/nag.h"
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


Writer::Writer(const std::string& root, const std::string& relpath, const std::string& abspath, int mode)
    : fd::Writer(root, relpath, open_file(abspath, O_WRONLY | O_CREAT | mode, 0666))
{
}

const char* Writer::type() const { return "concat"; }

std::unique_ptr<fd::File> Writer::open_file(const std::string& pathname, int flags, mode_t mode)
{
    return unique_ptr<fd::File>(new File(pathname, flags, mode));
}

HoleWriter::HoleWriter(const std::string& root, const std::string& relpath, const std::string& abspath, int mode)
    : fd::Writer(root, relpath, open_file(abspath, O_WRONLY | O_CREAT | mode, 0666))
{
}

const char* HoleWriter::type() const { return "hole_concat"; }

std::unique_ptr<fd::File> HoleWriter::open_file(const std::string& pathname, int flags, mode_t mode)
{
    return unique_ptr<fd::File>(new HoleFile(pathname, flags, mode));
}


const char* Checker::type() const { return "concat"; }

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
    fd::CheckBackend checker(abspath, relpath, reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}


const char* HoleChecker::type() const { return "hole_concat"; }

std::unique_ptr<fd::File> HoleChecker::open_file(const std::string& pathname, int flags, mode_t mode)
{
    return unique_ptr<fd::File>(new HoleFile(pathname, flags, mode));
}

std::unique_ptr<fd::File> HoleChecker::open(const std::string& pathname)
{
    return std::unique_ptr<fd::File>(new HoleFile(pathname, O_RDWR, 0666));
}

Pending HoleChecker::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    string tmpabspath = abspath + ".repack";

    Pending p(new files::RenameTransaction(tmpabspath, abspath));

    fd::Creator creator(rootdir, relpath, mds, open_file(tmpabspath, O_WRONLY | O_CREAT | O_TRUNC, 0666));
    // creator.validator = &scan::Validator::by_filename(abspath);
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

bool Checker::can_store(const std::string& format)
{
    return format == "grib" || format == "bufr";
}

std::shared_ptr<Checker> Checker::create(const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds)
{
    fd::Creator creator(rootdir, relpath, mds, std::unique_ptr<fd::File>(new File(abspath)));
    creator.create();
    return make_shared<Checker>(rootdir, relpath, abspath);
}

}
}
}
