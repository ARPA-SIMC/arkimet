#include "seqfile.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace {

struct FdLock
{
    sys::NamedFileDescriptor& fd;
    std::shared_ptr<lock::Policy> lock_policy;
    Lock lock;

    FdLock(sys::NamedFileDescriptor& fd, std::shared_ptr<lock::Policy> lock_policy) : fd(fd), lock_policy(lock_policy)
    {
        lock.l_type = F_WRLCK;
        lock.l_whence = SEEK_SET;
        lock.l_start = 0;
        lock.l_len = 0;
        // Use SETLKW, so that if it is already locked, we just wait
        lock_policy->setlkw(fd, lock);
    }

    ~FdLock()
    {
        lock.l_type = F_UNLCK;
        lock_policy->setlk(fd, lock);
    }
};

}


namespace arki {
namespace dataset {
namespace segment {

SequenceFile::SequenceFile(const std::string& dirname, std::shared_ptr<core::lock::Policy> lock_policy)
    : dirname(dirname), fd(str::joinpath(dirname, ".sequence")), lock_policy(lock_policy)
{
}

SequenceFile::~SequenceFile()
{
}

void SequenceFile::open()
{
    fd.open(O_RDWR | O_CREAT | O_CLOEXEC | O_NOATIME | O_NOFOLLOW, 0666);
}

void SequenceFile::close()
{
    fd.close();
}

void SequenceFile::test_add_padding(unsigned size)
{
    FdLock lock(fd, lock_policy);
    uint64_t cur;

    // Read the value in the sequence file
    ssize_t count = fd.pread(&cur, sizeof(cur), 0);
    if ((size_t)count < sizeof(cur))
        cur = size;
    else
        cur += size;

    // Write it out
    count = fd.pwrite(&cur, sizeof(cur), 0);
    if (count != sizeof(cur))
        fd.throw_runtime_error("cannot write the whole sequence file");
}

std::pair<std::string, size_t> SequenceFile::next(const std::string& format)
{
    FdLock lock(fd, lock_policy);
    uint64_t cur;

    // Read the value in the sequence file
    ssize_t count = fd.pread(&cur, sizeof(cur), 0);
    if ((size_t)count < sizeof(cur))
        cur = 0;
    else
        ++cur;

    // Write it out
    count = fd.pwrite(&cur, sizeof(cur), 0);
    if (count != sizeof(cur))
        fd.throw_runtime_error("cannot write the whole sequence file");

    return make_pair(str::joinpath(dirname, data_fname(cur, format)), (size_t)cur);
}

File SequenceFile::open_next(const std::string& format, size_t& pos)
{
    while (true)
    {
        pair<string, size_t> dest = next(format);

        File fd(dest.first);
        if (fd.open_ifexists(O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL, 0666))
        {
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

}
}
}
