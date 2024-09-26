#include "seqfile.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace segment {

SequenceFile::SequenceFile(const std::filesystem::path& dirname)
    : core::File(dirname / ".sequence"), dirname(dirname)
{
}

SequenceFile::~SequenceFile()
{
}

void SequenceFile::open()
{
    core::File::open(O_RDWR | O_CREAT | O_CLOEXEC | O_NOATIME | O_NOFOLLOW, 0666);
}

size_t SequenceFile::read_sequence()
{
    uint64_t cur;
    ssize_t count = pread(&cur, sizeof(cur), 0);
    if ((size_t)count < sizeof(cur))
    {
        new_file = true;
        return 0;
    } else {
        new_file = false;
        return cur;
    }
}

void SequenceFile::write_sequence(size_t val)
{
    uint64_t cur = val;
    ssize_t count = pwrite(&cur, sizeof(cur), 0);
    if (count != sizeof(cur))
        throw_runtime_error("cannot write the whole sequence file");
}

std::filesystem::path SequenceFile::data_fname(size_t pos, const std::string& format)
{
    char buf[32];
    snprintf(buf, 32, "%06zu.%s", pos, format.c_str());
    return buf;
}

}
}
