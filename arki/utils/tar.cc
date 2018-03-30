#include "tar.h"

namespace arki {
namespace utils {

TarOutput::TarOutput(sys::NamedFileDescriptor& out)
    : out(out)
{
}

off_t TarOutput::append(const std::string& name, const std::string& data)
{
    return 512;
}

off_t TarOutput::append(const std::string& name, const std::vector<uint8_t>& data)
{
    return 512;
}

off_t TarOutput::append(const std::string& name, sys::NamedFileDescriptor& in, size_t size)
{
    return 512;
}

off_t TarOutput::append(const std::string& name, sys::NamedFileDescriptor& in, off_t ofs, size_t size)
{
    return 512;
}

void TarOutput::end()
{
}

}
}
