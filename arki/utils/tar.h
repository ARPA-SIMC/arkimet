#ifndef ARKI_UTILS_TAR_H
#define ARKI_UTILS_TAR_H

#include <arki/utils/sys.h>
#include <string>
#include <vector>

namespace arki {
namespace utils {

class TarOutput
{
protected:
    sys::NamedFileDescriptor& out;

public:
    TarOutput(sys::NamedFileDescriptor& out);

    off_t append(const std::string& name, const std::string& data);
    off_t append(const std::string& name, const std::vector<uint8_t>& data);
    off_t append(const std::string& name, sys::NamedFileDescriptor& in, size_t size);
    off_t append(const std::string& name, sys::NamedFileDescriptor& in, off_t ofs, size_t size);

    void end();
};

}
}

#endif
