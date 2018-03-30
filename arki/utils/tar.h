#ifndef ARKI_UTILS_TAR_H
#define ARKI_UTILS_TAR_H

#include <arki/utils/sys.h>
#include <string>
#include <vector>

namespace arki {
namespace utils {

struct TarHeader
{
    char data[512];

    TarHeader();
    TarHeader(const std::string& name, mode_t mode=0644);
    void set_name(const std::string& name);
    void set_mode(mode_t mode);
    void set_uid(uid_t uid);
    void set_gid(gid_t gid);
    void set_size(size_t size);
    void set_mtime(time_t mtime);
    void compute_checksum();
};

class TarOutput
{
protected:
    sys::NamedFileDescriptor& out;
    off_t out_pos = 0;

    void _write(TarHeader& header);
    void _write(const std::string& data);
    void _write(const std::vector<uint8_t>& data);

public:
    TarOutput(sys::NamedFileDescriptor& out);

    off_t append(const std::string& name, const std::string& data);
    off_t append(const std::string& name, const std::vector<uint8_t>& data);

    void end();
};

}
}

#endif
