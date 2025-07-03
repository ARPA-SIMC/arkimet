#ifndef ARKI_UTILS_TAR_H
#define ARKI_UTILS_TAR_H

#include <arki/utils/sys.h>
#include <filesystem>
#include <string>
#include <vector>
#include <cstdint>

namespace arki {
namespace utils {

struct TarHeader
{
    char data[512];

    TarHeader();
    TarHeader(const std::filesystem::path& name, mode_t mode=0644);
    void set_name(const std::filesystem::path& name);
    void set_mode(mode_t mode);
    void set_uid(uid_t uid);
    void set_gid(gid_t gid);
    void set_size(size_t size);
    void set_mtime(time_t mtime);
    void set_typeflag(char flag);
    void compute_checksum();
};

struct PaxHeader
{
    std::vector<uint8_t> data;

    void append(const std::string& key, const std::string& value);
    void append(const std::string& key, const std::vector<uint8_t>& value);

    static size_t size_with_length(size_t field_size);
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

    /**
     * Append a PAX header
     */
    void append(const PaxHeader& pax);

    /**
     * Append a file with the given name and data
     */
    off_t append(const std::filesystem::path& name, const std::string& data);

    /**
     * Append a file with the given name and data
     */
    off_t append(const std::filesystem::path& name, const std::vector<uint8_t>& data);

    void end();
};

}
}

#endif
