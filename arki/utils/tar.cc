#include "tar.h"
#include <cstring>

namespace arki {
namespace utils {

TarHeader::TarHeader()
{
    memset(data, 0, 512);
    memcpy(data + 100, "0000000\0000000000\0000000000\00000000000000\00000000000000\0        ", 56);
}

TarHeader::TarHeader(const std::string& name, mode_t mode)
    : TarHeader()
{
    set_name(name);
    set_mode(mode);
}

void TarHeader::set_name(const std::string& name)
{
    if (name.size() > 100)
        throw std::runtime_error("File name " + name + " is too long for this tar writer");

    memcpy(data, name.data(), name.size());
}

void TarHeader::set_mode(mode_t mode)
{
    snprintf(data + 100, 8, "%07o", (unsigned)mode);
}

void TarHeader::set_uid(uid_t uid)
{
    snprintf(data + 108, 8, "%07o", (unsigned)uid);
}

void TarHeader::set_gid(gid_t gid)
{
    snprintf(data + 116, 8, "%07o", (unsigned)gid);
}

void TarHeader::set_size(size_t size)
{
    if (size > 8589934592)
        throw std::runtime_error("Data size " + std::to_string(size) + " is too big for this tar writer");
    snprintf(data + 124, 12, "%11zo", size);
}

void TarHeader::set_mtime(time_t mtime)
{
    snprintf(data + 136, 12, "%11zo", (size_t)mtime);
}

void TarHeader::compute_checksum()
{
    unsigned sum = 0;
    for (unsigned i = 0; i < 512; ++i)
        sum += (unsigned char)data[i];
    snprintf(data + 148, 8, "%07o", sum);
}


TarOutput::TarOutput(sys::NamedFileDescriptor& out)
    : out(out)
{
}

void TarOutput::_write(TarHeader& header)
{
    header.compute_checksum();
    out.write_all_or_retry(header.data, 512);
    out_pos += 512;
}

void TarOutput::_write(const std::string& data)
{
    out.write_all_or_retry(data);
    out_pos += data.size();
    unsigned pad_len = 512 - (data.size() % 512);
    if (pad_len)
    {
        std::vector<uint8_t> pad(pad_len, 0);
        out.write_all_or_retry(pad);
    }
    out_pos += pad_len;
}

void TarOutput::_write(const std::vector<uint8_t>& data)
{
    out.write_all_or_retry(data);
    out_pos += data.size();
    unsigned pad_len = 512 - (data.size() % 512);
    if (pad_len)
    {
        std::vector<uint8_t> pad(pad_len, 0);
        out.write_all_or_retry(pad);
    }
    out_pos += pad_len;
}

off_t TarOutput::append(const std::string& name, const std::string& data)
{
    TarHeader header(name, 0644);;
    header.set_size(data.size());
    _write(header);
    off_t res = out_pos;
    _write(data);
    return res;
}

off_t TarOutput::append(const std::string& name, const std::vector<uint8_t>& data)
{
    TarHeader header(name, 0644);
    header.set_size(data.size());
    _write(header);
    off_t res = out_pos;
    _write(data);
    return res;
}

void TarOutput::end()
{
    std::vector<uint8_t> pad(1024, 0);
    out.write_all_or_retry(pad);
}

}
}
