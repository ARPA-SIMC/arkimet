#include "tar.h"
#include <algorithm>
#include <cstring>

namespace arki {
namespace utils {

TarHeader::TarHeader()
{
    memset(data, 0, 512);
    memcpy(data + 100, "0000000\0000000000\0000000000\00000000000000\00000000000000\0        ", 56);
}

TarHeader::TarHeader(const std::filesystem::path& name, mode_t mode)
    : TarHeader()
{
    set_name(name);
    set_mode(mode);
}

void TarHeader::set_name(const std::filesystem::path& name)
{
    if (name.native().size() > 100)
        throw std::runtime_error("File name " + name.native() + " is too long for this tar writer");

    memcpy(data, name.native().data(), name.native().size());
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
    snprintf(data + 124, 13, "%11zo", size);
}

void TarHeader::set_mtime(time_t mtime)
{
    snprintf(data + 136, 12, "%11zo", (size_t)mtime);
}

void TarHeader::set_typeflag(char flag)
{
    data[156] = flag;
}

void TarHeader::compute_checksum()
{
    unsigned sum = 0;
    for (unsigned i = 0; i < 512; ++i)
        sum += (unsigned char)data[i];
    snprintf(data + 148, 8, "%07o", sum);
}


size_t PaxHeader::size_with_length(size_t field_size)
{
    size_t e10 = 10;
    for (unsigned i = 1; i < 12; ++i, e10 *= 10)
        if (field_size < e10 - i)
        {
            field_size += i;
            break;
        }
    return field_size;
}

void PaxHeader::append(const std::string& key, const std::string& value)
{
    size_t size = size_with_length(1 + key.size() + 1 + value.size() + 1);
    std::string len = std::to_string(size);
    std::copy(len.begin(), len.end(), std::back_inserter(data));
    data.push_back(' ');
    std::copy(key.begin(), key.end(), std::back_inserter(data));
    data.push_back('=');
    std::copy(value.begin(), value.end(), std::back_inserter(data));
    data.push_back('\n');
}

void PaxHeader::append(const std::string& key, const std::vector<uint8_t>& value)
{
    std::string len = std::to_string(1 + key.size() + 1 + value.size() + 1);
    std::copy(len.begin(), len.end(), std::back_inserter(data));
    data.push_back(' ');
    std::copy(key.begin(), key.end(), std::back_inserter(data));
    data.push_back('=');
    std::copy(value.begin(), value.end(), std::back_inserter(data));
    data.push_back('\n');
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

void TarOutput::append(const PaxHeader& pax)
{
    TarHeader header("././@PaxHeader", 0);
    header.set_size(pax.data.size());
    header.set_typeflag('x');
    _write(header);
    _write(pax.data);
}

off_t TarOutput::append(const std::filesystem::path& name, const std::string& data)
{
    TarHeader header(name, 0644);
    header.set_size(data.size());
    _write(header);
    off_t res = out_pos;
    _write(data);
    return res;
}

off_t TarOutput::append(const std::filesystem::path& name, const std::vector<uint8_t>& data)
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
