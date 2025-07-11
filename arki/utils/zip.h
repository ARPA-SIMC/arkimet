#ifndef ARKI_UTILS_ZIP_H
#define ARKI_UTILS_ZIP_H

#include <arki/core/fwd.h>
#include <arki/libconfig.h>
#include <arki/segment/defs.h>
#include <arki/segment/fwd.h>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>
#ifdef HAVE_LIBZIP
#include <zip.h>
#endif

namespace arki {
namespace utils {

#ifdef HAVE_LIBZIP
class zip_error : public std::runtime_error
{
public:
    zip_error(int code, const std::string& msg);
    zip_error(zip_t* zip, const std::string& msg);
    zip_error(zip_file_t* file, const std::string& msg);
};
#endif

class ZipBase
{
public:
    DataFormat format;
    std::filesystem::path zipname;

protected:
#ifdef HAVE_LIBZIP
    zip_t* zip = nullptr;

    zip_int64_t locate(const std::string& name);
    void stat(zip_int64_t index, zip_stat_t* st);
#endif

public:
    std::vector<segment::Span> list_data();

    ZipBase(DataFormat format, const std::filesystem::path& zipname);
    ~ZipBase();

    /**
     * Get the data associated to the entry in the zip file called
     * <000ofs.format>
     */
    std::vector<uint8_t> get(const segment::Span& span);

    static std::filesystem::path data_fname(size_t pos, DataFormat format);

    friend class ZipFile;
};

class ZipReader : public ZipBase
{
public:
    ZipReader(DataFormat format, core::NamedFileDescriptor&& fd);
};

class ZipWriter : public ZipBase
{
public:
    ZipWriter(DataFormat format, const std::filesystem::path& pathname);

    /**
     * Commit changes and close the .zip file
     */
    void close();

    /**
     * Remove the file associated to the entry in the zip file called
     * <000ofs.format>
     */
    void remove(const segment::Span& span);

    /**
     * Add or overwrite data to the zip file
     */
    void write(const segment::Span& span, const std::vector<uint8_t>& data);

    /**
     * Rename a file in the zip file
     */
    void rename(const segment::Span& old_span, const segment::Span& new_span);
};

} // namespace utils
} // namespace arki

#endif
