#ifndef ARKI_UTILS_ZIP_H
#define ARKI_UTILS_ZIP_H

#include <arki/core/fwd.h>
#include <arki/segment.h>
#include <arki/libconfig.h>
#include <string>
#include <vector>
#include <stdexcept>
#include <set>
#ifdef HAVE_LIBZIP
#include <zip.h>
#endif

namespace arki {
namespace utils {

class ZipFile;

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
    std::string format;
    std::string zipname;

protected:
#ifdef HAVE_LIBZIP
    zip_t* zip = nullptr;

    zip_int64_t locate(const std::string& name);
    void stat(zip_int64_t index, zip_stat_t* st);
#endif

public:
    std::vector<segment::Span> list_data();

    ZipBase(const std::string& format, const std::string& zipname);
    ~ZipBase();

    /**
     * Get the data associated to the entry in the zip file called
     * <000ofs.format>
     */
    std::vector<uint8_t> get(const segment::Span& span);

    static std::string data_fname(size_t pos, const std::string& format);

    friend class ZipFile;
};

class ZipReader : public ZipBase
{
public:
    ZipReader(const std::string& format, core::NamedFileDescriptor&& fd);
};

class ZipWriter : public ZipBase
{
public:
    ZipWriter(const std::string& format, const std::string& pathname);

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

}
}

#endif
