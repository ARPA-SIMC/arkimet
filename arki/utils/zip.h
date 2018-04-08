#ifndef ARKI_UTILS_ZIP_H
#define ARKI_UTILS_ZIP_H

#include <arki/core/fwd.h>
#include <arki/segment.h>
#include <arki/libconfig.h>
#include <string>
#include <vector>
#include <stdexcept>
#include <set>
#include <zip.h>

namespace arki {
namespace utils {

struct ZipFile;

#ifdef HAVE_LIBZIP
struct zip_error : public std::runtime_error
{
    zip_error(int code, const std::string& msg);
    zip_error(zip_t* zip, const std::string& msg);
    zip_error(zip_file_t* file, const std::string& msg);
};
#endif

class ZipReader
{
protected:
    std::string format;
    std::string zipname;
#ifdef HAVE_LIBZIP
    zip_t* zip = nullptr;

    zip_int64_t locate(const std::string& name);
    void stat(zip_int64_t index, zip_stat_t* st);
#endif

public:
    ZipReader(const std::string& format, core::NamedFileDescriptor&& fd);
    ~ZipReader();

    std::vector<segment::Span> list_data();

    /**
     * Get the data associated to the entry in the zip file called
     * <000ofs.format>
     */
    std::vector<uint8_t> get(const segment::Span& span);

    friend class ZipFile;
};

}
}

#endif
