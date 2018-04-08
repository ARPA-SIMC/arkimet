#include "zip.h"
#include "arki/utils/sys.h"
#include <cctype>

namespace arki {
namespace utils {

#ifdef HAVE_LIBZIP
struct ZipFile
{
    zip_file_t* file = nullptr;

    ZipFile(ZipReader& zip, const std::string& name)
    {
        file = zip_fopen(zip.zip, name.c_str(), ZIP_FL_ENC_RAW);
        if (!file)
            throw zip_error(zip.zip, zip.zipname + ": cannot access entry " + name);
    }

    ZipFile(ZipReader& zip, zip_int64_t idx)
    {
        file = zip_fopen_index(zip.zip, idx, 0);
        if (!file)
            throw zip_error(zip.zip, zip.zipname + ": cannot access entry #" + std::to_string(idx));
    }

    ~ZipFile()
    {
        if (file)
            zip_fclose(file);
        // zip_close error is ignored because we are in a destructor
    }

    std::vector<uint8_t> read_all(size_t size)
    {
        std::vector<uint8_t> res(size, 0);
        size_t ofs = 0;
        while (ofs < size)
        {
            zip_int64_t len = zip_fread(file, res.data() + ofs, res.size() - ofs);
            if (len == -1)
                throw zip_error(file, "cannot read entry data");
            ofs += len;
        }
        return res;
    }
};

std::string zip_code_to_error(int code)
{
    zip_error_t err;
    zip_error_init_with_code(&err, code);
    std::string res = zip_error_strerror(&err);
    zip_error_fini(&err);
    return res;
}

std::string zip_file_to_error(zip_file_t* file)
{
    zip_error_t* err = zip_file_get_error(file);
    std::string res = zip_error_strerror(err);
    //zip_error_fini(err);
    return res;
}

zip_error::zip_error(int code, const std::string& msg)
    : runtime_error(msg + ": " + zip_code_to_error(code))
{
}

zip_error::zip_error(zip_t* zip, const std::string& msg)
    : runtime_error(msg + ": " + zip_strerror(zip))
{
}

zip_error::zip_error(zip_file_t* file, const std::string& msg)
    : runtime_error(msg + ": " + zip_file_to_error(file))
{
}
#endif

ZipReader::ZipReader(const std::string& format, core::NamedFileDescriptor&& fd)
    : format(format), zipname(fd.name())
{
#ifndef HAVE_LIBZIP
    throw std::runtime_error("cannot read .zip files: libzip was not available at compile time");
#else
    int err;
    zip = zip_fdopen(fd, 0, &err);
    if (!err)
    {
        fd.close();
        throw zip_error(err, "cannot open zip file " + fd.name());
    }
#endif
}

ZipReader::~ZipReader()
{
#ifdef HAVE_LIBZIP
    if (zip)
        zip_discard(zip);
#endif
}

std::string ZipReader::data_fname(size_t pos, const std::string& format)
{
    char buf[32];
    snprintf(buf, 32, "%06zd.%s", pos, format.c_str());
    return buf;
}

#ifdef HAVE_LIBZIP
void ZipReader::stat(zip_int64_t index, zip_stat_t* st)
{
    if (zip_stat_index(zip, index, ZIP_FL_ENC_RAW, st) == -1)
        throw zip_error(zip, zipname + ": cannot read information on zip entry #" + std::to_string(index));
}

zip_int64_t ZipReader::locate(const std::string& name)
{
    zip_int64_t idx = zip_name_locate(zip, name.c_str(), ZIP_FL_ENC_RAW);
    if (idx == -1)
        throw std::runtime_error(zipname + ": file " + name + " not found in archive");
    return idx;
}
#endif

std::vector<segment::Span> ZipReader::list_data()
{
    std::vector<segment::Span> res;
#ifdef HAVE_LIBZIP
    zip_int64_t count = zip_get_num_entries(zip, 0);
    if (count == -1)
        throw std::runtime_error(zipname + ": zip_get_num_entries called on an unopened zip file");
    zip_stat_t st;
    //zip_stat_init(&st);
    for (size_t i = 0; i < (size_t)count; ++i)
    {
        stat(i, &st);
        if (isdigit(st.name[0]))
            res.push_back(segment::Span((size_t)strtoull(st.name, 0, 10), st.size));
    }
#endif
    return res;
}

std::vector<uint8_t> ZipReader::get(const segment::Span& span)
{
#ifndef HAVE_LIBZIP
    throw std::runtime_error("cannot read .zip files: libzip was not available at compile time");
#else
    auto fname = data_fname(span.offset, format);
    auto idx = locate(fname);
    zip_stat_t st;
    stat(idx, &st);
    if (st.size != span.size)
        throw std::runtime_error(zipname + ": found " + std::to_string(st.size) + "b of data when " + std::to_string(span.size) + "b were expected");
    ZipFile file(*this, fname);
    return file.read_all(span.size);
#endif
}

}
}
