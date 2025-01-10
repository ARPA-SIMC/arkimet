#include "zip.h"
#include "arki/utils/sys.h"
#include "arki/segment/data.h"
#include <cctype>

using namespace std::string_literals;

namespace arki {
namespace utils {

#ifdef HAVE_LIBZIP
class ZipFile
{
public:
    zip_file_t* file = nullptr;

    ZipFile(ZipBase& zip, const std::filesystem::path& name)
    {
        file = zip_fopen(zip.zip, name.c_str(), ZIP_FL_ENC_RAW);
        if (!file)
            throw zip_error(zip.zip, zip.zipname.native() + ": cannot access entry " + name.native());
    }

    ZipFile(ZipBase& zip, zip_int64_t idx)
    {
        file = zip_fopen_index(zip.zip, idx, 0);
        if (!file)
            throw zip_error(zip.zip, zip.zipname.native() + ": cannot access entry #" + std::to_string(idx));
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

static std::string zip_code_to_error(int code)
{
    zip_error_t err;
    zip_error_init_with_code(&err, code);
    std::string res = zip_error_strerror(&err);
    zip_error_fini(&err);
    return res;
}

static std::string zip_file_to_error(zip_file_t* file)
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

ZipBase::ZipBase(DataFormat format, const std::filesystem::path& zipname)
    : format(format), zipname(zipname)
{
}

ZipBase::~ZipBase()
{
#ifdef HAVE_LIBZIP
    if (zip)
        zip_discard(zip);
#endif
}

std::filesystem::path ZipBase::data_fname(size_t pos, DataFormat format)
{
    char buf[32];
    snprintf(buf, 32, "%06zu.%s", pos, format_name(format).c_str());
    return buf;
}

#ifdef HAVE_LIBZIP
void ZipBase::stat(zip_int64_t index, zip_stat_t* st)
{
    if (zip_stat_index(zip, index, ZIP_FL_ENC_RAW, st) == -1)
        throw zip_error(zip, zipname.native() + ": cannot read information on zip entry #" + std::to_string(index));
}

zip_int64_t ZipBase::locate(const std::string& name)
{
    zip_int64_t idx = zip_name_locate(zip, name.c_str(), ZIP_FL_ENC_RAW);
    if (idx == -1)
        throw std::runtime_error(zipname.native() + ": file " + name + " not found in archive");
    return idx;
}
#endif

std::vector<segment::Span> ZipBase::list_data()
{
    std::vector<segment::Span> res;
#ifdef HAVE_LIBZIP
    zip_int64_t count = zip_get_num_entries(zip, 0);
    if (count == -1)
        throw std::runtime_error(zipname.native() + ": zip_get_num_entries called on an unopened zip file");
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

std::vector<uint8_t> ZipBase::get(const segment::Span& span)
{
#ifndef HAVE_LIBZIP
    throw std::runtime_error("cannot read .zip files: libzip was not available at compile time");
#else
    auto fname = data_fname(span.offset, format);
    auto idx = locate(fname);
    zip_stat_t st;
    stat(idx, &st);
    if (st.size != span.size)
        throw std::runtime_error(zipname.native() + ": found " + std::to_string(st.size) + "b of data when " + std::to_string(span.size) + "b were expected");
    ZipFile file(*this, fname);
    return file.read_all(span.size);
#endif
}


ZipReader::ZipReader(DataFormat format, core::NamedFileDescriptor&& fd)
    : ZipBase(format, fd.path())
{
#ifndef HAVE_LIBZIP
    throw std::runtime_error("cannot read .zip files: libzip was not available at compile time");
#else
    int err = 0;
    zip = zip_fdopen(fd, 0, &err);
    if (!zip)
    {
        fd.close();
        throw zip_error(err, "cannot open zip file "s + fd.path().native());
    }
#endif
}


ZipWriter::ZipWriter(DataFormat format, const std::filesystem::path& pathname)
    : ZipBase(format, pathname)
{
#ifndef HAVE_LIBZIP
    throw std::runtime_error("cannot read .zip files: libzip was not available at compile time");
#else
    int err = 0;
    zip = zip_open(pathname.c_str(), 0, &err);
    if (!zip)
        throw zip_error(err, "cannot open zip file " + pathname.native());
#endif
}

void ZipWriter::close()
{
#ifndef HAVE_LIBZIP
    throw std::runtime_error("cannot read .zip files: libzip was not available at compile time");
#else
    if (zip_close(zip) != 0)
        throw zip_error(zip, "cannot close file " + zipname.native());
    zip = nullptr;
#endif
}

void ZipWriter::remove(const segment::Span& span)
{
#ifndef HAVE_LIBZIP
    throw std::runtime_error("cannot write to .zip files: libzip was not available at compile time");
#else
    auto fname = data_fname(span.offset, format);
    auto idx = locate(fname);
    if (zip_delete(zip, idx) != 0)
        throw zip_error(zip, "cannot delete file " + fname.native());
#endif
}

void ZipWriter::write(const segment::Span& span, const std::vector<uint8_t>& data)
{
#ifndef HAVE_LIBZIP
    throw std::runtime_error("cannot write to .zip files: libzip was not available at compile time");
#else
    auto fname = data_fname(span.offset, format);

    zip_source_t* source = zip_source_buffer(zip, data.data(), data.size(), 0);
    if (source == nullptr)
        throw zip_error(zip, "cannot create source for data to append to zip file");

    if (zip_file_add(zip, fname.c_str(), source, ZIP_FL_OVERWRITE | ZIP_FL_ENC_UTF_8) == -1)
    {
        zip_source_free(source);
        throw zip_error(zip, "cannot add file "s + fname.native());
    }
#endif
}

void ZipWriter::rename(const segment::Span& old_span, const segment::Span& new_span)
{
#ifndef HAVE_LIBZIP
    throw std::runtime_error("cannot write to .zip files: libzip was not available at compile time");
#else
    auto old_fname = data_fname(old_span.offset, format);
    auto old_idx = locate(old_fname);
    auto new_fname = data_fname(new_span.offset, format);

    if (zip_file_rename(zip, old_idx, new_fname.c_str(), ZIP_FL_ENC_UTF_8) == -1)
        throw zip_error(zip, "cannot rename "s + old_fname.native() + " to " + new_fname.native());
#endif
}

}
}
