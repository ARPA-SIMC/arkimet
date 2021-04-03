#include "arki/metadata/archive.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include <arki/metadata/collection.h>
#include "arki/core/binary.h"
#include "arki/stream.h"
#include "arki/exceptions.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/types/source.h"
#include "arki/types/reftime.h"
#include "arki/libconfig.h"
#include <cstring>
#ifdef HAVE_LIBARCHIVE
#include <archive.h>
#include <archive_entry.h>
#endif

using namespace arki::utils;

namespace arki {
namespace metadata {

#ifdef HAVE_LIBARCHIVE

struct archive_runtime_error: public std::runtime_error
{
    archive_runtime_error(struct ::archive* a, const std::string& msg)
        : std::runtime_error(msg + ": " + archive_error_string(a))
    {
    }
};

/**
 * Output metadata and data using one of the archive formats supported by libarchive
 */
class LibarchiveOutput : public ArchiveOutput
{
protected:
    struct ::archive* a = nullptr;
    struct ::archive_entry* entry = nullptr;
    Collection mds;
    char filename_buf[255];

    void write_buffer(const std::vector<uint8_t>& buf);
    void append_metadata();

public:
    std::string format;
    std::string subdir;

    LibarchiveOutput(const std::string& format);
    ~LibarchiveOutput()
    {
        archive_entry_free(entry);
        archive_write_free(a);
    }

    void set_subdir(const std::string& subdir) override { this->subdir = subdir; }
    size_t append(const Metadata& md) override;
    void flush(bool with_metadata) override;
};

LibarchiveOutput::LibarchiveOutput(const std::string& format)
    : format(format), subdir("data")
{
    a = archive_write_new();
    if (a == nullptr)
        throw_system_error("archive_write_new failed");
    entry = archive_entry_new();
    if (entry == nullptr)
        throw_system_error("archive_entry_new failed");

    if (format == "tar")
    {
        if (archive_write_set_format_gnutar(a) != ARCHIVE_OK)
            throw archive_runtime_error(a, "cannot set tar archive format");
    }
    else if (format == "tar.gz")
    {
        if (archive_write_set_format_gnutar(a) != ARCHIVE_OK)
            throw archive_runtime_error(a, "cannot set tar archive format");
        if (archive_write_add_filter_gzip(a) != ARCHIVE_OK)
            throw archive_runtime_error(a, "cannot add gzip compression");
    }
    else if (format == "tar.xz")
    {
        if (archive_write_set_format_gnutar(a) != ARCHIVE_OK)
            throw archive_runtime_error(a, "cannot set tar archive format");
        if (archive_write_add_filter_lzma(a) != ARCHIVE_OK)
            throw archive_runtime_error(a, "cannot add lzma compression");
    }
    else if (format == "zip")
    {
        if (archive_write_set_format_zip(a) != ARCHIVE_OK)
            throw archive_runtime_error(a, "cannot set zip archive format");
    }
}

void LibarchiveOutput::write_buffer(const std::vector<uint8_t>& buf)
{
    size_t ofs = 0;
    while (ofs < buf.size())
    {
        la_ssize_t written = archive_write_data(a, buf.data() + ofs, buf.size() - ofs);
        if (written < 0)
            throw archive_runtime_error(a, "cannot write entry data");
        /*
         * archive_write_data(3) says:
         * In libarchive 3.x, this function sometimes returns zero on success
         * instead of returning the number of bytes written.  Specifically,
         * this occurs when writing to an archive_write_disk handle.  Clients
         * should treat any value less than zero as an error and consider any
         * non-negative value as success.
         *
         * Assuming that 0 means "all was written" and breaking out of the
         * loop, although we are not using archive_write_disk, so it should
         * never happen.
         */
        if (written == 0)
            break;
        ofs += written;
    }
}

size_t LibarchiveOutput::append(const Metadata& md)
{
    size_t ofs = mds.size() + 1;
    if (subdir.empty())
        snprintf(filename_buf, 255, "%06zd.%s", ofs, md.source().format.c_str());
    else
        snprintf(filename_buf, 255, "%s/%06zd.%s", subdir.c_str(), ofs, md.source().format.c_str());
    auto stored_md = md.clone();
    const auto& stored_data = stored_md->get_data().read();
    std::unique_ptr<types::Source> stored_source = types::Source::createBlobUnlocked(md.source().format, "", filename_buf, 0, stored_data.size());

    archive_entry_clear(entry);
    archive_entry_set_pathname(entry, filename_buf);
    archive_entry_set_size(entry, stored_data.size());
    archive_entry_set_filetype(entry, AE_IFREG);
    archive_entry_set_perm(entry, 0644);
    if (const auto* reftime = stored_md->get<types::reftime::Position>())
        archive_entry_set_mtime(entry, reftime->get_Position().to_unix(), 0);

    if (archive_write_header(a, entry) != ARCHIVE_OK)
        throw archive_runtime_error(a, "cannot write entry header");

    write_buffer(stored_data);

    stored_md->drop_cached_data();
    mds.acquire(std::move(stored_md));
    return ofs;
}

void LibarchiveOutput::append_metadata()
{
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    for (const auto& md: mds)
        md->encodeBinary(enc);

    std::string name;
    if (subdir.empty())
        name = "metadata.md";
    else
        name = str::joinpath(subdir, "metadata.md");
    archive_entry_clear(entry);
    archive_entry_set_pathname(entry, name.c_str());
    archive_entry_set_size(entry, buf.size());
    archive_entry_set_filetype(entry, AE_IFREG);
    archive_entry_set_perm(entry, 0644);
    archive_entry_set_mtime(entry, time(nullptr), 0);
    if (archive_write_header(a, entry) != ARCHIVE_OK)
        throw archive_runtime_error(a, "cannot write entry header");

    write_buffer(buf);
}

void LibarchiveOutput::flush(bool with_metadata)
{
    if (with_metadata)
        append_metadata();

    if (archive_write_close(a) != ARCHIVE_OK)
        throw archive_runtime_error(a, "cannot close archive");
}


class LibarchiveFileOutput : public LibarchiveOutput
{
    std::shared_ptr<core::NamedFileDescriptor> out;

public:
    LibarchiveFileOutput(const std::string& format, std::shared_ptr<core::NamedFileDescriptor> out)
        : LibarchiveOutput(format), out(out)
    {
        if (archive_write_open_fd(a, *out) != ARCHIVE_OK)
            throw archive_runtime_error(a, "archive_write_open_fd failed");
    }
};


namespace {

int archive_streamoutput_open_callback(struct archive* a, void *client_data)
{
    return ARCHIVE_OK;
}

int archive_streamoutput_close_callback(struct archive *a, void *client_data)
{
    return ARCHIVE_OK;
}

la_ssize_t archive_streamoutput_write_callback(struct archive *a, void *client_data, const void *buffer, size_t length)
{
    StreamOutput* out = reinterpret_cast<StreamOutput*>(client_data);
    try {
        auto res = out->send_buffer(buffer, length);
        return res.sent;
    } catch (std::system_error& e) {
        archive_set_error(a, e.code().value(), "%s", e.what());
        return -1;
    } catch (stream::TimedOut& e) {
        archive_set_error(a, ETIMEDOUT, "%s", e.what());
        return -1;
    } catch (std::exception& e) {
        archive_set_error(a, EIO, "%s", e.what());
        return -1;
    }
}

}

class LibarchiveStreamOutput : public LibarchiveOutput
{
    std::shared_ptr<StreamOutput> out;

public:
    LibarchiveStreamOutput(const std::string& format, std::shared_ptr<StreamOutput> out)
        : LibarchiveOutput(format), out(out)
    {
        if (archive_write_open(a, out.get(), archive_streamoutput_open_callback, archive_streamoutput_write_callback, archive_streamoutput_close_callback) != ARCHIVE_OK)
            throw archive_runtime_error(a, "archive_write_open_fd failed");
        // The default for the last block is, surprisingly, not 1, meaning that
        // unless we explicitly set it, libarchive will add padding at the end
        // of the data, which will corrupt things like xz compressed streams
        if (archive_write_set_bytes_in_last_block(a, 1) != ARCHIVE_OK)
            throw archive_runtime_error(a, "archive_write_set_bytes_in_last_block failed");
    }
};
#endif


ArchiveOutput::~ArchiveOutput()
{
}

std::unique_ptr<ArchiveOutput> ArchiveOutput::create(const std::string& format, std::shared_ptr<core::NamedFileDescriptor> out)
{
#ifdef HAVE_LIBARCHIVE
    return std::unique_ptr<LibarchiveOutput>(new LibarchiveFileOutput(format, out));
#else
    throw std::runtime_error("libarchive not supported in this build");
#endif
}

std::unique_ptr<ArchiveOutput> ArchiveOutput::create(const std::string& format, std::shared_ptr<StreamOutput> out)
{
#ifdef HAVE_LIBARCHIVE
    return std::unique_ptr<LibarchiveOutput>(new LibarchiveStreamOutput(format, out));
#else
    throw std::runtime_error("libarchive not supported in this build");
#endif
}

}
}
