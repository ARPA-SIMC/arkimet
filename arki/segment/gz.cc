#include "gz.h"
#include "common.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/validator.h"
#include "arki/scan.h"
#include "arki/utils/compress.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/iotrace.h"
#include <fcntl.h>
#include <vector>
#include <algorithm>
#include <sys/uio.h>

using namespace std;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace segment {
namespace gz {

namespace {

struct Creator : public AppendCreator
{
    std::vector<uint8_t> padding;
    File out;
    compress::GzipWriter gzout;
    size_t written = 0;
    std::filesystem::path dest_abspath_idx;

    Creator(const std::string& root, const std::filesystem::path& relpath, metadata::Collection& mds, const std::filesystem::path& dest_abspath)
        : AppendCreator(root, relpath, mds), out(dest_abspath), gzout(out, 0)
    {
    }
    Creator(const std::filesystem::path& root, const std::filesystem::path& relpath, metadata::Collection& mds, const std::filesystem::path& dest_abspath, const std::filesystem::path& dest_abspath_idx, unsigned groupsize)
        : AppendCreator(root, relpath, mds), out(dest_abspath), gzout(out, groupsize), dest_abspath_idx(dest_abspath_idx)
    {
    }

    size_t append(const metadata::Data& data) override
    {
        size_t wrpos = written;
        gzout.add(data.read());
        if (!padding.empty())
            gzout.add(padding);
        gzout.close_entry();
        written += data.size() + padding.size();
        return wrpos;
    }

    void create()
    {
        out.open(O_WRONLY | O_CREAT | O_TRUNC, 0666);
        AppendCreator::create();
        gzout.flush();
        out.fdatasync();
        out.close();
        if (!(dest_abspath_idx.empty() || gzout.idx.only_one_group()))
        {
            sys::File outidx(dest_abspath_idx, O_WRONLY | O_CREAT | O_TRUNC, 0666);
            gzout.idx.write(outidx);
            outidx.fdatasync();
            outidx.close();
        }
    }
};

template<typename Segment>
struct CheckBackend : public AppendCheckBackend
{
    const std::filesystem::path& gzabspath;
    std::vector<uint8_t> all_data;

    CheckBackend(const std::filesystem::path& gzabspath, const std::filesystem::path& relpath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
        : AppendCheckBackend(reporter, relpath, mds), gzabspath(gzabspath)
    {
    }

    size_t actual_end(off_t offset, size_t size) const override
    {
        return offset + size + Segment::padding;
    }

    void validate(Metadata& md, const types::source::Blob& source) override
    {
        validator->validate_buf(all_data.data() + source.offset, source.size);
    }

    size_t offset_end() const override { return all_data.size(); }

    State check()
    {
        std::unique_ptr<struct stat> st = sys::stat(gzabspath);
        if (st.get() == nullptr)
            return SEGMENT_DELETED;

        // Decompress all the segment in memory
        all_data = compress::gunzip(gzabspath);

        return AppendCheckBackend::check();
    }
};

}

time_t Segment::timestamp() const
{
    return max(sys::timestamp(sys::with_suffix(abspath, ".gz")), sys::timestamp(sys::with_suffix(abspath, ".gz.idx"), 0));
}

bool Segment::can_store(const std::string& format)
{
    return format == "grib" || format == "bufr" || format == "vm2";
}

template<typename Segment>
Reader<Segment>::Reader(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath, std::shared_ptr<core::Lock> lock)
    : BaseReader<Segment>(format, root, relpath, abspath, lock), fd(sys::with_suffix(abspath, ".gz"), O_RDONLY), reader(fd)
{
    // Read index
    std::filesystem::path idxfname = sys::with_suffix(fd.path(), ".idx");
    if (std::filesystem::exists(idxfname))
        reader.idx.read(idxfname);
}

template<typename Segment>
bool Reader<Segment>::scan_data(metadata_dest_func dest)
{
    auto scanner = scan::Scanner::get_scanner(this->segment().format);
    compress::TempUnzip uncompressed(this->segment().abspath);
    return scanner->scan_segment(this->shared_from_this(), dest);
}

template<typename Segment>
std::vector<uint8_t> Reader<Segment>::read(const types::source::Blob& src)
{
    vector<uint8_t> buf = reader.read(src.offset, src.size);
    iotrace::trace_file(this->segment().abspath, src.offset, src.size, "read data");
    return buf;
}


/*
 * Checker
 */

template<typename Segment>
Checker<Segment>::Checker(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath)
    : BaseChecker<Segment>(format, root, relpath, abspath), gzabspath(sys::with_suffix(abspath, ".gz")), gzidxabspath(sys::with_suffix(abspath, ".gz.idx"))
{
}

template<typename Segment>
bool Checker<Segment>::exists_on_disk()
{
    return std::filesystem::exists(gzabspath);
}

template<typename Segment>
bool Checker<Segment>::is_empty()
{
    if (sys::size(gzabspath) > 1024) return false;
    return utils::compress::gunzip(gzabspath).empty();
}

template<typename Segment>
size_t Checker<Segment>::size()
{
    return sys::size(gzabspath) + sys::size(gzidxabspath, 0);
}

template<typename Segment>
void Checker<Segment>::move_data(const std::filesystem::path& new_root, const std::filesystem::path& new_relpath, const std::filesystem::path& new_abspath)
{
    std::filesystem::rename(gzabspath, sys::with_suffix(new_abspath, ".gz"));
    sys::rename_ifexists(gzidxabspath, sys::with_suffix(new_abspath, ".gz.idx"));
}

template<typename Segment>
bool Checker<Segment>::rescan_data(std::function<void(const std::string&)> reporter, std::shared_ptr<core::Lock> lock, metadata_dest_func dest)
{
    auto reader = this->segment().reader(lock);
    return reader->scan_data(dest);
}

template<typename Segment>
State Checker<Segment>::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    CheckBackend<Segment> checker(gzabspath, this->segment().relpath, reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

template<typename Segment>
size_t Checker<Segment>::remove()
{
    size_t size = this->size();
    sys::unlink(gzabspath);
    std::filesystem::remove(gzidxabspath);
    return size;
}

template<typename Segment>
core::Pending Checker<Segment>::repack(const std::filesystem::path& rootdir, metadata::Collection& mds, const RepackConfig& cfg)
{
    auto tmpabspath = sys::with_suffix(gzabspath, ".repack");
    files::FinalizeTempfilesTransaction* finalize;
    core::Pending p = finalize = new files::FinalizeTempfilesTransaction;
    finalize->tmpfiles.push_back(tmpabspath);

    if (cfg.gz_group_size == 0)
    {
        finalize->on_commit = [&](const std::vector<std::filesystem::path>& tmpfiles) {
            std::filesystem::rename(tmpfiles[0], gzabspath);
            std::filesystem::remove(sys::with_suffix(gzabspath, ".idx"));
        };

        Creator creator(rootdir, this->segment().relpath, mds, tmpabspath);
        creator.validator = &scan::Validator::by_filename(this->segment().abspath);
        if (Segment::padding == 1) creator.padding.push_back('\n');
        creator.create();
    } else {
        auto tmpidxabspath = sys::with_suffix(gzidxabspath, ".repack");
        finalize->tmpfiles.push_back(tmpidxabspath);
        finalize->on_commit = [&](const std::vector<std::filesystem::path>& tmpfiles) {
            std::filesystem::rename(tmpfiles[0], this->segment().abspath);
            if (!sys::rename_ifexists(tmpfiles[1], gzidxabspath))
                std::filesystem::remove(gzidxabspath);
        };

        Creator creator(rootdir, this->segment().relpath, mds, tmpabspath, tmpidxabspath, cfg.gz_group_size);
        creator.validator = &scan::Validator::by_filename(this->segment().abspath);
        if (Segment::padding == 1) creator.padding.push_back('\n');
        creator.create();
    }

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

template<typename Segment>
void Checker<Segment>::test_truncate(size_t offset)
{
    if (offset > 0)
        throw std::runtime_error("gz test_truncate not implemented for offset > 0");

    utils::files::PreserveFileTimes pft(gzabspath);

    std::filesystem::remove(gzabspath);
    std::filesystem::remove(gzidxabspath);

    sys::File out(gzabspath, O_WRONLY | O_CREAT | O_TRUNC);
    compress::GzipWriter writer(out);
    writer.flush();
    out.close();
}

template<typename Segment>
void Checker<Segment>::test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx)
{
    throw std::runtime_error("test_make_hole not implemented");
}

template<typename Segment>
void Checker<Segment>::test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx)
{
    throw std::runtime_error("test_make_overlap not implemented");
}

template<typename Segment>
void Checker<Segment>::test_corrupt(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    utils::files::PreserveFileTimes pt(this->segment().abspath);
    sys::File fd(this->segment().abspath, O_RDWR);
    fd.lseek(s.offset);
    fd.write_all_or_throw("\0", 1);
}

}

namespace gzconcat {

const char* Segment::type() const { return "gzconcat"; }
bool Segment::single_file() const { return true; }
std::shared_ptr<segment::Reader> Segment::reader(std::shared_ptr<core::Lock> lock) const
{
    return make_shared<Reader>(format, root, relpath, abspath, lock);
}
std::shared_ptr<segment::Checker> Segment::checker() const
{
    return make_shared<Checker>(format, root, relpath, abspath);
}
bool Segment::can_store(const std::string& format)
{
    return format == "grib" || format == "bufr";
}
std::shared_ptr<segment::Checker> Segment::make_checker(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath)
{
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::create(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath, metadata::Collection& mds, const RepackConfig& cfg)
{
    if (cfg.gz_group_size == 0)
    {
        gz::Creator creator(rootdir, relpath, mds, sys::with_suffix(abspath, ".gz"));
        creator.create();
        return make_shared<Checker>(format, rootdir, relpath, abspath);
    } else {
        gz::Creator creator(rootdir, relpath, mds, sys::with_suffix(abspath, ".gz"), sys::with_suffix(abspath, ".gz.idx"), cfg.gz_group_size);
        creator.create();
        return make_shared<Checker>(format, rootdir, relpath, abspath);
    }
}

}

namespace gzlines {

const char* Segment::type() const { return "gzlines"; }
bool Segment::single_file() const { return true; }
std::shared_ptr<segment::Reader> Segment::reader(std::shared_ptr<core::Lock> lock) const
{
    return make_shared<Reader>(format, root, relpath, abspath, lock);
}
std::shared_ptr<segment::Checker> Segment::checker() const
{
    return make_shared<Checker>(format, root, relpath, abspath);
}
bool Segment::can_store(const std::string& format)
{
    return format == "vm2";
}
std::shared_ptr<segment::Checker> Segment::make_checker(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath)
{
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::create(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath, metadata::Collection& mds, const RepackConfig& cfg)
{
    if (cfg.gz_group_size == 0)
    {
        gz::Creator creator(rootdir, relpath, mds, sys::with_suffix(abspath, ".gz"));
        creator.padding.push_back('\n');
        creator.create();
        return make_shared<Checker>(format, rootdir, relpath, abspath);
    } else {
        gz::Creator creator(rootdir, relpath, mds, sys::with_suffix(abspath, ".gz"), sys::with_suffix(abspath, ".gz.idx"), cfg.gz_group_size);
        creator.padding.push_back('\n');
        creator.create();
        return make_shared<Checker>(format, rootdir, relpath, abspath);
    }
}

}

namespace gz {
template class Reader<gzlines::Segment>;
template class Checker<gzlines::Segment>;
template class Reader<gzconcat::Segment>;
template class Checker<gzconcat::Segment>;
}

}
}
#include "base.tcc"
