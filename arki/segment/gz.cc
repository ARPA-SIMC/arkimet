#include "gz.h"
#include "common.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/validator.h"
#include "arki/scan.h"
#include "arki/utils/compress.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/tar.h"
#include "arki/utils.h"
#include "arki/nag.h"
#include "arki/utils/accounting.h"
#include "arki/iotrace.h"
#include <fcntl.h>
#include <vector>
#include <algorithm>
#include <sys/uio.h>
#include <system_error>

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

    Creator(const std::string& root, const std::string& relpath, metadata::Collection& mds, const std::string& dest_abspath)
        : AppendCreator(root, relpath, mds), out(dest_abspath), gzout(out, 0)
    {
    }

    size_t append(const std::vector<uint8_t>& data) override
    {
        size_t wrpos = written;
        gzout.add(data);
        if (!padding.empty())
            gzout.add(padding);
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
    }
};

template<typename Segment>
struct CheckBackend : public AppendCheckBackend
{
    const std::string& gzabspath;
    std::vector<uint8_t> all_data;

    CheckBackend(const std::string& gzabspath, const std::string& relpath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
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

    size_t offset_end() const { return all_data.size(); }

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
    return max(sys::timestamp(abspath + ".gz"), sys::timestamp(abspath + ".gz.idx", 0));
}


template<typename Segment>
Reader<Segment>::Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock)
    : BaseReader<Segment>(format, root, relpath, abspath, lock), fd(abspath + ".gz", O_RDONLY), reader(fd)
{
    // Read index
    std::string idxfname = fd.name() + ".idx";
    if (sys::exists(idxfname))
        reader.idx.read(idxfname);
}

template<typename Segment>
bool Reader<Segment>::scan_data(metadata_dest_func dest)
{
    auto scanner = scan::Scanner::get_scanner(this->segment().format);
    compress::TempUnzip uncompressed(this->segment().abspath);
    return scanner->scan_file(this->segment().abspath, this->shared_from_this(), dest);
}

template<typename Segment>
std::vector<uint8_t> Reader<Segment>::read(const types::source::Blob& src)
{
    vector<uint8_t> buf = reader.read(src.offset, src.size);
    iotrace::trace_file(this->segment().abspath, src.offset, src.size, "read data");
    return buf;
}

template<typename Segment>
size_t Reader<Segment>::stream(const types::source::Blob& src, core::NamedFileDescriptor& out)
{
    vector<uint8_t> buf = read(src);
    if (src.format == "vm2")
    {
        struct iovec todo[2] = {
            { (void*)buf.data(), buf.size() },
            { (void*)"\n", 1 },
        };
        ssize_t res = ::writev(out, todo, 2);
        if (res < 0 || (unsigned)res != buf.size() + 1)
            throw_system_error("cannot write " + to_string(buf.size() + 1) + " bytes to " + out.name());
        return buf.size() + 1;
    } else {
        out.write_all_or_throw(buf);
        return buf.size();
    }
}


template<typename Segment>
Checker<Segment>::Checker(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath)
    : BaseChecker<Segment>(format, root, relpath, abspath), gzabspath(abspath + ".gz")
{
}

template<typename Segment>
bool Checker<Segment>::exists_on_disk()
{
    return sys::exists(gzabspath);
}

template<typename Segment>
size_t Checker<Segment>::size()
{
    return sys::size(gzabspath);
}

template<typename Segment>
void Checker<Segment>::move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath)
{
    sys::rename(gzabspath, new_abspath + ".gz");
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
    size_t size = sys::size(gzabspath);
    sys::unlink(gzabspath);
    return size;
}

template<typename Segment>
Pending Checker<Segment>::repack(const std::string& rootdir, metadata::Collection& mds, const RepackConfig& cfg)
{
    string tmpabspath = gzabspath + ".repack";

    Pending p(new files::RenameTransaction(tmpabspath, this->segment().abspath));

    Creator creator(rootdir, this->segment().relpath, mds, tmpabspath);
    creator.validator = &scan::Validator::by_filename(this->segment().abspath);
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

template<typename Segment>
void Checker<Segment>::test_truncate(size_t offset)
{
    if (!sys::exists(this->segment().abspath))
        sys::write_file(this->segment().abspath, "");

    if (offset % 512 != 0)
        offset += 512 - (offset % 512);

    utils::files::PreserveFileTimes pft(this->segment().abspath);
    if (::truncate(this->segment().abspath.c_str(), offset) < 0)
    {
        stringstream ss;
        ss << "cannot truncate " << this->segment().abspath << " at " << offset;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
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
    sys::File fd(this->segment().abspath, O_RDWR);
    sys::PreserveFileTimes pt(fd);
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
std::shared_ptr<segment::Checker> Segment::make_checker(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath)
{
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags)
{
    gz::Creator creator(rootdir, relpath, mds, abspath + ".gz");
    creator.create();
    return make_shared<Checker>(format, rootdir, relpath, abspath);
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
std::shared_ptr<segment::Checker> Segment::make_checker(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath)
{
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags)
{
    gz::Creator creator(rootdir, relpath, mds, abspath + ".gz");
    creator.padding.push_back('\n');
    creator.create();
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}

Pending Checker::repack(const std::string& rootdir, metadata::Collection& mds, const RepackConfig& cfg)
{
    string tmpabspath = gzabspath + ".repack";

    files::RenameTransaction* rename;
    Pending p(rename = new files::RenameTransaction(tmpabspath, segment().abspath));

    gz::Creator creator(rootdir, segment().relpath, mds, tmpabspath);
    creator.validator = &scan::Validator::by_filename(segment().abspath);
    creator.padding.push_back('\n');
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
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
