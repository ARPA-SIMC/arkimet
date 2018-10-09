#include "dir.h"
#include "common.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/scan.h"
#include "arki/scan/validator.h"
#include "arki/scan/dir.h"
#include "arki/utils/string.h"
#include "arki/utils/accounting.h"
#include "arki/iotrace.h"
#include "arki/nag.h"
#include <cerrno>
#include <cstring>
#include <cstdint>
#include <set>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <sys/sendfile.h>
#include <algorithm>

using namespace std;
using namespace arki::types;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace segment {
namespace dir {

namespace {

struct Creator : public AppendCreator
{
    std::string format;
    std::string dest_abspath;
    size_t current_pos = 0;
    bool hardlink = false;

    Creator(const std::string& root, const std::string& relpath, metadata::Collection& mds, const std::string& dest_abspath)
        : AppendCreator(root, relpath, mds), dest_abspath(dest_abspath)
    {
        if (!mds.empty())
            format = mds[0].source().format;
    }

    Span append_md(Metadata& md) override
    {
        Span res;

        if (hardlink)
        {
            const source::Blob& source = md.sourceBlob();
            res.size = source.size;
            std::string src = str::joinpath(source.absolutePathname(), SequenceFile::data_fname(source.offset, format));
            std::string dst = str::joinpath(dest_abspath, SequenceFile::data_fname(current_pos, format));

            if (::link(src.c_str(), dst.c_str()) != 0)
                throw_system_error("cannot link " + src + " as " + dst);
        } else {
            const auto& data = md.get_data();
            res.size = data.size();
            if (validator)
                validator->validate_data(data);

            sys::File fd(str::joinpath(dest_abspath, SequenceFile::data_fname(current_pos, format)),
                        O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL, 0666);
            try {
                data.write(fd);
                if (fdatasync(fd) < 0)
                    fd.throw_error("cannot flush write");
                fd.close();
            } catch (...) {
                unlink(fd.name().c_str());
                throw;
            }
        }

        res.offset = current_pos;
        ++current_pos;
        return res;
    }

    void create()
    {
        sys::makedirs(dest_abspath);
        AppendCreator::create();
        SequenceFile seqfile(dest_abspath);
        seqfile.open();
        seqfile.write_sequence(current_pos);
    }
};

struct CheckBackend : public AppendCheckBackend
{
    const std::string& format;
    const std::string& abspath;
    std::unique_ptr<struct stat> st;
    // File size by offset
    map<size_t, size_t> on_disk;
    size_t max_sequence = 0;

    CheckBackend(const std::string& format, const std::string& abspath, const std::string& relpath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
        : AppendCheckBackend(reporter, relpath, mds), format(format), abspath(abspath)
    {
    }

    void validate(Metadata& md, const types::source::Blob& source) override
    {
        string fname = str::joinpath(abspath, SequenceFile::data_fname(source.offset, format));
        core::File data(fname, O_RDONLY);
        validator->validate_file(data, 0, source.size);
    }

    size_t actual_end(off_t offset, size_t size) const override { return offset + 1; }
    size_t offset_end() const override { return max_sequence + 1; }

    State check_source(const types::source::Blob& source) override
    {
        auto si = on_disk.find(source.offset);
        if (si == on_disk.end())
        {
            stringstream ss;
            ss << "expected file " << source.offset << " not found in the file system";
            reporter(ss.str());
            return SEGMENT_CORRUPTED;
        }
        if (!(source.size == si->second || (format == "vm2" && (source.size + 1 == si->second))))
        {
            stringstream ss;
            ss << "expected file " << source.offset << " has size " << si->second << " instead of expected " << source.size;
            reporter(ss.str());
            return SEGMENT_CORRUPTED;
        }
        on_disk.erase(si);
        return SEGMENT_OK;
    }

    State check()
    {
        st = sys::stat(abspath);
        if (st.get() == nullptr)
        {
            reporter(abspath + " not found on disk");
            return SEGMENT_DELETED;
        }

        if (!S_ISDIR(st->st_mode))
        {
            reporter(abspath + " is not a directory");
            return SEGMENT_CORRUPTED;
        }

        sys::Path dir(abspath);
        for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
        {
            if (!i.isreg()) continue;
            if (!str::endswith(i->d_name, format)) continue;
            struct stat st;
            i.path->fstatat(i->d_name, st);
            size_t seq = (size_t)strtoul(i->d_name, 0, 10);
            on_disk.insert(make_pair(seq, st.st_size));
            max_sequence = max(max_sequence, seq);
        }

        bool dirty = false;
        State state = AppendCheckBackend::check();
        if (!state.is_ok())
        {
            if (state.value == SEGMENT_DIRTY)
                dirty = true;
            else
                return state;
        }

        // Check files on disk that were not accounted for
        for (const auto& di: on_disk)
        {
            auto scanner = scan::Scanner::get_scanner(format);
            auto idx = di.first;
            if (accurate)
            {
                string fname = SequenceFile::data_fname(idx, format);
                size_t size;
                Metadata md;
                try {
                    size = scanner->scan_singleton(fname, md);
                } catch (std::exception& e) {
                    stringstream out;
                    out << "unexpected data file " << idx << " fails to scan (" << e.what() << ")";
                    reporter(out.str());
                    dirty = true;
                    continue;
                }

                if (size == 0)
                {
                    stringstream ss;
                    ss << "unexpected data file " << idx << " does not contain any " << format << " items";
                    reporter(ss.str());
                    dirty = true;
                    continue;
                }
#if 0
                if (mds.size() > 1)
                {
                    stringstream ss;
                    ss << "unexpected data file " << idx << " contains " << mds.size() << " " << format << " items instead of 1";
                    reporter(ss.str());
                    return SEGMENT_CORRUPTED;
                }
#endif
            }
        }

        if (!on_disk.empty())
        {
            stringstream ss;
            ss << "segment contains " << on_disk.size() << " file(s) that the index does now know about";
            reporter(ss.str());
            dirty = true;
        }

        return dirty ? SEGMENT_DIRTY : SEGMENT_OK;
    }
};

}


const char* Segment::type() const { return "dir"; }
bool Segment::single_file() const { return false; }
time_t Segment::timestamp() const
{
    return sys::timestamp(str::joinpath(abspath, ".sequence"));
}
std::shared_ptr<segment::Reader> Segment::reader(std::shared_ptr<core::Lock> lock) const
{
    return make_shared<Reader>(format, root, relpath, abspath, lock);
}
std::shared_ptr<segment::Checker> Segment::checker() const
{
    return make_shared<Checker>(format, root, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::make_checker(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath)
{
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, const RepackConfig& cfg)
{
    Creator creator(rootdir, relpath, mds, abspath);
    creator.create();
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
bool Segment::can_store(const std::string& format)
{
    return format == "grib" || format == "bufr" || format == "odimh5" || format == "vm2";
}


const char* HoleSegment::type() const { return "hole_dir"; }



Reader::Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock)
    : BaseReader<Segment>(format, root, relpath, abspath, lock), dirfd(abspath, O_DIRECTORY)
{
}

bool Reader::scan_data(metadata_dest_func dest)
{
    scan::Dir scanner;
    return scanner.scan_segment(static_pointer_cast<segment::Reader>(this->shared_from_this()), dest);
}

sys::File Reader::open_src(const types::source::Blob& src)
{
    char dataname[32];
    snprintf(dataname, 32, "%06zd.%s", (size_t)src.offset, m_segment.format.c_str());
    int fd = dirfd.openat_ifexists(dataname, O_RDONLY | O_CLOEXEC);
    if (fd == -1)
    {
        stringstream msg;
        msg << dataname << " does not exist in directory segment " << dirfd.name();
        throw std::runtime_error(msg.str());
    }
    sys::File file_fd(fd, str::joinpath(dirfd.name(), dataname));

    if (posix_fadvise(file_fd, 0, src.size, POSIX_FADV_DONTNEED) != 0)
        nag::debug("fadvise on %s failed: %m", file_fd.name().c_str());

    return file_fd;
}

std::vector<uint8_t> Reader::read(const types::source::Blob& src)
{
    vector<uint8_t> buf;
    buf.resize(src.size);

    sys::File file_fd = open_src(src);

    ssize_t res = file_fd.pread(buf.data(), src.size, 0);
    if ((size_t)res != src.size)
    {
        stringstream msg;
        msg << "cannot read " << src.size << " bytes of " << src.format << " data from " << m_segment.abspath << ":"
            << src.offset << ": only " << res << "/" << src.size << " bytes have been read";
        throw std::runtime_error(msg.str());
    }

    acct::plain_data_read_count.incr();
    iotrace::trace_file(dirfd.name(), src.offset, src.size, "read data");

    return buf;
}

size_t Reader::stream(const types::source::Blob& src, core::NamedFileDescriptor& out)
{
    if (src.format == "vm2")
    {
        vector<uint8_t> buf = read(src);

        struct iovec todo[2] = {
            { (void*)buf.data(), buf.size() },
            { (void*)"\n", 1 },
        };
        ssize_t res = ::writev(out, todo, 2);
        if (res < 0 || (unsigned)res != buf.size() + 1)
            throw_system_error("cannot write " + to_string(buf.size() + 1) + " bytes to " + out.name());
        return buf.size() + 1;
    } else {
        sys::File file_fd = open_src(src);

        // TODO: add a stream method to sys::FileDescriptor that does the
        // right thing depending on what's available in the system, and
        // potentially also handles retries
        off_t offset = 0;
        while ((unsigned)offset < src.size)
        {
            size_t size = src.size - offset;
            ssize_t res = sendfile(out, file_fd, &offset, size);
            if (res < 0)
            {
                stringstream msg;
                msg << "cannot stream " << size << " bytes of " << src.format << " data from " << file_fd.name() << ":"
                    << src.offset << "+" << offset;
                throw_system_error(msg.str());
            }
        }

        acct::plain_data_read_count.incr();
        iotrace::trace_file(dirfd.name(), src.offset, src.size, "streamed data");
        return offset;
    }
}


template<typename Segment>
BaseWriter<Segment>::BaseWriter(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath)
    : segment::BaseWriter<Segment>(format, root, relpath, abspath), seqfile(abspath)
{
    // Ensure that the directory 'abspath' exists
    sys::makedirs(abspath);
    seqfile.open();
    current_pos = seqfile.read_sequence();
}

template<typename Segment>
BaseWriter<Segment>::~BaseWriter()
{
    if (!this->fired) rollback_nothrow();
}

template<typename Segment>
size_t BaseWriter<Segment>::next_offset() const
{
    return current_pos;
}

template<typename Segment>
const types::source::Blob& BaseWriter<Segment>::append(Metadata& md, bool drop_cached_data_on_commit)
{
    this->fired = false;

    File fd(str::joinpath(this->segment().abspath, SequenceFile::data_fname(current_pos, this->segment().format)),
                O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL, 0666);
    try {
        write_file(md, fd);
    } catch (...) {
        unlink(fd.name().c_str());
        throw;
    }
    written.push_back(fd.name());
    fd.close();
    pending.emplace_back(md, source::Blob::create_unlocked(md.source().format, this->segment().root, this->segment().relpath, current_pos, md.data_size()), drop_cached_data_on_commit);
    ++current_pos;
    return *pending.back().new_source;
}

template<typename Segment>
void BaseWriter<Segment>::commit()
{
    if (this->fired) return;
    seqfile.write_sequence(current_pos);
    for (auto& p: pending)
        p.set_source();
    pending.clear();
    written.clear();
    this->fired = true;
}

template<typename Segment>
void BaseWriter<Segment>::rollback()
{
    if (this->fired) return;
    for (auto fn: written)
        sys::unlink(fn);
    pending.clear();
    written.clear();
    this->fired = true;
}

template<typename Segment>
void BaseWriter<Segment>::rollback_nothrow() noexcept
{
    if (this->fired) return;
    for (auto fn: written)
        ::unlink(fn.c_str());
    pending.clear();
    written.clear();
    this->fired = true;
}


void Writer::write_file(Metadata& md, NamedFileDescriptor& fd)
{
    const metadata::Data& data = md.get_data();
    data.write(fd);
    if (fdatasync(fd) < 0)
        fd.throw_error("cannot flush write");
}


void HoleWriter::write_file(Metadata& md, NamedFileDescriptor& fd)
{
    if (ftruncate(fd, md.data_size()) == -1)
        fd.throw_error("cannot set file size");
}


template<typename Segment>
bool BaseChecker<Segment>::exists_on_disk()
{
    if (!sys::isdir(this->segment().abspath)) return false;
    return sys::exists(str::joinpath(this->segment().abspath, ".sequence"));
}

template<typename Segment>
bool BaseChecker<Segment>::is_empty()
{
    if (!sys::isdir(this->segment().abspath)) return false;
    sys::Path dir(this->segment().abspath);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (strcmp(i->d_name, ".") == 0) continue;
        if (strcmp(i->d_name, "..") == 0) continue;
        if (strcmp(i->d_name, ".sequence") == 0) continue;
        return false;
    }
    return true;
}

template<typename Segment>
size_t BaseChecker<Segment>::size()
{
    size_t res = 0;
    sys::Path dir(this->segment().abspath);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (!i.isreg()) continue;
        if (!str::endswith(i->d_name, this->segment().format)) continue;
        struct stat st;
        i.path->fstatat(i->d_name, st);
        res += st.st_size;
    }
    return res;
}

template<typename Segment>
void BaseChecker<Segment>::move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath)
{
    sys::rename(this->segment().abspath.c_str(), new_abspath.c_str());
}

template<typename Segment>
State BaseChecker<Segment>::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    CheckBackend checker(this->segment().format, this->segment().abspath, this->segment().relpath, reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

template<typename Segment>
void BaseChecker<Segment>::validate(Metadata& md, const scan::Validator& v)
{
    if (const types::source::Blob* blob = md.has_source_blob()) {
        if (blob->filename != this->segment().relpath)
            throw std::runtime_error("metadata to validate does not appear to be from this segment");

        string fname = str::joinpath(this->segment().abspath, SequenceFile::data_fname(blob->offset, blob->format));
        sys::File fd(fname, O_RDONLY);
        v.validate_file(fd, 0, blob->size);
        return;
    }
    const auto& data = md.get_data();
    auto buf = data.read();
    v.validate_buf(buf.data(), buf.size());  // TODO: add a validate_data that takes the metadata::Data
}

template<typename Segment>
void BaseChecker<Segment>::foreach_datafile(std::function<void(const char*)> f)
{
    sys::Path dir(this->segment().abspath);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (!i.isreg()) continue;
        if (strcmp(i->d_name, ".sequence") == 0) continue;
        if (!str::endswith(i->d_name, this->segment().format)) continue;
        f(i->d_name);
    }
}

template<typename Segment>
size_t BaseChecker<Segment>::remove()
{
    size_t size = 0;
    foreach_datafile([&](const char* name) {
        string pathname = str::joinpath(this->segment().abspath, name);
        size += sys::size(pathname);
        sys::unlink(pathname);
    });
    sys::unlink_ifexists(str::joinpath(this->segment().abspath, ".sequence"));
    sys::unlink_ifexists(str::joinpath(this->segment().abspath, ".write-lock"));
    sys::unlink_ifexists(str::joinpath(this->segment().abspath, ".repack-lock"));
    // Also remove the directory if it is empty
    rmdir(this->segment().abspath.c_str());
    return size;
}

template<typename Segment>
Pending BaseChecker<Segment>::repack(const std::string& rootdir, metadata::Collection& mds, const RepackConfig& cfg)
{
    struct Rename : public Transaction
    {
        std::string tmpabspath;
        std::string abspath;
        std::string tmppos;
        bool fired;

        Rename(const std::string& tmpabspath, const std::string& abspath)
            : tmpabspath(tmpabspath), abspath(abspath), tmppos(abspath + ".pre-repack"), fired(false)
        {
        }

        virtual ~Rename()
        {
            if (!fired) rollback();
        }

        void commit() override
        {
            if (fired) return;

            // It is impossible to make this atomic, so we just try to make it as quick as possible

            // Move the old directory inside the temp dir, to free the old directory name
            if (rename(abspath.c_str(), tmppos.c_str()))
                throw_system_error("cannot rename " + abspath + " to " + tmppos);

            // Rename the data file to its final name
            if (rename(tmpabspath.c_str(), abspath.c_str()) < 0)
                throw_system_error("cannot rename " + tmpabspath + " to " + abspath
                    + " (ATTENTION: please check if you need to rename " + tmppos + " to " + abspath
                    + " manually to restore it as it was before the repack)");

            // Remove the old data
            sys::rmtree(tmppos);

            fired = true;
        }

        void rollback() override
        {
            if (fired) return;

            try
            {
                sys::rmtree(tmpabspath);
            } catch (std::exception& e) {
                nag::warning("Failed to remove %s while recovering from a failed repack: %s", tmpabspath.c_str(), e.what());
            }

            rename(tmppos.c_str(), abspath.c_str());

            fired = true;
        }

        void rollback_nothrow() noexcept override
        {
            if (fired) return;

            try
            {
                sys::rmtree(tmpabspath);
            } catch (std::exception& e) {
                nag::warning("Failed to remove %s while recovering from a failed repack: %s", tmpabspath.c_str(), e.what());
            }

            rename(tmppos.c_str(), abspath.c_str());

            fired = true;
        }
    };

    string tmprelpath = this->segment().relpath + ".repack";
    string tmpabspath = this->segment().abspath + ".repack";

    Pending p(new Rename(tmpabspath, this->segment().abspath));

    Creator creator(rootdir, this->segment().relpath, mds, tmpabspath);
    creator.hardlink = true;
    creator.validator = &scan::Validator::by_format(this->segment().format);
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

template<typename Segment>
void BaseChecker<Segment>::test_truncate(size_t offset)
{
    utils::files::PreserveFileTimes pft(this->segment().abspath);

    // Truncate dir segment
    foreach_datafile([&](const char* name) {
        if (strtoul(name, 0, 10) >= offset)
            sys::unlink(str::joinpath(this->segment().abspath, name));
    });
}

template<typename Segment>
void BaseChecker<Segment>::test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx)
{
    SequenceFile seqfile(this->segment().abspath);
    utils::files::PreserveFileTimes pf(seqfile.name());
    seqfile.open();
    size_t pos = seqfile.read_sequence();
    if (data_idx >= mds.size())
    {
        for (unsigned i = 0; i < hole_size; ++i)
        {
            File fd(str::joinpath(this->segment().abspath, SequenceFile::data_fname(pos, this->segment().format)),
                O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL, 0666);
            fd.close();
            ++pos;
        }
    } else {
        for (int i = mds.size() - 1; i >= (int)data_idx; --i)
        {
            unique_ptr<source::Blob> source(mds[i].sourceBlob().clone());
            sys::rename(
                    str::joinpath(source->absolutePathname(), SequenceFile::data_fname(source->offset, source->format)),
                    str::joinpath(source->absolutePathname(), SequenceFile::data_fname(source->offset + hole_size, source->format)));
            source->offset += hole_size;
            mds[i].set_source(std::move(source));
        }
        pos += hole_size;
    }
    seqfile.write_sequence(pos);
}

template<typename Segment>
void BaseChecker<Segment>::test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx)
{
    for (unsigned i = data_idx; i < mds.size(); ++i)
    {
        unique_ptr<source::Blob> source(mds[i].sourceBlob().clone());
        sys::rename(
                str::joinpath(source->absolutePathname(), SequenceFile::data_fname(source->offset, source->format)),
                str::joinpath(source->absolutePathname(), SequenceFile::data_fname(source->offset - overlap_size, source->format)));
        source->offset -= overlap_size;
        mds[i].set_source(std::move(source));
    }
}

template<typename Segment>
void BaseChecker<Segment>::test_corrupt(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    File fd(str::joinpath(s.absolutePathname(), SequenceFile::data_fname(s.offset, s.format)), O_WRONLY);
    fd.write_all_or_throw("\0", 1);
}


State HoleChecker::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    // Force quick, since the file contents are fake
    return BaseChecker::check(reporter, mds, true);
}

template class BaseWriter<Segment>;
template class BaseWriter<HoleSegment>;
template class BaseChecker<Segment>;
template class BaseChecker<HoleSegment>;

}
}
}
#include "base.tcc"
