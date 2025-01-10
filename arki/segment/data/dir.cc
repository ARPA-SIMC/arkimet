#include "dir.h"
#include "common.h"
#include "missing.h"
#include "arki/exceptions.h"
#include "arki/stream.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/scan.h"
#include "arki/scan/validator.h"
#include "arki/utils/accounting.h"
#include "arki/iotrace.h"
#include "arki/nag.h"
#include <cerrno>
#include <cstring>
#include <cstdint>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <algorithm>

using namespace std;
using namespace arki::types;
using namespace arki::core;
using namespace arki::utils;

namespace arki::segment::data::dir {

namespace {

struct Creator : public AppendCreator
{
    std::filesystem::path dest_abspath;
    size_t current_pos = 0;
    bool hardlink = false;

    Creator(const Segment& segment, metadata::Collection& mds, const std::filesystem::path& dest_abspath)
        : AppendCreator(segment, mds), dest_abspath(dest_abspath)
    {
    }

    Span append_md(Metadata& md) override
    {
        Span res;

        if (hardlink)
        {
            const source::Blob& source = md.sourceBlob();
            res.size = source.size;
            std::filesystem::path src = source.absolutePathname() / SequenceFile::data_fname(source.offset, segment.format);
            std::filesystem::path dst = dest_abspath / SequenceFile::data_fname(current_pos, segment.format);

            if (::link(src.c_str(), dst.c_str()) != 0)
                throw_system_error("cannot link "s + src.native() + " as " + dst.native());
        } else {
            const auto& data = md.get_data();
            res.size = data.size();
            if (validator)
                validator->validate_data(data);

            sys::File fd(dest_abspath / SequenceFile::data_fname(current_pos, segment.format),
                        O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL, 0666);
            try {
                data.write(fd);
                if (fdatasync(fd) < 0)
                    fd.throw_error("cannot flush write");
                fd.close();
            } catch (...) {
                unlink(fd.path().c_str());
                throw;
            }
        }

        res.offset = current_pos;
        ++current_pos;
        return res;
    }

    void create()
    {
        std::filesystem::create_directories(dest_abspath);
        AppendCreator::create();
        SequenceFile seqfile(dest_abspath);
        seqfile.open();
        seqfile.write_sequence(current_pos - 1);
    }
};

struct CheckBackend : public AppendCheckBackend
{
    std::unique_ptr<struct stat> st;
    dir::Scanner scanner;

    CheckBackend(const Segment& segment, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
        : AppendCheckBackend(reporter, segment, mds), scanner(segment)
    {
    }

    void validate(Metadata& md, const types::source::Blob& source) override
    {
        filesystem::path fname = segment.abspath / SequenceFile::data_fname(source.offset, segment.format);
        core::File data(fname, O_RDONLY);
        validator->validate_file(data, 0, source.size);
    }

    size_t actual_end(off_t offset, size_t size) const override { return offset + 1; }
    size_t offset_end() const override { return scanner.max_sequence + 1; }
    size_t compute_unindexed_space(const std::vector<Span>& indexed_spans) const override
    {
        // When this is called, all elements found in the index have already
        // been removed from scanner. We can just then add up what's left of
        // sizes in scanner
        size_t res = 0;
        for (const auto& i: scanner.on_disk)
            res += i.second.size;
        return res;
    }

    State check_source(const types::source::Blob& source) override
    {
        auto si = scanner.on_disk.find(source.offset);
        if (si == scanner.on_disk.end())
        {
            stringstream ss;
            ss << "expected file " << source.offset << " not found in the file system";
            reporter(ss.str());
            return State(SEGMENT_CORRUPTED);
        }
        if (!(source.size == si->second.size || (segment.format == DataFormat::VM2 && (source.size + 1 == si->second.size))))
        {
            stringstream ss;
            ss << "expected file " << source.offset << " has size " << si->second.size << " instead of expected " << source.size;
            reporter(ss.str());
            return State(SEGMENT_CORRUPTED);
        }
        scanner.on_disk.erase(si);
        return State(SEGMENT_OK);
    }

    State check()
    {
        st = sys::stat(segment.abspath);
        if (st.get() == nullptr)
        {
            reporter(segment.abspath.native() + " not found on disk");
            return State(SEGMENT_DELETED);
        }

        if (!S_ISDIR(st->st_mode))
        {
            reporter(segment.abspath.native() + " is not a directory");
            return State(SEGMENT_CORRUPTED);
        }

        size_t cur_sequence = 0;
        {
            SequenceFile sf(segment.abspath);
            sf.open();
            cur_sequence = sf.read_sequence();
        }

        scanner.list_files();

        bool dirty = false;
        State state = AppendCheckBackend::check();
        if (!state.is_ok())
        {
            if (state.has(SEGMENT_DIRTY))
                dirty = true;
            else
                return state;
        }

        if (cur_sequence < scanner.max_sequence)
        {
            stringstream out;
            out << "sequence file has value " << cur_sequence << " but found files until sequence " << scanner.max_sequence;
            reporter(out.str());
            return State(SEGMENT_UNALIGNED);
        }

        // Check files on disk that were not accounted for
        for (const auto& di: scanner.on_disk)
        {
            auto scanner = scan::Scanner::get_scanner(segment.format);
            auto idx = di.first;
            if (accurate)
            {
                filesystem::path fname = SequenceFile::data_fname(idx, segment.format);
                try {
                    scanner->scan_singleton(fname);
                } catch (std::exception& e) {
                    stringstream out;
                    out << "unexpected data file " << idx << " fails to scan (" << e.what() << ")";
                    reporter(out.str());
                    dirty = true;
                    continue;
                }
            }
        }

        if (!scanner.on_disk.empty())
        {
            stringstream ss;
            ss << "segment contains " << scanner.on_disk.size() << " file(s) that the index does now know about";
            reporter(ss.str());
            dirty = true;
        }

        return State(dirty ? SEGMENT_DIRTY : SEGMENT_OK);
    }
};

}


const char* Data::type() const { return "dir"; }
bool Data::single_file() const { return false; }
time_t Data::timestamp() const
{
    return sys::timestamp(segment().abspath / ".sequence");
}
std::shared_ptr<data::Reader> Data::reader(std::shared_ptr<core::Lock> lock) const
{
    try {
        return make_shared<Reader>(static_pointer_cast<const Data>(shared_from_this()), lock);
    } catch (std::system_error& e) {
        if (e.code() == std::errc::no_such_file_or_directory)
            return make_shared<arki::segment::data::missing::Reader>(shared_from_this(), lock);
        else
            throw;
    }
}
std::shared_ptr<data::Writer> Data::writer(const data::WriterConfig& config, bool mock_data) const
{
    if (mock_data)
        return make_shared<HoleWriter>(config, static_pointer_cast<const Data>(shared_from_this()));
    else
        return make_shared<Writer>(config, static_pointer_cast<const Data>(shared_from_this()));
}
std::shared_ptr<data::Checker> Data::checker(bool mock_data) const
{
    if (mock_data)
        return make_shared<HoleChecker>(static_pointer_cast<const Data>(shared_from_this()));
    else
        return make_shared<Checker>(static_pointer_cast<const Data>(shared_from_this()));
}
std::shared_ptr<data::Checker> Data::create(const Segment& segment, metadata::Collection& mds, const RepackConfig& cfg)
{
    Creator creator(segment, mds, segment.abspath);
    creator.create();
    auto data = std::make_shared<const Data>(segment.shared_from_this());
    return make_shared<Checker>(data);
}

bool Data::can_store(DataFormat format)
{
    switch (format)
    {
        case DataFormat::GRIB:
        case DataFormat::BUFR:
        case DataFormat::ODIMH5:
        case DataFormat::VM2:
        case DataFormat::NETCDF:
        case DataFormat::JPEG:
            return true;
        default:
            return false;
    }
}


Reader::Reader(std::shared_ptr<const Data> data, std::shared_ptr<core::Lock> lock)
    : BaseReader<Data>(data, lock), dirfd(data->segment().abspath, O_DIRECTORY)
{
}

bool Reader::scan_data(metadata_dest_func dest)
{
    Scanner scanner(segment());
    scanner.list_files();
    return scanner.scan(static_pointer_cast<data::Reader>(this->shared_from_this()), dest);
}

sys::File Reader::open_src(const types::source::Blob& src)
{
    char dataname[32];
    snprintf(dataname, 32, "%06zu.%s", (size_t)src.offset, format_name(segment().format).c_str());
    int fd = dirfd.openat_ifexists(dataname, O_RDONLY | O_CLOEXEC);
    if (fd == -1)
    {
        stringstream msg;
        msg << dataname << " does not exist in directory segment " << dirfd.path();
        throw std::runtime_error(msg.str());
    }
    sys::File file_fd(fd, dirfd.path() / dataname);

    if (posix_fadvise(file_fd, 0, src.size, POSIX_FADV_DONTNEED) != 0)
        nag::debug("fadvise on %s failed: %s", file_fd.path().c_str(), strerror(errno));

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
        msg << "cannot read " << src.size << " bytes of " << src.format << " data from " << segment().abspath << ":"
            << src.offset << ": only " << res << "/" << src.size << " bytes have been read";
        throw std::runtime_error(msg.str());
    }

    acct::plain_data_read_count.incr();
    iotrace::trace_file(dirfd, src.offset, src.size, "read data");

    return buf;
}

stream::SendResult Reader::stream(const types::source::Blob& src, StreamOutput& out)
{
    if (src.format == DataFormat::VM2)
        return data::Reader::stream(src, out);

    sys::File file_fd = open_src(src);
    iotrace::trace_file(dirfd, src.offset, src.size, "streamed data");
    return out.send_file_segment(file_fd, 0, src.size);
}


template<typename Data>
BaseWriter<Data>::BaseWriter(const WriterConfig& config, std::shared_ptr<const Data> data)
    : data::BaseWriter<Data>(config, data), seqfile(data->segment().abspath)
{
    // Ensure that the directory 'abspath' exists
    std::filesystem::create_directories(this->segment().abspath);
    seqfile.open();
    // current_pos is the last sequence generated
    current_pos = seqfile.read_sequence();
    if (!seqfile.new_file)
        ++current_pos;
}

template<typename Data>
BaseWriter<Data>::~BaseWriter()
{
    if (!this->fired) rollback_nothrow();
}

template<typename Data>
size_t BaseWriter<Data>::next_offset() const
{
    return current_pos;
}

template<typename Data>
const types::source::Blob& BaseWriter<Data>::append(Metadata& md)
{
    this->fired = false;

    File fd(this->segment().abspath / SequenceFile::data_fname(current_pos, this->segment().format),
                O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL, 0666);
    try {
        write_file(md, fd);
    } catch (...) {
        unlink(fd.path().c_str());
        throw;
    }
    written.push_back(fd.path());
    fd.close();
    pending.emplace_back(this->config, md, source::Blob::create_unlocked(md.source().format, this->segment().root, this->segment().relpath, current_pos, md.data_size()));
    ++current_pos;
    return *pending.back().new_source;
}

template<typename Data>
void BaseWriter<Data>::commit()
{
    if (this->fired) return;
    seqfile.write_sequence(current_pos - 1);
    for (auto& p: pending)
        p.set_source();
    pending.clear();
    written.clear();
    this->fired = true;
}

template<typename Data>
void BaseWriter<Data>::rollback()
{
    if (this->fired) return;
    for (auto fn: written)
        sys::unlink(fn);
    pending.clear();
    written.clear();
    this->fired = true;
}

template<typename Data>
void BaseWriter<Data>::rollback_nothrow() noexcept
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
    if (!config.eatmydata)
        if (fdatasync(fd) < 0)
            fd.throw_error("cannot flush write");
}


void HoleWriter::write_file(Metadata& md, NamedFileDescriptor& fd)
{
    if (ftruncate(fd, md.data_size()) == -1)
        fd.throw_error("cannot set file size");
}


template<typename Data>
bool BaseChecker<Data>::exists_on_disk()
{
    /**
     * To consider the segment an existing dir segment, it needs to be a
     * directory that contains a .sequence file.
     *
     * Just an empty directory is considered not enough, to leave space for
     * implementing different formats of directory-based segments
     */
    if (!std::filesystem::is_directory(this->segment().abspath)) return false;
    return std::filesystem::exists(this->segment().abspath / ".sequence");
}

template<typename Data>
bool BaseChecker<Data>::is_empty()
{
    if (!std::filesystem::is_directory(this->segment().abspath)) return false;
    sys::Path dir(this->segment().abspath);

    // If we just have an empty directory, do not consider it as a valid
    // segment
    bool has_sequence = false;
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (strcmp(i->d_name, ".") == 0) continue;
        if (strcmp(i->d_name, "..") == 0) continue;
        if (strcmp(i->d_name, ".sequence") == 0)
        {
            has_sequence = true;
            continue;
        }
        return false;
    }
    return has_sequence;
}

template<typename Data>
size_t BaseChecker<Data>::size()
{
    std::string ext(".");
    ext += format_name(this->segment().format);
    size_t res = 0;
    sys::Path dir(this->segment().abspath);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (!i.isreg()) continue;
        if (!str::endswith(i->d_name, ext)) continue;
        struct stat st;
        i.path->fstatat(i->d_name, st);
        res += st.st_size;
    }
    return res;
}

template<typename Data>
void BaseChecker<Data>::move_data(const std::filesystem::path& new_root, const std::filesystem::path& new_relpath, const std::filesystem::path& new_abspath)
{
    std::filesystem::rename(this->segment().abspath.c_str(), new_abspath.c_str());
}

template<typename Data>
bool BaseChecker<Data>::rescan_data(std::function<void(const std::string&)> reporter, std::shared_ptr<core::Lock> lock, metadata_dest_func dest)
{
    Scanner scanner(this->segment());

    {
        SequenceFile sf(this->segment().abspath);
        sf.open();
        size_t cur_sequence = sf.read_sequence();

        scanner.list_files();

        if (cur_sequence < scanner.max_sequence)
        {
            stringstream out;
            out << "sequence file value set to " << scanner.max_sequence << " from old value " << cur_sequence << " earlier than files found on disk";
            reporter(out.str());
            sf.write_sequence(scanner.max_sequence);
        }
    }

    auto reader = this->data().reader(lock);
    return scanner.scan(reporter, static_pointer_cast<data::Reader>(reader), dest);
}

template<typename Data>
State BaseChecker<Data>::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    CheckBackend checker(this->segment(), reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

template<typename Data>
void BaseChecker<Data>::validate(Metadata& md, const scan::Validator& v)
{
    if (const types::source::Blob* blob = md.has_source_blob()) {
        if (blob->filename != this->segment().relpath)
            throw std::runtime_error("metadata to validate does not appear to be from this segment");

        filesystem::path fname = this->segment().abspath / SequenceFile::data_fname(blob->offset, blob->format);
        sys::File fd(fname, O_RDONLY);
        v.validate_file(fd, 0, blob->size);
        return;
    }
    const auto& data = md.get_data();
    auto buf = data.read();
    v.validate_buf(buf.data(), buf.size());  // TODO: add a validate_data that takes the metadata::Data
}

template<typename Data>
void BaseChecker<Data>::foreach_datafile(std::function<void(const char*)> f)
{
    sys::Path dir(this->segment().abspath);
    std::string ext(".");
    ext += format_name(this->segment().format);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (!i.isreg()) continue;
        if (strcmp(i->d_name, ".sequence") == 0) continue;
        if (!str::endswith(i->d_name, ext)) continue;
        f(i->d_name);
    }
}

template<typename Data>
size_t BaseChecker<Data>::remove()
{
    size_t size = 0;
    foreach_datafile([&](const char* name) {
            filesystem::path pathname = this->segment().abspath / name;
        size += sys::size(pathname);
        sys::unlink(pathname);
    });
    std::filesystem::remove(this->segment().abspath / ".sequence");
    std::filesystem::remove(this->segment().abspath / ".write-lock");
    std::filesystem::remove(this->segment().abspath / ".repack-lock");
    // Also remove the directory if it is empty
    rmdir(this->segment().abspath.c_str());
    return size;
}

template<typename Data>
core::Pending BaseChecker<Data>::repack(const std::filesystem::path& rootdir, metadata::Collection& mds, const RepackConfig& cfg)
{
    struct Rename : public Transaction
    {
        std::filesystem::path tmpabspath;
        std::filesystem::path abspath;
        std::filesystem::path tmppos;
        bool fired;

        Rename(const std::filesystem::path& tmpabspath, const std::filesystem::path& abspath)
            : tmpabspath(tmpabspath), abspath(abspath), tmppos(sys::with_suffix(abspath, ".pre-repack")), fired(false)
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
                throw_system_error("cannot rename "s + abspath.native() + " to " + tmppos.native());

            // Rename the data file to its final name
            if (rename(tmpabspath.c_str(), abspath.c_str()) < 0)
                throw_system_error("cannot rename "s + tmpabspath.native() + " to " + abspath.native()
                    + " (ATTENTION: please check if you need to rename " + tmppos.native() + " to " + abspath.native()
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

    filesystem::path tmprelpath = sys::with_suffix(this->segment().relpath, ".repack");
    filesystem::path tmpabspath = sys::with_suffix(this->segment().abspath, ".repack");

    core::Pending p(new Rename(tmpabspath, this->segment().abspath));

    Creator creator(this->segment(), mds, tmpabspath);
    creator.hardlink = true;
    creator.validator = &scan::Validator::by_format(this->segment().format);
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

template<typename Data>
void BaseChecker<Data>::test_truncate(size_t offset)
{
    utils::files::PreserveFileTimes pft(this->segment().abspath);

    // Truncate dir segment
    foreach_datafile([&](const char* name) {
        if (strtoul(name, 0, 10) >= offset)
            sys::unlink(this->segment().abspath / name);
    });
}

template<typename Data>
void BaseChecker<Data>::test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx)
{
    SequenceFile seqfile(this->segment().abspath);
    utils::files::PreserveFileTimes pf(seqfile.path());
    seqfile.open();
    size_t pos = seqfile.read_sequence();
    if (!seqfile.new_file)
        ++pos;
    if (data_idx >= mds.size())
    {
        for (unsigned i = 0; i < hole_size; ++i)
        {
            File fd(this->segment().abspath / SequenceFile::data_fname(pos, this->segment().format),
                O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL, 0666);
            fd.close();
            ++pos;
        }
    } else {
        for (int i = mds.size() - 1; i >= (int)data_idx; --i)
        {
            std::unique_ptr<source::Blob> source(mds[i].sourceBlob().clone());
            std::filesystem::rename(
                    source->absolutePathname() / SequenceFile::data_fname(source->offset, source->format),
                    source->absolutePathname() / SequenceFile::data_fname(source->offset + hole_size, source->format));
            source->offset += hole_size;
            mds[i].set_source(std::move(source));
        }
        pos += hole_size;
    }
    seqfile.write_sequence(pos - 1);
}

template<typename Data>
void BaseChecker<Data>::test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx)
{
    for (unsigned i = data_idx; i < mds.size(); ++i)
    {
        unique_ptr<source::Blob> source(mds[i].sourceBlob().clone());
        std::filesystem::rename(
                source->absolutePathname() / SequenceFile::data_fname(source->offset, source->format),
                source->absolutePathname() / SequenceFile::data_fname(source->offset - overlap_size, source->format));
        source->offset -= overlap_size;
        mds[i].set_source(std::move(source));
    }
}

template<typename Data>
void BaseChecker<Data>::test_corrupt(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    File fd(s.absolutePathname() / SequenceFile::data_fname(s.offset, s.format), O_WRONLY);
    fd.write_all_or_throw("\0", 1);
}


State HoleChecker::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    // Force quick, since the file contents are fake
    return BaseChecker::check(reporter, mds, true);
}

template class BaseWriter<Data>;
template class BaseChecker<Data>;


Scanner::Scanner(const Segment& segment)
    : segment(segment)
{
}

void Scanner::list_files()
{
    sys::Path dir(segment.abspath);
    std::string ext(".");
    ext += format_name(segment.format);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (!i.isreg()) continue;
        if (!str::endswith(i->d_name, ext)) continue;
        struct stat st;
        i.path->fstatat(i->d_name, st);
        size_t seq = (size_t)strtoul(i->d_name, 0, 10);
        on_disk.insert(make_pair(seq, ScannerData(i->d_name, st.st_size)));
        max_sequence = max(max_sequence, seq);
    }
}

bool Scanner::scan(std::shared_ptr<data::Reader> reader, metadata_dest_func dest)
{
    // Scan them one by one
    auto scanner = scan::Scanner::get_scanner(segment.format);
    for (const auto& fi : on_disk)
    {
        auto md = scanner->scan_singleton(segment.abspath / fi.second.fname);
        md->set_source(Source::createBlob(reader, fi.first, fi.second.size));
        if (!dest(md))
            return false;
    }

    return true;
}

bool Scanner::scan(std::function<void(const std::string&)> reporter, std::shared_ptr<data::Reader> reader, metadata_dest_func dest)
{
    // Scan them one by one
    auto scanner = scan::Scanner::get_scanner(segment.format);
    for (const auto& fi : on_disk)
    {
        std::shared_ptr<Metadata> md;
        try {
            md = scanner->scan_singleton(segment.abspath / fi.second.fname);
        } catch (std::exception& e) {
            stringstream out;
            out << "data file " << fi.second.fname << " fails to scan (" << e.what() << ")";
            reporter(out.str());
            continue;
        }
        md->set_source(Source::createBlob(reader, fi.first, fi.second.size));
        if (!dest(md))
            return false;
    }

    return true;
}

}
#include "base.tcc"
