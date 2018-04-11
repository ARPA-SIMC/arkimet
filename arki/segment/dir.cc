#include "dir.h"
#include "common.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/scan/any.h"
#include "arki/utils/string.h"
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
            auto buf = md.getData();
            res.size = buf.size();
            if (validator)
                validator->validate_buf(buf.data(), buf.size());

            sys::File fd(str::joinpath(dest_abspath, SequenceFile::data_fname(current_pos, format)),
                        O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL, 0666);
            try {
                size_t count = fd.pwrite(buf.data(), buf.size(), 0);
                if (count != buf.size())
                    throw std::runtime_error(fd.name() + ": written only " + std::to_string(count) + "/" + std::to_string(buf.size()) + " bytes");

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
        if (source.size != si->second)
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
            auto idx = di.first;
            if (accurate)
            {
                string fname = str::joinpath(abspath, SequenceFile::data_fname(idx, format));
                metadata::Collection mds;
                try {
                    scan::scan(fname, std::make_shared<core::lock::Null>(), format, [&](unique_ptr<Metadata> md) {
                        mds.acquire(std::move(md));
                        return true;
                    });
                } catch (std::exception& e) {
                    stringstream out;
                    out << "unexpected data file " << idx << " fails to scan (" << e.what() << ")";
                    reporter(out.str());
                    dirty = true;
                    continue;
                }

                if (mds.size() == 0)
                {
                    stringstream ss;
                    ss << "unexpected data file " << idx << " does not contain any " << format << " items";
                    reporter(ss.str());
                    dirty = true;
                    continue;
                }

                if (mds.size() > 1)
                {
                    stringstream ss;
                    ss << "unexpected data file " << idx << " contains " << mds.size() << " " << format << " items instead of 1";
                    reporter(ss.str());
                    return SEGMENT_CORRUPTED;
                }
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


Writer::Writer(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath)
    : segment::Writer(root, relpath, abspath), seqfile(abspath), format(format)
{
    // Ensure that the directory 'abspath' exists
    sys::makedirs(abspath);
    seqfile.open();
    current_pos = seqfile.read_sequence();
}

Writer::~Writer()
{
}

const char* Writer::type() const { return "dir"; }
bool Writer::single_file() const { return false; }

size_t Writer::next_offset() const
{
    return current_pos;
}

const types::source::Blob& Writer::append(Metadata& md)
{
    fired = false;

    File fd(str::joinpath(abspath, SequenceFile::data_fname(current_pos, format)),
                O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL, 0666);
    try {
        write_file(md, fd);
    } catch (...) {
        unlink(fd.name().c_str());
        throw;
    }
    written.push_back(fd.name());
    fd.close();
    pending.emplace_back(md, source::Blob::create_unlocked(md.source().format, root, relpath, current_pos, md.data_size()));
    ++current_pos;
    return *pending.back().new_source;
}

void Writer::commit()
{
    if (fired) return;
    seqfile.write_sequence(current_pos);
    for (auto& p: pending)
        p.set_source();
    pending.clear();
    written.clear();
    fired = true;
}

void Writer::rollback()
{
    if (fired) return;
    for (auto fn: written)
        sys::unlink(fn);
    pending.clear();
    written.clear();
    fired = true;
}

void Writer::rollback_nothrow() noexcept
{
    if (fired) return;
    for (auto fn: written)
        ::unlink(fn.c_str());
    pending.clear();
    written.clear();
    fired = true;
}

void Writer::write_file(Metadata& md, NamedFileDescriptor& fd)
{
    const std::vector<uint8_t>& buf = md.getData();

    size_t count = fd.pwrite(buf.data(), buf.size(), 0);
    if (count != buf.size())
        throw std::runtime_error(fd.name() + ": written only " + std::to_string(count) + "/" + std::to_string(buf.size()) + " bytes");

    if (fdatasync(fd) < 0)
        fd.throw_error("cannot flush write");
}

const char* HoleWriter::type() const { return "hole_dir"; }

void HoleWriter::write_file(Metadata& md, NamedFileDescriptor& fd)
{
    if (ftruncate(fd, md.data_size()) == -1)
        fd.throw_error("cannot set file size");
}


Checker::Checker(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath)
    : segment::Checker(root, relpath, abspath), format(format)
{
}

const char* Checker::type() const { return "dir"; }
bool Checker::single_file() const { return false; }

bool Checker::exists_on_disk()
{
    if (!sys::isdir(abspath)) return false;
    return sys::exists(str::joinpath(abspath, ".sequence"));
}

time_t Checker::timestamp()
{
    return sys::timestamp(str::joinpath(abspath, ".sequence"));
}

size_t Checker::size()
{
    size_t res = 0;
    sys::Path dir(abspath);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (!i.isreg()) continue;
        if (!str::endswith(i->d_name, format)) continue;
        struct stat st;
        i.path->fstatat(i->d_name, st);
        res += st.st_size;
    }
    return res;
}

void Checker::move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath)
{
    sys::rename(abspath.c_str(), new_abspath.c_str());
}

State Checker::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    CheckBackend checker(format, abspath, relpath, reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

void Checker::validate(Metadata& md, const scan::Validator& v)
{
    if (const types::source::Blob* blob = md.has_source_blob()) {
        if (blob->filename != relpath)
            throw std::runtime_error("metadata to validate does not appear to be from this segment");

        string fname = str::joinpath(abspath, SequenceFile::data_fname(blob->offset, blob->format));
        sys::File fd(fname, O_RDONLY);
        v.validate_file(fd, 0, blob->size);
        return;
    }
    const auto& buf = md.getData();
    v.validate_buf(buf.data(), buf.size());
}

void Checker::foreach_datafile(std::function<void(const char*)> f)
{
    sys::Path dir(abspath);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (!i.isreg()) continue;
        if (strcmp(i->d_name, ".sequence") == 0) continue;
        if (!str::endswith(i->d_name, format)) continue;
        f(i->d_name);
    }
}

size_t Checker::remove()
{
    std::string format = utils::require_format(abspath);

    size_t size = 0;
    foreach_datafile([&](const char* name) {
        string pathname = str::joinpath(abspath, name);
        size += sys::size(pathname);
        sys::unlink(pathname);
    });
    sys::unlink_ifexists(str::joinpath(abspath, ".sequence"));
    sys::unlink_ifexists(str::joinpath(abspath, ".write-lock"));
    sys::unlink_ifexists(str::joinpath(abspath, ".repack-lock"));
    // Also remove the directory if it is empty
    rmdir(abspath.c_str());
    return size;
}

Pending Checker::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
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

    string tmprelpath = relpath + ".repack";
    string tmpabspath = abspath + ".repack";

    Pending p(new Rename(tmpabspath, abspath));

    Creator creator(rootdir, relpath, mds, tmpabspath);
    creator.hardlink = true;
    creator.validator = &scan::Validator::by_filename(abspath);
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

void Checker::test_truncate(size_t offset)
{
    utils::files::PreserveFileTimes pft(abspath);

    // Truncate dir segment
    string format = utils::require_format(abspath);
    foreach_datafile([&](const char* name) {
        if (strtoul(name, 0, 10) >= offset)
            sys::unlink(str::joinpath(abspath, name));
    });
}

void Checker::test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx)
{
    SequenceFile seqfile(abspath);
    seqfile.open();
    sys::PreserveFileTimes pf(seqfile);
    size_t pos = seqfile.read_sequence();
    if (data_idx >= mds.size())
    {
        for (unsigned i = 0; i < hole_size; ++i)
        {
            File fd(str::joinpath(abspath, SequenceFile::data_fname(pos, format)),
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

void Checker::test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx)
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

void Checker::test_corrupt(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    File fd(str::joinpath(s.absolutePathname(), SequenceFile::data_fname(s.offset, s.format)), O_WRONLY);
    fd.write_all_or_throw("\0", 1);
}

std::shared_ptr<Checker> Checker::create(const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags)
{
    Creator creator(rootdir, relpath, mds, abspath);
    creator.create();
    return make_shared<Checker>(creator.format, rootdir, relpath, abspath);
}

const char* HoleChecker::type() const { return "hole_dir"; }

State HoleChecker::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    // Force quick, since the file contents are fake
    return Checker::check(reporter, mds, true);
}

bool Checker::can_store(const std::string& format)
{
    return format == "grib" || format == "grib1" || format == "grib2"
        || format == "bufr"
        || format == "odimh5" || format == "h5" || format == "odim"
        || format == "vm2";
}

}
}
}