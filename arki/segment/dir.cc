#include "dir.h"
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
#include "arki/dataset/reporter.h"
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
    write_file(md, fd);
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
    try {
        const std::vector<uint8_t>& buf = md.getData();

        size_t count = fd.pwrite(buf.data(), buf.size(), 0);
        if (count != buf.size())
            throw std::runtime_error(fd.name() + ": written only " + std::to_string(count) + "/" + std::to_string(buf.size()) + " bytes");

        if (fdatasync(fd) < 0)
            fd.throw_error("cannot flush write");
    } catch (...) {
        unlink(abspath.c_str());
        throw;
    }
}

const char* HoleWriter::type() const { return "hole_dir"; }

void HoleWriter::write_file(Metadata& md, NamedFileDescriptor& fd)
{
    try {
        if (ftruncate(fd, md.data_size()) == -1)
            fd.throw_error("cannot set file size");
    } catch (...) {
        unlink(abspath.c_str());
        throw;
    }
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

State Checker::check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick)
{
    size_t next_sequence_expected(0);
    const scan::Validator* validator(0);
    bool out_of_order(false);
    std::string format = utils::require_format(abspath);

    if (!quick)
        validator = &scan::Validator::by_filename(abspath.c_str());

    // Deal with segments that just do not exist
    if (!sys::exists(abspath))
        return SEGMENT_UNALIGNED;

    // List the dir elements we know about
    map<size_t, size_t> expected;
    sys::Path dir(abspath);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (!i.isreg()) continue;
        if (!str::endswith(i->d_name, format)) continue;
        struct stat st;
        i.path->fstatat(i->d_name, st);
        expected.insert(make_pair(
                    (size_t)strtoul(i->d_name, 0, 10),
                    st.st_size));
    }

    // Check the list of elements we expect for sort order, gaps, and match
    // with what is in the file system
    for (const auto& i: mds)
    {
        if (validator)
        {
            try {
                validate(*i, *validator);
            } catch (std::exception& e) {
                stringstream out;
                out << "validation failed at " << i->source() << ": " << e.what();
                reporter.segment_info(ds, relpath, out.str());
                return SEGMENT_UNALIGNED;
            }
        }

        const source::Blob& source = i->sourceBlob();

        if (source.offset != next_sequence_expected)
            out_of_order = true;

        auto ei = expected.find(source.offset);
        if (ei == expected.end())
        {
            stringstream ss;
            ss << "expected file " << source.offset << " not found in the file system";
            reporter.segment_info(ds, relpath, ss.str());
            return SEGMENT_UNALIGNED;
        } else {
            if (source.size != ei->second)
            {
                stringstream ss;
                ss << "expected file " << source.offset << " has size " << ei->second << " instead of expected " << source.size;
                reporter.segment_info(ds, relpath, ss.str());
                return SEGMENT_CORRUPTED;
            }
            expected.erase(ei);
        }

        ++next_sequence_expected;
    }

    State res = SEGMENT_OK;

    for (const auto& ei: expected)
    {
        auto idx = ei.first;
        if (validator)
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
                out << "scan of unexpected file failed at " << abspath << ":" << idx << ": " << e.what();
                reporter.segment_info(ds, relpath, out.str());
                res = SEGMENT_DIRTY;
                continue;
            }

            if (mds.size() == 0)
            {
                stringstream ss;
                ss << "file does not contain any " << format << " items";
                reporter.segment_info(ds, relpath, ss.str());
                res = SEGMENT_DIRTY;
                continue;
            }

            if (mds.size() > 1)
            {
                stringstream ss;
                ss << "file contains " << mds.size() << " " << format << " items instead of 1";
                reporter.segment_info(ds, relpath, ss.str());
                return SEGMENT_CORRUPTED;
            }
        }
        stringstream ss;
        ss << "found " << expected.size() << " file(s) that the index does now know about";
        reporter.segment_info(ds, relpath, ss.str());
        return SEGMENT_DIRTY;
    }

    // Take note of files with holes
    if (out_of_order)
    {
        reporter.segment_info(ds, relpath, "contains deleted data or data to be reordered");
        return SEGMENT_DIRTY;
    } else {
        return res;
    }
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

    // Make sure mds are not holding a read lock on the file to repack
    for (auto& md: mds) md->sourceBlob().unlock();

    // Reacquire the lock here for writing
    Rename* rename;
    Pending p(rename = new Rename(tmpabspath, abspath));

    // Create a writer for the temp dir
    sys::makedirs(tmpabspath);
    size_t pos = 0;

    if (test_flags & TEST_MISCHIEF_MOVE_DATA)
        ++pos;

    // Fill the temp file with all the data in the right order, using hard links
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        const source::Blob& source = (*i)->sourceBlob();
        auto new_source = Source::createBlobUnlocked(source.format, rootdir, relpath, pos, source.size);

        std::string src = str::joinpath(source.absolutePathname(), SequenceFile::data_fname(source.offset, format));
        std::string dst = str::joinpath(tmpabspath, SequenceFile::data_fname(pos, format));

        if (::link(src.c_str(), dst.c_str()) != 0)
            throw_system_error("cannot link " + src + " as " + dst);

        // Update the source information in the metadata
        (*i)->set_source(Source::createBlobUnlocked(source.format, rootdir, relpath, pos, source.size));

        ++pos;
    }

    // Write it out
    SequenceFile seqfile(tmpabspath);
    seqfile.open();
    seqfile.write_sequence(pos);

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

const char* HoleChecker::type() const { return "hole_dir"; }

State HoleChecker::check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick)
{
    // Force quick, since the file contents are fake
    return Checker::check(reporter, ds, mds, true);
}

bool can_store(const std::string& format)
{
    return format == "grib" || format == "grib1" || format == "grib2"
        || format == "bufr"
        || format == "odimh5" || format == "h5" || format == "odim"
        || format == "vm2";
}

}
}
}
