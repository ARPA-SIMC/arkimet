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
#include "arki/utils/lock.h"
#include "arki/dataset/reporter.h"
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
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace segment {
namespace dir {

Writer::Writer(const std::string& format, const std::string& root, const std::string& relname, const std::string& absname)
    : segment::Writer(root, relname, absname), seqfile(absname), format(format)
{
    // Ensure that the directory 'absname' exists
    sys::makedirs(absname);

    seqfile.open();
}

size_t Writer::write_file(Metadata& md, File& fd)
{
    try {
        const std::vector<uint8_t>& buf = md.getData();

        size_t count = fd.pwrite(buf.data(), buf.size(), 0);
        if (count != buf.size())
            throw std::runtime_error(fd.name() + ": written only " + std::to_string(count) + "/" + std::to_string(buf.size()) + " bytes");

        if (fdatasync(fd) < 0)
            fd.throw_error("cannot flush write");

        return buf.size();
    } catch (...) {
        unlink(absname.c_str());
        throw;
    }
}

size_t HoleWriter::write_file(Metadata& md, File& fd)
{
    try {
        if (ftruncate(fd, md.data_size()) == -1)
            fd.throw_error("cannot set file size");

        return md.data_size();
    } catch (...) {
        unlink(absname.c_str());
        throw;
    }
}


namespace {

struct Append : public Transaction
{
    const dir::Writer& writer;
    bool fired;
    Metadata& md;
    size_t pos;
    size_t size;
    std::unique_ptr<types::source::Blob> new_source;

    Append(const dir::Writer& writer, Metadata& md, size_t pos, size_t size)
        : writer(writer), fired(false), md(md), pos(pos), size(size)
    {
        new_source = source::Blob::create_unlocked(md.source().format, writer.root, writer.relname, pos, size);
    }

    virtual ~Append()
    {
        if (!fired) rollback();
    }

    virtual void commit()
    {
        if (fired) return;

        // Set the source information that we are writing in the metadata
        md.set_source(move(new_source));

        fired = true;
    }

    virtual void rollback()
    {
        if (fired) return;

        // If we had a problem, remove the file that we have created. The
        // sequence will have skipped one number, but we do not need to
        // guarantee consecutive numbers, so it is just a cosmetic issue. This
        // case should be rare enough that even the cosmetic issue should
        // rarely be noticeable.
        string target_name = str::joinpath(writer.absname, SequenceFile::data_fname(pos, writer.format));
        unlink(target_name.c_str());

        fired = true;
    }
};

}

Pending Writer::append(Metadata& md, const types::source::Blob** new_source)
{
    size_t pos;
    File fd = seqfile.open_next(format, pos);
    size_t size = write_file(md, fd);
    fd.close();
    auto res = new Append(*this, md, pos, size);
    if (new_source) *new_source = res->new_source.get();
    return res;
}

void Writer::foreach_datafile(std::function<void(const char*)> f)
{
    sys::Path dir(absname);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (!i.isreg()) continue;
        if (strcmp(i->d_name, ".sequence") == 0) continue;
        if (!str::endswith(i->d_name, format)) continue;
        f(i->d_name);
    }
}

off_t Writer::link(const std::string& srcabsname)
{
    size_t pos;
    while (true)
    {
        pair<string, size_t> dest = seqfile.next(format);
        if (::link(srcabsname.c_str(), dest.first.c_str()) == 0)
        {
            pos = dest.second;
            break;
        }
        if (errno != EEXIST)
            throw_system_error("cannot link " + srcabsname + " as " + dest.first);
    }
    return pos;
}


Checker::Checker(const std::string& format, const std::string& root, const std::string& relname, const std::string& absname)
    : segment::Checker(root, relname, absname), format(format)
{
}

State Checker::check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick)
{
    size_t next_sequence_expected(0);
    const scan::Validator* validator(0);
    bool out_of_order(false);
    std::string format = utils::require_format(absname);

    if (!quick)
        validator = &scan::Validator::by_filename(absname.c_str());

    // Deal with segments that just do not exist
    if (!sys::exists(absname))
        return SEGMENT_UNALIGNED;

    // List the dir elements we know about
    map<size_t, size_t> expected;
    sys::Path dir(absname);
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
                reporter.segment_info(ds, relname, out.str());
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
            reporter.segment_info(ds, relname, ss.str());
            return SEGMENT_UNALIGNED;
        } else {
            if (source.size != ei->second)
            {
                stringstream ss;
                ss << "expected file " << source.offset << " has size " << ei->second << " instead of expected " << source.size;
                reporter.segment_info(ds, relname, ss.str());
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
            string fname = str::joinpath(absname, SequenceFile::data_fname(idx, format));
            metadata::Collection mds;
            try {
                scan::scan(fname, [&](unique_ptr<Metadata> md) {
                    mds.acquire(move(md));
                    return true;
                }, format);
            } catch (std::exception& e) {
                stringstream out;
                out << "scan of unexpected file failed at " << absname << ":" << idx << ": " << e.what();
                reporter.segment_info(ds, relname, out.str());
                res = SEGMENT_DIRTY;
                continue;
            }

            if (mds.size() == 0)
            {
                stringstream ss;
                ss << "file does not contain any " << format << " items";
                reporter.segment_info(ds, relname, ss.str());
                res = SEGMENT_DIRTY;
                continue;
            }

            if (mds.size() > 1)
            {
                stringstream ss;
                ss << "file contains " << mds.size() << " " << format << " items instead of 1";
                reporter.segment_info(ds, relname, ss.str());
                return SEGMENT_CORRUPTED;
            }
        }
        stringstream ss;
        ss << "found " << expected.size() << " file(s) that the index does now know about";
        reporter.segment_info(ds, relname, ss.str());
        return SEGMENT_DIRTY;
    }

    // Take note of files with holes
    if (out_of_order)
    {
        reporter.segment_info(ds, relname, "contains deleted data or data to be reordered");
        return SEGMENT_DIRTY;
    } else {
        return res;
    }
}

void Checker::validate(Metadata& md, const scan::Validator& v)
{
    if (const types::source::Blob* blob = md.has_source_blob()) {
        if (blob->filename != relname)
            throw std::runtime_error("metadata to validate does not appear to be from this segment");

        string fname = str::joinpath(absname, SequenceFile::data_fname(blob->offset, blob->format));
        sys::File fd(fname, O_RDONLY);
        v.validate_file(fd, 0, blob->size);
        return;
    }
    const auto& buf = md.getData();
    v.validate_buf(buf.data(), buf.size());
}

void Checker::foreach_datafile(std::function<void(const char*)> f)
{
    sys::Path dir(absname);
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
    std::string format = utils::require_format(absname);

    size_t size = 0;
    foreach_datafile([&](const char* name) {
        string pathname = str::joinpath(absname, name);
        size += sys::size(pathname);
        sys::unlink(pathname);
    });
    sys::unlink_ifexists(str::joinpath(absname, ".sequence"));
    sys::unlink_ifexists(str::joinpath(absname, ".repack-lock"));
    // Also remove the directory if it is empty
    rmdir(absname.c_str());
    return size;
}

Pending Checker::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    struct Rename : public Transaction
    {
        std::string tmpabsname;
        std::string absname;
        std::string tmppos;
        bool fired;
        sys::File repack_lock;
        Lock lock;

        Rename(const std::string& tmpabsname, const std::string& absname)
            : tmpabsname(tmpabsname), absname(absname), tmppos(absname + ".pre-repack"), fired(false), repack_lock(str::joinpath(absname, ".repack-lock"), O_RDWR | O_CREAT, 0777)
        {
            lock.l_type = F_WRLCK;
            lock.l_whence = SEEK_SET;
            lock.l_start = 0;
            lock.l_len = 0;
            lock.ofd_setlkw(repack_lock);
        }

        virtual ~Rename()
        {
            if (!fired) rollback();
        }

        virtual void commit()
        {
            if (fired) return;

            // It is impossible to make this atomic, so we just try to make it as quick as possible

            // Move the old directory inside the temp dir, to free the old directory name
            if (rename(absname.c_str(), tmppos.c_str()))
                throw_system_error("cannot rename " + absname + " to " + tmppos);

            // Rename the data file to its final name
            if (rename(tmpabsname.c_str(), absname.c_str()) < 0)
                throw_system_error("cannot rename " + tmpabsname + " to " + absname
                    + " (ATTENTION: please check if you need to rename " + tmppos + " to " + absname
                    + " manually to restore the dataset as it was before the repack)");

            // Remove the old data
            sys::rmtree(tmppos);

            // Release the lock
            lock.l_type = F_UNLCK;
            lock.ofd_setlk(repack_lock);

            fired = true;
        }

        virtual void rollback()
        {
            if (fired) return;

            try
            {
                sys::rmtree(tmpabsname);
            } catch (std::exception& e) {
                nag::warning("Failed to remove %s while recovering from a failed repack: %s", tmpabsname.c_str(), e.what());
            }

            rename(tmppos.c_str(), absname.c_str());

            // Release the lock
            lock.l_type = F_UNLCK;
            lock.ofd_setlk(repack_lock);

            fired = true;
        }
    };

    string tmprelname = relname + ".repack";
    string tmpabsname = absname + ".repack";

    // Make sure mds are not holding a read lock on the file to repack
    for (auto& md: mds) md->sourceBlob().unlock();

    // Reacquire the lock here for writing
    Rename* rename;
    Pending p(rename = new Rename(tmpabsname, absname));

    // Create a writer for the temp dir
    auto writer(make_tmp_segment(format, tmprelname, tmpabsname));

    if (test_flags & TEST_MISCHIEF_MOVE_DATA)
        writer->seqfile.test_add_padding(1);

    // Fill the temp file with all the data in the right order
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        const source::Blob& source = (*i)->sourceBlob();

        // Make a hardlink in the target directory for the file pointed by *i
        off_t pos = writer->link(str::joinpath(source.absolutePathname(), SequenceFile::data_fname(source.offset, source.format)));

        // Update the source information in the metadata
        (*i)->set_source(Source::createBlobUnlocked(source.format, rootdir, relname, pos, source.size));
    }

    // Close the temp writer
    writer.reset();

    return p;
}

unique_ptr<dir::Writer> Checker::make_tmp_segment(const std::string& format, const std::string& relname, const std::string& absname)
{
    if (sys::exists(absname)) sys::rmtree(absname);
    unique_ptr<dir::Writer> res(new dir::Writer(format, root, relname, absname));
    return res;
}

void Checker::test_truncate(size_t offset)
{
    utils::files::PreserveFileTimes pft(absname);

    // Truncate dir segment
    string format = utils::require_format(absname);
    foreach_datafile([&](const char* name) {
        if (strtoul(name, 0, 10) >= offset)
            sys::unlink(str::joinpath(absname, name));
    });
}

void Checker::test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx)
{
    if (data_idx >= mds.size())
    {
        SequenceFile seqfile(absname);
        seqfile.open();
        sys::PreserveFileTimes pf(seqfile.fd);
        size_t pos;
        for (unsigned i = 0; i < hole_size; ++i)
        {
            File fd = seqfile.open_next(mds[0].sourceBlob().format, pos);
            fd.close();
        }
    } else {
        for (int i = mds.size() - 1; i >= (int)data_idx; --i)
        {
            unique_ptr<source::Blob> source(mds[i].sourceBlob().clone());
            sys::rename(
                    str::joinpath(source->absolutePathname(), SequenceFile::data_fname(source->offset, source->format)),
                    str::joinpath(source->absolutePathname(), SequenceFile::data_fname(source->offset + hole_size, source->format)));
            source->offset += hole_size;
            mds[i].set_source(move(source));
        }
        // TODO: update seqfile to be mds.size() + hole_size
    }
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
        mds[i].set_source(move(source));
    }
}

void Checker::test_corrupt(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    File fd(str::joinpath(s.absolutePathname(), SequenceFile::data_fname(s.offset, s.format)), O_WRONLY);
    fd.write_all_or_throw("\0", 1);
}

State HoleChecker::check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick)
{
    // Force quick, since the file contents are fake
    return Checker::check(reporter, ds, mds, true);
}

unique_ptr<dir::Writer> HoleChecker::make_tmp_segment(const std::string& format, const std::string& relname, const std::string& absname)
{
    if (sys::exists(absname)) sys::rmtree(absname);
    unique_ptr<dir::Writer> res(new dir::HoleWriter(format, root, relname, absname));
    return res;
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
}
