#include "fd.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/any.h"
#include "arki/utils/compress.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils.h"
#include "arki/nag.h"
#include <algorithm>
#include <system_error>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>

using namespace std;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace segment {
namespace fd {

void File::fdtruncate_nothrow(off_t pos) noexcept
{
    if (::ftruncate(*this, pos) == -1)
        nag::warning("truncating %s to previous size %zd (rollback of append operation): %m", name().c_str(), pos);
}

Creator::Creator(const std::string& root, const std::string& relpath, const std::string& abspath, metadata::Collection& mds)
    : AppendCreator(root, relpath, abspath, mds)
{
}

Creator::~Creator()
{
    delete out;
}

void Creator::create()
{
    if (!out->is_open())
        out->open(O_WRONLY | O_CREAT | O_TRUNC, 0666);
    AppendCreator::create();
    out->fdatasync();
    out->close();
}

size_t Creator::append(const std::vector<uint8_t>& data)
{
    size_t wrpos = written;
    written += out->write_data(data);
    return wrpos;
}


CheckBackend::CheckBackend(const std::string& abspath, const std::string& relpath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
    : AppendCheckBackend(reporter, relpath, mds), data(abspath)
{
}

size_t CheckBackend::offset_end() const { return st.st_size; }

void CheckBackend::validate(Metadata& md, const types::source::Blob& source)
{
    validator->validate_file(data, source.offset, source.size);
}

State CheckBackend::check()
{
    if (!data.open_ifexists(O_RDONLY))
    {
        reporter(data.name() + " not found on disk");
        return SEGMENT_DELETED;
    }
    data.fstat(st);
    return AppendCheckBackend::check();
}


Writer::Writer(const std::string& root, const std::string& relpath, std::unique_ptr<File> fd)
    : segment::Writer(root, relpath, fd->name()), fd(fd.release())
{
    initial_size = this->fd->lseek(0, SEEK_END);
    current_pos = initial_size;
}

Writer::~Writer()
{
    if (!fired) rollback_nothrow();
    delete fd;
}

bool Writer::single_file() const { return true; }

size_t Writer::next_offset() const { return current_pos; }

const types::source::Blob& Writer::append(Metadata& md)
{
    fired = false;
    const std::vector<uint8_t>& buf = md.getData();
    pending.emplace_back(md, source::Blob::create_unlocked(md.source().format, root, relpath, current_pos, buf.size()));
    current_pos += fd->write_data(buf);
    return *pending.back().new_source;
}

void Writer::commit()
{
    if (fired) return;
    fd->fsync();
    for (auto& p: pending)
        p.set_source();
    pending.clear();
    initial_size = current_pos;
    fired = true;
}

void Writer::rollback()
{
    if (fired) return;
    fd->ftruncate(initial_size);
    fd->lseek(initial_size, SEEK_SET);
    current_pos = initial_size;
    pending.clear();
    fired = true;
}

void Writer::rollback_nothrow() noexcept
{
    if (fired) return;
    fd->fdtruncate_nothrow(initial_size);
    ::lseek(*fd, initial_size, SEEK_SET);
    pending.clear();
    fired = true;
}


bool Checker::single_file() const { return true; }

bool Checker::exists_on_disk()
{
    std::unique_ptr<struct stat> st = sys::stat(abspath);
    if (!st) return false;
    if (S_ISDIR(st->st_mode)) return false;
    return true;
}

time_t Checker::timestamp()
{
    struct stat st;
    sys::stat(abspath, st);
    return st.st_mtime;
}

size_t Checker::size()
{
    struct stat st;
    sys::stat(abspath, st);
    return st.st_size;
}

void Checker::move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath)
{
    sys::rename(abspath, new_abspath);
}

size_t Checker::remove()
{
    size_t size = sys::size(abspath);
    sys::unlink(abspath.c_str());
    return size;
}

Pending Checker::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    string tmpabspath = abspath + ".repack";

    Pending p(new files::RenameTransaction(tmpabspath, abspath));

    Creator creator(rootdir, relpath, abspath, mds);
    creator.out = open_file(tmpabspath, O_WRONLY | O_CREAT | O_TRUNC, 0666).release();
    creator.validator = &scan::Validator::by_filename(abspath);
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

void Checker::test_truncate(size_t offset)
{
    if (!sys::exists(abspath))
        utils::createFlagfile(abspath);

    utils::files::PreserveFileTimes pft(abspath);
    if (::truncate(abspath.c_str(), offset) < 0)
    {
        stringstream ss;
        ss << "cannot truncate " << abspath << " at " << offset;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
}

void Checker::test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx)
{
    sys::File fd(abspath, O_RDWR);
    sys::PreserveFileTimes pt(fd);
    off_t end = fd.lseek(0, SEEK_END);
    if (data_idx >= mds.size())
    {
        fd.ftruncate(end + hole_size);
    } else {
        off_t start_ofs = mds[data_idx].sourceBlob().offset;
        std::vector<uint8_t> buf(end - start_ofs);
        fd.lseek(start_ofs);
        fd.read_all_or_throw(buf.data(), buf.size());
        fd.lseek(start_ofs + hole_size);
        fd.write_all_or_throw(buf.data(), buf.size());

        for (unsigned i = data_idx; i < mds.size(); ++i)
        {
            unique_ptr<source::Blob> source(mds[i].sourceBlob().clone());
            source->offset += hole_size;
            mds[i].set_source(std::move(source));
        }
    }
}

void Checker::test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx)
{
    sys::File fd(abspath, O_RDWR);
    sys::PreserveFileTimes pt(fd);
    off_t start_ofs = mds[data_idx].sourceBlob().offset;
    off_t end = fd.lseek(0, SEEK_END);
    std::vector<uint8_t> buf(end - start_ofs);
    fd.lseek(start_ofs);
    fd.read_all_or_throw(buf.data(), buf.size());
    fd.lseek(start_ofs - overlap_size);
    fd.write_all_or_throw(buf.data(), buf.size());
    fd.ftruncate(end - overlap_size);

    for (unsigned i = data_idx; i < mds.size(); ++i)
    {
        unique_ptr<source::Blob> source(mds[i].sourceBlob().clone());
        source->offset -= overlap_size;
        mds[i].set_source(std::move(source));
    }
}

void Checker::test_corrupt(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    sys::File fd(abspath, O_RDWR);
    sys::PreserveFileTimes pt(fd);
    fd.lseek(s.offset);
    fd.write_all_or_throw("\0", 1);
}


bool can_store(const std::string& format)
{
    return format == "grib" || format == "grib1" || format == "grib2"
        || format == "bufr" || format == "vm2";
}

}
}
}
