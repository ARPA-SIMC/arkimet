#include "zip.h"
#include "common.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/archive.h"
#include "arki/types/source/blob.h"
#include "arki/scan/validator.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/zip.h"
#include "arki/scan.h"
#include "arki/utils/accounting.h"
#include "arki/iotrace.h"
#include <fcntl.h>
#include <vector>
#include <algorithm>
#include <map>
#include <sys/uio.h>
#include <system_error>
#include <sstream>

using namespace std;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;

namespace arki::segment::data::zip {

namespace {

struct Creator : public AppendCreator
{
protected:
    std::shared_ptr<File> out;
    std::shared_ptr<metadata::ArchiveOutput> zipout;

public:
    size_t idx = 0;
    char fname[100];

    Creator(const Segment& segment, metadata::Collection& mds, const std::filesystem::path& dest_abspath)
        : AppendCreator(segment, mds),
          out(std::make_shared<File>(dest_abspath, O_WRONLY | O_CREAT | O_TRUNC, 0666)),
          zipout(metadata::ArchiveOutput::create_file("zip", out))
    {
        zipout->set_subdir(std::string());
    }

#if 0
    std::unique_ptr<types::Source> create_source(const Metadata& md, const Span& span) override
    {
        return types::Source::createBlobUnlocked(md.source().format, root, relpath + ".zip", span.offset, span.size);
    }
#endif

    Span append_md(Metadata& md) override
    {
        Span res;
        res.offset = zipout->append(md);
        res.size = md.get_data().size();
        return res;
    }

    void create()
    {
        AppendCreator::create();
        zipout->flush(false);
        out->fdatasync();
        out->close();
    }
};

struct CheckBackend : public AppendCheckBackend
{
    ZipReader reader;
    std::unique_ptr<struct stat> st;
    // File size by offset
    map<size_t, size_t> on_disk;
    size_t max_sequence = 0;

    CheckBackend(const Segment& segment, const std::filesystem::path& zipabspath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
        : AppendCheckBackend(reporter, segment, mds), reader(segment.format, core::File(zipabspath, O_RDONLY))
    {
    }

    void validate(Metadata& md, const types::source::Blob& source) override
    {
        std::vector<uint8_t> buf = reader.get(Span(source.offset, source.size));
        validator->validate_buf(buf.data(), buf.size());
    }

    size_t actual_start(off_t offset, size_t size) const override { return offset - 1; }
    size_t actual_end(off_t offset, size_t size) const override { return offset; }
    size_t offset_end() const override { return max_sequence; }
    size_t compute_unindexed_space(const std::vector<Span>& indexed_spans) const override
    {
        // When this is called, all elements found in the index have already
        // been removed from scanner. We can just then add up what's left of
        // sizes in scanner
        size_t res = 0;
        for (const auto& i: on_disk)
            res += i.second;
        return res;
    }

    State check_source(const types::source::Blob& source) override
    {
        auto si = on_disk.find(source.offset);
        if (si == on_disk.end())
        {
            stringstream ss;
            ss << "expected file " << source.offset << " not found in the zip archive";
            reporter(ss.str());
            return State(SEGMENT_CORRUPTED);
        }
        if (source.size != si->second)
        {
            stringstream ss;
            ss << "expected file " << source.offset << " has size " << si->second << " instead of expected " << source.size;
            reporter(ss.str());
            return State(SEGMENT_CORRUPTED);
        }
        on_disk.erase(si);
        return State(SEGMENT_OK);
    }

    State check()
    {
/*
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
*/

        std::vector<segment::Span> spans = reader.list_data();
        for (const auto& span: spans)
        {
            on_disk.insert(make_pair(span.offset, span.size));
            max_sequence = max(max_sequence, span.offset);
        }

        bool dirty = false;
        State state = AppendCheckBackend::check();
        if (!state.is_ok())
        {
            if (state.has(SEGMENT_DIRTY))
                dirty = true;
            else
                return state;
        }

        // Check files on disk that were not accounted for
        // TODO: we need to implement scanning from a memory buffer, or write
        // the decompressed data to a temp file for scanning
        /*
        for (const auto& di: on_disk)
        {
            auto idx = di.first;
            if (accurate)
            {
                string fname = str::joinpath(abspath, ZipReader::data_fname(idx, format));
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
        */

        if (!on_disk.empty())
        {
            stringstream ss;
            ss << "segment contains " << on_disk.size() << " file(s) that the index does now know about";
            reporter(ss.str());
            dirty = true;
        }

        return State(dirty ? SEGMENT_DIRTY : SEGMENT_OK);
    }
};

}

const char* Data::type() const { return "zip"; }
bool Data::single_file() const { return true; }
time_t Data::timestamp() const { return sys::timestamp(sys::with_suffix(segment().abspath, ".zip")); }

std::shared_ptr<data::Reader> Data::reader(std::shared_ptr<core::Lock> lock) const
{
    return make_shared<Reader>(static_pointer_cast<const Data>(shared_from_this()), lock);
}
std::shared_ptr<data::Writer> Data::writer(const data::WriterConfig& config, bool mock_data) const
{
    throw std::runtime_error(std::string(type()) + " writing is not yet implemented");
    // return std::make_shared<Writer>(config, static_pointer_cast<const Data>(shared_from_this()));
}
std::shared_ptr<data::Checker> Data::checker(bool mock_data) const
{
    return make_shared<Checker>(static_pointer_cast<const Data>(shared_from_this()));
}
bool Data::can_store(DataFormat format)
{
    return true;
}
std::shared_ptr<data::Checker> Data::create(const Segment& segment, metadata::Collection& mds, const RepackConfig& cfg)
{
    Creator creator(segment, mds, sys::with_suffix(segment.abspath, ".zip"));
    creator.create();
    auto data = std::make_shared<const Data>(segment.shared_from_this());
    return make_shared<Checker>(data);
}



Reader::Reader(shared_ptr<const Data> data, std::shared_ptr<core::Lock> lock)
    : data::BaseReader<Data>(data, lock), zip(segment().format, core::File(sys::with_suffix(segment().abspath, ".zip"), O_RDONLY | O_CLOEXEC))
{
}

bool Reader::scan_data(metadata_dest_func dest)
{
    // Collect all file names in the directory
    std::vector<Span> spans = zip.list_data();

    // Sort them numerically
    std::sort(spans.begin(), spans.end());

    // Scan them one by one
    auto scanner = scan::Scanner::get_scanner(segment().format);
    for (const auto& span : spans)
    {
        std::vector<uint8_t> data = zip.get(span);
        auto md = scanner->scan_data(data);
        md->set_source(Source::createBlob(shared_from_this(), span.offset, span.size));
        if (!dest(move(md)))
            return false;
    }

    return true;
}

std::vector<uint8_t> Reader::read(const types::source::Blob& src)
{
    vector<uint8_t> buf = zip.get(Span(src.offset, src.size));
    acct::plain_data_read_count.incr();
    iotrace::trace_file(zip.zipname, src.offset, src.size, "read data");
    return buf;
}


/*
 * Checker
 */

Checker::Checker(std::shared_ptr<const Data> data)
    : BaseChecker<Data>(data), zipabspath(sys::with_suffix(segment().abspath, ".zip"))
{
}

bool Checker::exists_on_disk()
{
    return std::filesystem::exists(zipabspath);
}

bool Checker::is_empty()
{
    utils::ZipReader zip(segment().format, core::File(zipabspath, O_RDONLY | O_CLOEXEC));
    return zip.list_data().empty();
}

size_t Checker::size()
{
    return sys::size(zipabspath);
}

void Checker::move_data(const std::filesystem::path& new_root, const std::filesystem::path& new_relpath, const std::filesystem::path& new_abspath)
{
    auto new_zipabspath = sys::with_suffix(new_abspath, ".zip");
    if (rename(zipabspath.c_str(), new_zipabspath.c_str()) < 0)
    {
        stringstream ss;
        ss << "cannot rename " << zipabspath << " to " << new_zipabspath;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
}

bool Checker::rescan_data(std::function<void(const std::string&)> reporter, std::shared_ptr<core::Lock> lock, metadata_dest_func dest)
{
    auto reader = this->data().reader(lock);
    return reader->scan_data(dest);
}

State Checker::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    CheckBackend checker(segment(), zipabspath, reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

void Checker::validate(Metadata& md, const scan::Validator& v)
{
    if (const types::source::Blob* blob = md.has_source_blob())
    {
        if (blob->filename != segment().relpath)
            throw std::runtime_error("metadata to validate does not appear to be from this segment");

        sys::File fd(zipabspath, O_RDONLY);
        v.validate_file(fd, blob->offset, blob->size);
        return;
    }
    const auto& data = md.get_data();
    auto buf = data.read();
    v.validate_buf(buf.data(), buf.size());  // TODO: add a validate_data that takes the metadata::Data
}

size_t Checker::remove()
{
    size_t size = sys::size(zipabspath);
    sys::unlink(zipabspath);
    return size;
}

core::Pending Checker::repack(const std::filesystem::path& rootdir, metadata::Collection& mds, const RepackConfig& cfg)
{
    auto tmpabspath = sys::with_suffix(segment().abspath, ".repack");

    core::Pending p(new files::RenameTransaction(tmpabspath, zipabspath));

    Creator creator(segment(), mds, tmpabspath);
    creator.validator = &scan::Validator::by_filename(segment().abspath);

    creator.create();


    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

void Checker::test_truncate(size_t offset)
{
    // Zip file indices are 1-based, and we are passed an actual index from a metadata
    utils::files::PreserveFileTimes pft(zipabspath);
    if (offset == 1)
    {
        static const char empty_zip_data[] = "PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        sys::File out(zipabspath, O_WRONLY | O_CREAT | O_TRUNC);
        out.write_all_or_throw(empty_zip_data, sizeof(empty_zip_data));
    } else {
        utils::ZipWriter zip(segment().format, zipabspath);
        std::vector<segment::Span> spans = zip.list_data();
        for (const auto& span: spans)
        {
            if (span.offset < offset) continue;
            zip.remove(span);
        }
        zip.close();
    }
}

void Checker::test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx)
{
    utils::files::PreserveFileTimes pf(zipabspath);

    utils::ZipWriter zip(segment().format, zipabspath);

    if (data_idx >= mds.size())
    {
        std::vector<segment::Span> spans = zip.list_data();
        size_t pos = spans.back().offset + 1;
        for (unsigned i = 0; i < hole_size; ++i)
        {
            zip.write(Span(pos + 1, 0), std::vector<uint8_t>());
        }
    } else {
        for (int i = mds.size() - 1; i >= (int)data_idx; --i)
        {
            unique_ptr<source::Blob> source(mds[i].sourceBlob().clone());
            zip.rename(Span(source->offset, source->size), Span(source->offset + hole_size, source->size));
            source->offset += hole_size;
            mds[i].set_source(std::move(source));
        }
    }
    zip.close();
}

void Checker::test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx)
{
    throw std::runtime_error("test_make_overlap not implemented");
}

void Checker::test_corrupt(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    Span span(s.offset, s.size);

    utils::files::PreserveFileTimes pt(zipabspath);
    utils::ZipWriter zip(segment().format, zipabspath);
    std::vector<uint8_t> data = zip.get(span);
    data[0] = 0;
    zip.write(span, data);
    zip.close();
}


}
#include "base.tcc"
