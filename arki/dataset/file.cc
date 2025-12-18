#include "arki/dataset/file.h"
#include "arki/core/lock.h"
#include "arki/core/time.h"
#include "arki/dataset/session.h"
#include "arki/matcher.h"
#include "arki/metadata.h"
#include "arki/metadata/reader.h"
#include "arki/metadata/sort.h"
#include "arki/query.h"
#include "arki/query/progress.h"
#include "arki/segment.h"
#include "arki/segment/metadata.h"
#include "arki/segment/scan.h"
#include "arki/types/source/blob.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <fcntl.h>
#include <sstream>

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace arki::dataset::file {

std::shared_ptr<segment::Reader> SegmentSession::create_segment_reader(
    std::shared_ptr<const Segment> segment,
    std::shared_ptr<const core::ReadLock> lock) const
{
    if (segment::metadata::has_valid_metadata(segment))
        return std::make_shared<segment::metadata::Reader>(segment, lock);
    else
        return std::make_shared<segment::scan::Reader>(segment, lock);
}

std::shared_ptr<segment::Writer>
SegmentSession::segment_writer(std::shared_ptr<const Segment>,
                               std::shared_ptr<core::AppendLock>) const
{
    throw std::runtime_error("file dataset does not support writing");
}

std::shared_ptr<segment::Checker>
SegmentSession::segment_checker(std::shared_ptr<const Segment>,
                                std::shared_ptr<core::CheckLock>) const
{
    throw std::runtime_error("file dataset does not support checking");
}

Dataset::Dataset(std::shared_ptr<Session> session,
                 const core::cfg::Section& cfg)
    : dataset::Dataset(session, cfg)
{
}

std::shared_ptr<dataset::Reader> Dataset::create_reader()
{
    return std::make_shared<Reader>(
        static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<Dataset> Dataset::from_config(std::shared_ptr<Session> session,
                                              const core::cfg::Section& cfg)
{
    std::string format(cfg.value("format"));
    if (format == "arkimet")
        return std::make_shared<ArkimetFile>(session, cfg);
    if (format == "yaml")
        return std::make_shared<YamlFile>(session, cfg);
    return std::make_shared<SegmentDataset>(session, cfg);
}

bool Reader::impl_query_data(const query::Data& q, metadata_dest_func dest)
{
    query::TrackProgress track(q.progress);
    dest = track.wrap(dest);
    return track.done(dataset().scan(q, dest));
}

core::Interval Reader::get_stored_time_interval()
{
    throw std::runtime_error(
        "file::Reader::get_stored_time_interval not yet implemented");
}

/**
 * Normalise a file format string using the most widely used version
 *
 * This currently normalises:
 *  - grib1 and grib2 to grib
 *  - all of h5, hdf5, odim and odimh5 to odimh5
 *
 * If the format is unsupported, it throws an exception.
 */
static std::optional<std::string> normalise_format_name(const std::string& name)
{
    std::string f = str::lower(name);

    if (f == "grib")
        return "grib";
    if (f == "grib1")
        return "grib";
    if (f == "grib2")
        return "grib";

    if (f == "bufr")
        return "bufr";
    if (f == "vm2")
        return "vm2";

    if (f == "h5")
        return "odimh5";
    if (f == "hdf5")
        return "odimh5";
    if (f == "odim")
        return "odimh5";
    if (f == "odimh5")
        return "odimh5";

    if (f == "nc")
        return "netcdf";
    if (f == "netcdf")
        return "netcdf";

    if (f == "jpg")
        return "jpeg";
    if (f == "jpeg")
        return "jpeg";

    if (f == "arkimet")
        return "arkimet";
    if (f == "metadata")
        return "arkimet";

    if (f == "yaml")
        return "yaml";

    return std::optional<std::string>();
}

std::shared_ptr<core::cfg::Section>
read_config(const std::string& prefix, const std::filesystem::path& path)
{
    if (!std::filesystem::exists(path))
    {
        std::stringstream ss;
        ss << prefix << " file " << path << " does not exist";
        throw runtime_error(ss.str());
    }
    auto format_name = normalise_format_name(prefix);
    if (!format_name)
        throw std::runtime_error("unsupported format '" + prefix + "'");

    auto section = std::make_shared<core::cfg::Section>();
    section->set("type", "file");
    section->set("format", format_name.value());
    section->set("path", std::filesystem::canonical(path));
    section->set("name", path);
    return section;
}

std::shared_ptr<core::cfg::Section>
read_config(const std::filesystem::path& path, DataFormat format)
{
    if (!std::filesystem::exists(path))
    {
        std::stringstream ss;
        ss << path << " does not exist";
        throw runtime_error(ss.str());
    }
    auto section = std::make_shared<core::cfg::Section>();
    section->set("type", "file");
    section->set("format", format_name(format));
    section->set("path", std::filesystem::canonical(path));
    section->set("name", path);
    return section;
}

std::shared_ptr<core::cfg::Section>
read_config(const std::filesystem::path& path)
{
    if (!std::filesystem::exists(path))
    {
        std::stringstream ss;
        ss << "file " << path << " does not exist";
        throw runtime_error(ss.str());
    }
    auto ext = path.extension();
    if (ext.empty())
        return std::shared_ptr<core::cfg::Section>();

    auto format_name = normalise_format_name(ext.native().substr(1));
    if (!format_name)
        return std::shared_ptr<core::cfg::Section>();

    auto section = std::make_shared<core::cfg::Section>();
    section->set("type", "file");
    section->set("path", std::filesystem::canonical(path));
    section->set("name", path);
    section->set("format", format_name.value());
    return section;
}

std::shared_ptr<core::cfg::Sections>
read_configs(const std::filesystem::path& fname)
{
    auto sec = read_config(fname);
    if (!sec)
        return std::shared_ptr<core::cfg::Sections>();
    auto res = std::make_shared<core::cfg::Sections>();
    res->emplace(sec->value("name"), sec);
    return res;
}

std::shared_ptr<core::cfg::Sections>
read_configs(const std::string& prefix, const std::filesystem::path& fname)
{
    auto sec = read_config(prefix, fname);
    auto res = std::make_shared<core::cfg::Sections>();
    res->emplace(sec->value("name"), sec);
    return res;
}

static std::shared_ptr<metadata::sort::Stream>
wrap_with_query(const query::Data& q, metadata_dest_func& dest)
{
    // Wrap with a stream sorter if we need sorting
    shared_ptr<metadata::sort::Stream> sorter;
    if (q.sorter)
    {
        sorter.reset(new metadata::sort::Stream(*q.sorter, dest));
        dest = [sorter](std::shared_ptr<Metadata> md) {
            return sorter->add(md);
        };
    }

    dest = [dest, &q](std::shared_ptr<Metadata> md) {
        // And filter using the query matcher
        if (!q.matcher(*md))
            return true;
        return dest(md);
    };

    return sorter;
}

/*
 * SegmentDataset
 */

SegmentDataset::SegmentDataset(std::shared_ptr<Session> session,
                               const core::cfg::Section& cfg)
    : Dataset(session, cfg)
{
    std::filesystem::path basedir;
    std::filesystem::path relpath;
    utils::files::resolve_path(cfg.value("path"), basedir, relpath);
    if (basedir.empty())
        basedir = relpath.parent_path();
    segment_session = std::make_shared<SegmentSession>(basedir);
    segment         = segment_session->segment_from_relpath_and_format(
        relpath, format_from_string(cfg.value("format")));
}

bool SegmentDataset::scan(const query::Data& q, metadata_dest_func dest)
{
    auto sorter = wrap_with_query(q, dest);
    auto reader = segment->reader(std::make_shared<core::lock::NullReadLock>());
    if (!reader->read_all(dest))
        return false;
    if (sorter)
        return sorter->flush();
    return true;
}

/*
 * FdFile
 */

FdFile::FdFile(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : Dataset(session, cfg), fd(cfg.value("path"), O_RDONLY),
      path(cfg.value("path"))
{
    segment_session = std::make_shared<SegmentSession>(path.parent_path());
}

FdFile::~FdFile() {}

void FdFile::ensure_data_is_accessible(Metadata& md)
{
    if (!md.has_source_blob())
        return;
    const auto& blob = md.sourceBlob();
    std::shared_ptr<segment::Session> session;
    if (blob.basedir == segment_session->root)
        session = segment_session;
    else
        session = std::make_shared<SegmentSession>(blob.basedir);

    auto segment =
        session->segment_from_relpath_and_format(blob.filename, blob.format);
    auto reader = segment->reader(std::make_shared<core::lock::NullReadLock>());
    md.sourceBlob().lock(reader);
}

bool FdFile::scan_reader(metadata::BaseReader& reader, const query::Data& q,
                         metadata_dest_func dest)
{
    auto sorter = wrap_with_query(q, dest);
    if (!q.with_data)
    {
        if (!reader.read_all(dest))
            return false;
    }
    else
    {
        if (!reader.read_all([&](std::shared_ptr<Metadata> md) {
                ensure_data_is_accessible(*md);
                return dest(md);
            }))
            return false;
    }
    if (sorter)
        return sorter->flush();
    return true;
}

/*
 * ArkimetFile
 */

ArkimetFile::~ArkimetFile() {}

bool ArkimetFile::scan(const query::Data& q, metadata_dest_func dest)
{
    metadata::BinaryReader md_reader(fd);
    return scan_reader(md_reader, q, dest);
}

/*
 * YamlFile
 */

YamlFile::YamlFile(std::shared_ptr<Session> session,
                   const core::cfg::Section& cfg)
    : FdFile(session, cfg), reader(LineReader::from_fd(fd).release())
{
}

YamlFile::~YamlFile() { delete reader; }

bool YamlFile::scan(const query::Data& q, metadata_dest_func dest)
{
    metadata::YamlReader yaml_reader(fd);
    return scan_reader(yaml_reader, q, dest);
}

} // namespace arki::dataset::file
