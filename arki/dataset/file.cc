#include "arki/dataset/file.h"
#include "arki/dataset/session.h"
#include "arki/query.h"
#include "arki/query/progress.h"
#include "arki/metadata.h"
#include "arki/segment/data.h"
#include "arki/core/file.h"
#include "arki/core/time.h"
#include "arki/types/source/blob.h"
#include "arki/metadata/sort.h"
#include "arki/matcher.h"
#include "arki/scan.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <fcntl.h>
#include <sstream>

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace file {

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : dataset::Dataset(session, cfg)
{
}

std::shared_ptr<dataset::Reader> Dataset::create_reader()
{
    return std::make_shared<Reader>(static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<Dataset> Dataset::from_config(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
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
    throw std::runtime_error("file::Reader::get_stored_time_interval not yet implemented");
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

    if (f == "grib") return "grib";
    if (f == "grib1") return "grib";
    if (f == "grib2") return "grib";

    if (f == "bufr") return "bufr";
    if (f == "vm2") return "vm2";

    if (f == "h5")     return "odimh5";
    if (f == "hdf5")   return "odimh5";
    if (f == "odim")   return "odimh5";
    if (f == "odimh5") return "odimh5";

    if (f == "nc") return "netcdf";
    if (f == "netcdf") return "netcdf";

    if (f == "jpg") return "jpeg";
    if (f == "jpeg") return "jpeg";

    if (f == "arkimet") return "arkimet";
    if (f == "metadata") return "arkimet";

    if (f == "yaml") return "yaml";

    return std::optional<std::string>();
}

std::shared_ptr<core::cfg::Section> read_config(const std::string& prefix, const std::filesystem::path& path)
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

std::shared_ptr<core::cfg::Section> read_config(const std::filesystem::path& path)
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

std::shared_ptr<core::cfg::Sections> read_configs(const std::filesystem::path& fname)
{
    auto sec = read_config(fname);
    if (!sec)
        return std::shared_ptr<core::cfg::Sections>();
    auto res = std::make_shared<core::cfg::Sections>();
    res->emplace(sec->value("name"), sec);
    return res;
}

std::shared_ptr<core::cfg::Sections> read_configs(const std::string& prefix, const std::filesystem::path& fname)
{
    auto sec = read_config(prefix, fname);
    auto res = std::make_shared<core::cfg::Sections>();
    res->emplace(sec->value("name"), sec);
    return res;
}

static std::shared_ptr<metadata::sort::Stream> wrap_with_query(const query::Data& q, metadata_dest_func& dest)
{
    // Wrap with a stream sorter if we need sorting
    shared_ptr<metadata::sort::Stream> sorter;
    if (q.sorter)
    {
        sorter.reset(new metadata::sort::Stream(*q.sorter, dest));
        dest = [sorter](std::shared_ptr<Metadata> md) { return sorter->add(md); };
    }

    dest = [dest, &q](std::shared_ptr<Metadata> md) {
        // And filter using the query matcher
        if (!q.matcher(*md)) return true;
        return dest(md);
    };

    return sorter;
}


SegmentDataset::SegmentDataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : Dataset(session, cfg),
      segment_session(std::make_shared<segment::Session>()),
      segment(segment_session->segment_from_path_and_format(cfg.value("path"), format_from_string(cfg.value("format"))))
{
}

bool SegmentDataset::scan(const query::Data& q, metadata_dest_func dest)
{
    auto sorter = wrap_with_query(q, dest);
    auto reader = segment->detect_data_reader(std::make_shared<core::lock::Null>());
    if (!reader->scan(dest))
        return false;
    if (sorter) return sorter->flush();
    return true;
}


FdFile::FdFile(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : Dataset(session, cfg), fd(cfg.value("path"), O_RDONLY), path(cfg.value("path"))
{
}

FdFile::~FdFile()
{
}


ArkimetFile::~ArkimetFile() {}

bool ArkimetFile::scan(const query::Data& q, metadata_dest_func dest)
{
    // TODO: rewrite using Segment's reader query capabilities
    auto segment_session = std::make_shared<segment::Session>();
    auto sorter = wrap_with_query(q, dest);
    if (!q.with_data)
    {
        if (!Metadata::read_file(fd, dest))
            return false;
    } else {
        if (!Metadata::read_file(fd, [&](std::shared_ptr<Metadata> md) {
                    if (md->has_source_blob())
                    {
                        const auto& blob = md->sourceBlob();
                        auto segment = segment_session->segment(blob.format, blob.basedir, blob.filename);
                        auto reader = segment->detect_data_reader(std::make_shared<core::lock::Null>());
                        md->sourceBlob().lock(reader);
                    }
                    return dest(md);
                }))
            return false;
    }
    if (sorter)
        return sorter->flush();
    return true;
}


YamlFile::YamlFile(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : FdFile(session, cfg), reader(LineReader::from_fd(fd).release()) {}

YamlFile::~YamlFile() { delete reader; }

bool YamlFile::scan(const query::Data& q, metadata_dest_func dest)
{
    auto sorter = wrap_with_query(q, dest);

    while (true)
    {
        auto md = Metadata::read_yaml(*reader, fd.path());
        if (!md)
            break;
        if (!q.matcher(*md))
            continue;
        if (!dest(std::move(md)))
            return false;
    }

    if (sorter) return sorter->flush();

    return true;
}

}
}
}
