#include "arki/dataset/file.h"
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
#include <fcntl.h>
#include <sstream>

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace file {

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : dataset::Dataset(session, cfg),
      segment(Segment::from_isolated_file(cfg.value("path"), cfg.value("format")))
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
    return std::make_shared<RawFile>(session, cfg);
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

std::shared_ptr<core::cfg::Section> read_config(const std::filesystem::path& fname)
{
    auto section = std::make_shared<core::cfg::Section>();

    section->set("type", "file");
    if (std::filesystem::exists(fname))
    {
        section->set("path", std::filesystem::canonical(fname));
        section->set("format", scan::Scanner::format_from_filename(fname));
        section->set("name", fname);
    } else {
        size_t fpos = fname.native().find(':');
        if (fpos == string::npos)
        {
            stringstream ss;
            ss << "file " << fname << " does not exist";
            throw runtime_error(ss.str());
        }
        section->set("format", scan::Scanner::normalise_format(fname.native().substr(0, fpos)));

        std::filesystem::path fname1 = fname.native().substr(fpos+1);
        if (!std::filesystem::exists(fname1))
        {
            stringstream ss;
            ss << "file " << fname1 << " does not exist";
            throw runtime_error(ss.str());
        }
        section->set("path", std::filesystem::canonical(fname1));
        section->set("name", fname1);
    }

    return section;
}

std::shared_ptr<core::cfg::Sections> read_configs(const std::filesystem::path& fname)
{
    auto sec = read_config(fname);
    auto res = std::make_shared<core::cfg::Sections>();
    res->emplace(sec->value("name"), sec);
    return res;
}


FdFile::FdFile(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : Dataset(session, cfg), fd(segment->abspath, O_RDONLY)
{
}

FdFile::~FdFile()
{
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


ArkimetFile::~ArkimetFile() {}

bool ArkimetFile::scan(const query::Data& q, metadata_dest_func dest)
{
    // TODO: rewrite using Segment's reader query capabilities
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
                        auto segment = std::make_shared<Segment>(blob.format, blob.basedir, blob.filename);
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
        if (!dest(move(md)))
            return false;
    }

    if (sorter) return sorter->flush();

    return true;
}


bool RawFile::scan(const query::Data& q, metadata_dest_func dest)
{
    auto sorter = wrap_with_query(q, dest);
    auto reader = segment->detect_data_reader(std::make_shared<core::lock::Null>());
    if (!reader->scan(dest))
        return false;
    if (sorter) return sorter->flush();
    return true;
}

}
}
}
