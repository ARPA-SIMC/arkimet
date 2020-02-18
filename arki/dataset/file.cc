#include "arki/libconfig.h"
#include "arki/dataset/file.h"
#include "arki/types/source/blob.h"
#include "arki/segment.h"
#include "arki/core/file.h"
#include "arki/metadata/sort.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/scan.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace file {

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : dataset::Dataset(session, cfg),
      pathname(cfg.value("path")),
      format(cfg.value("format"))
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


bool Reader::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    return dataset().scan(q, dest);
}

core::cfg::Section read_config(const std::string& fname)
{
    core::cfg::Section section;

    section.set("type", "file");
    if (sys::exists(fname))
    {
        section.set("path", sys::abspath(fname));
        section.set("format", scan::Scanner::format_from_filename(fname, "arkimet"));
        section.set("name", fname);
    } else {
        size_t fpos = fname.find(':');
        if (fpos == string::npos)
        {
            stringstream ss;
            ss << "file " << fname << " does not exist";
            throw runtime_error(ss.str());
        }
        section.set("format", scan::Scanner::normalise_format(fname.substr(0, fpos)));

        string fname1 = fname.substr(fpos+1);
        if (!sys::exists(fname1))
        {
            stringstream ss;
            ss << "file " << fname1 << " does not exist";
            throw runtime_error(ss.str());
        }
        section.set("path", sys::abspath(fname1));
        section.set("name", fname1);
    }

    return section;
}

core::cfg::Sections read_configs(const std::string& fname)
{
    auto sec = read_config(fname);
    core::cfg::Sections res;
    res.obtain(sec.value("name")) = std::move(sec);
    return res;
}


FdFile::FdFile(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : Dataset(session, cfg), fd(pathname, O_RDONLY)
{
}

FdFile::~FdFile()
{
}


static std::shared_ptr<metadata::sort::Stream> wrap_with_query(const dataset::DataQuery& q, metadata_dest_func& dest)
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

bool ArkimetFile::scan(const dataset::DataQuery& q, metadata_dest_func dest)
{
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
                        auto reader = Segment::detect_reader(
                                blob.format, blob.basedir, blob.filename, blob.absolutePathname(),
                                std::make_shared<core::lock::Null>());
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

bool YamlFile::scan(const dataset::DataQuery& q, metadata_dest_func dest)
{
    auto sorter = wrap_with_query(q, dest);

    while (true)
    {
        unique_ptr<Metadata> md(new Metadata);
        if (!md->readYaml(*reader, fd.name()))
            break;
        if (!q.matcher(*md))
            continue;
        if (!dest(move(md)))
            return false;
    }

    if (sorter) return sorter->flush();

    return true;
}


bool RawFile::scan(const dataset::DataQuery& q, metadata_dest_func dest)
{
    string basedir, relpath;
    files::resolve_path(pathname, basedir, relpath);
    auto sorter = wrap_with_query(q, dest);
    auto reader = Segment::detect_reader(format, basedir, relpath, pathname, std::make_shared<core::lock::Null>());
    if (!reader->scan(dest))
        return false;
    if (sorter) return sorter->flush();
    return true;
}

}
}
}
