#include "arki/libconfig.h"
#include "arki/dataset/file.h"
#include "arki/types/source/blob.h"
#include "arki/segment.h"
#include "arki/core/file.h"
#include "arki/metadata/consumer.h"
#include "arki/configfile.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/postprocess.h"
#include "arki/sort.h"
#include "arki/utils/files.h"
#include "arki/scan.h"
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

FileConfig::FileConfig(const ConfigFile& cfg)
    : dataset::Config(cfg),
      pathname(cfg.value("path")),
      format(cfg.value("format"))
{
}

std::shared_ptr<const FileConfig> FileConfig::create(const ConfigFile& cfg)
{
    return std::shared_ptr<const FileConfig>(new FileConfig(cfg));
}

std::unique_ptr<Reader> FileConfig::create_reader() const
{
    if (format == "arkimet")
        return std::unique_ptr<Reader>(new ArkimetFile(dynamic_pointer_cast<const FileConfig>(shared_from_this())));
    if (format == "yaml")
        return std::unique_ptr<Reader>(new YamlFile(dynamic_pointer_cast<const FileConfig>(shared_from_this())));
    return std::unique_ptr<Reader>(new RawFile(dynamic_pointer_cast<const FileConfig>(shared_from_this())));
}

bool File::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    return scan(q, dest);
}

void File::readConfig(const std::string& fname, ConfigFile& cfg)
{
    ConfigFile section;

    section.setValue("type", "file");
    if (sys::exists(fname))
    {
        section.setValue("path", sys::abspath(fname));
        section.setValue("format", scan::Scanner::format_from_filename(fname, "arkimet"));
        string name = str::basename(fname);
        section.setValue("name", name);
    } else {
        size_t fpos = fname.find(':');
        if (fpos == string::npos)
        {
            stringstream ss;
            ss << "file " << fname << " does not exist";
            throw runtime_error(ss.str());
        }
        section.setValue("format", scan::Scanner::normalise_format(fname.substr(0, fpos)));

        string fname1 = fname.substr(fpos+1);
        if (!sys::exists(fname1))
        {
            stringstream ss;
            ss << "file " << fname1 << " does not exist";
            throw runtime_error(ss.str());
        }
        section.setValue("path", sys::abspath(fname1));
        section.setValue("name", str::basename(fname1));
    }

    // Merge into cfg
    cfg.mergeInto(section.value("name"), section);
}


FdFile::FdFile(std::shared_ptr<const FileConfig> config)
    : m_config(config), fd(config->pathname, O_RDONLY)
{
}

FdFile::~FdFile()
{
}


static shared_ptr<sort::Stream> wrap_with_query(const dataset::DataQuery& q, metadata_dest_func& dest)
{
    // Wrap with a stream sorter if we need sorting
    shared_ptr<sort::Stream> sorter;
    if (q.sorter)
    {
        sorter.reset(new sort::Stream(*q.sorter, dest));
        dest = [sorter](unique_ptr<Metadata> md) { return sorter->add(move(md)); };
    }

    dest = [dest, &q](unique_ptr<Metadata> md) {
        // And filter using the query matcher
        if (!q.matcher(*md)) return true;
        return dest(move(md));
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
        if (!Metadata::read_file(fd, [&](unique_ptr<Metadata>&& md) {
                    if (md->has_source_blob())
                    {
                        const auto& blob = md->sourceBlob();
                        auto reader = Segment::detect_reader(
                                blob.format, blob.basedir, blob.filename, blob.absolutePathname(),
                                std::make_shared<core::lock::Null>());
                        md->sourceBlob().lock(reader);
                    }
                    return dest(move(md));
                }))
            return false;
    }
    if (sorter)
        return sorter->flush();
    return true;
}


YamlFile::YamlFile(std::shared_ptr<const FileConfig> config)
    : FdFile(config), reader(LineReader::from_fd(fd).release()) {}

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


RawFile::RawFile(std::shared_ptr<const FileConfig> config)
    : m_config(config)
{
}

RawFile::~RawFile() {}

bool RawFile::scan(const dataset::DataQuery& q, metadata_dest_func dest)
{
    string basedir, relpath;
    files::resolve_path(config().pathname, basedir, relpath);
    auto sorter = wrap_with_query(q, dest);
    auto reader = Segment::detect_reader(config().format, basedir, relpath, config().pathname, std::make_shared<core::lock::Null>());
    if (!reader->scan(dest))
        return false;
    if (sorter) return sorter->flush();
    return true;
}

}
}
