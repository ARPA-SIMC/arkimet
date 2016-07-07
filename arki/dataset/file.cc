#include "arki/libconfig.h"
#include "arki/dataset/file.h"
#include "arki/metadata/consumer.h"
#include "arki/configfile.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/postprocess.h"
#include "arki/sort.h"
#include "arki/utils/files.h"
#include "arki/scan/any.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>


using namespace std;
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
#ifdef HAVE_GRIBAPI
    if (format == "grib")
        return std::unique_ptr<Reader>(new RawFile(dynamic_pointer_cast<const FileConfig>(shared_from_this())));
#endif
#ifdef HAVE_DBALLE
    if (format == "bufr")
        return std::unique_ptr<Reader>(new RawFile(dynamic_pointer_cast<const FileConfig>(shared_from_this())));
#endif
#ifdef HAVE_HDF5
    if (format == "odimh5")
        return std::unique_ptr<Reader>(new RawFile(dynamic_pointer_cast<const FileConfig>(shared_from_this())));
#endif
#ifdef HAVE_VM2
    if (format == "vm2")
        return std::unique_ptr<Reader>(new RawFile(dynamic_pointer_cast<const FileConfig>(shared_from_this())));
#endif

    throw runtime_error(pathname + ": unknown fiel format \"" + format + "\"");
}

void File::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    scan(q, dest);
}

void File::query_summary(const Matcher& matcher, Summary& summary)
{
    scan(DataQuery(matcher), [&](unique_ptr<Metadata> md) { summary.add(*md); return true; });
}

void File::readConfig(const std::string& fname, ConfigFile& cfg)
{
    ConfigFile section;

    if (fname == "-")
    {
        // If the input is stdin, make hardcoded assumptions
        section.setValue("name", "stdin");
        section.setValue("path", "-");
        section.setValue("type", "file");
        section.setValue("format", "arkimet");
    } else if (str::endswith(fname, ":-")) {
        // If the input is stdin, make hardcoded assumptions
        section.setValue("name", "stdin");
        section.setValue("path", "-");
        section.setValue("type", "file");
        section.setValue("format", fname.substr(0, fname.size()-2));
    } else {
        section.setValue("type", "file");
        if (sys::exists(fname))
        {
            section.setValue("path", sys::abspath(fname));
            section.setValue("format", files::format_from_ext(fname, "arkimet"));
            string name = str::basename(fname);
            section.setValue("name", name);
        } else {
            size_t fpos = fname.find(':');
            if (fpos == string::npos)
            {
                stringstream ss;
                ss << "dataset file " << fname << " does not exist";
                throw runtime_error(ss.str());
            }
            section.setValue("format", files::normaliseFormat(fname.substr(0, fpos)));

            string fname1 = fname.substr(fpos+1);
            if (!sys::exists(fname1))
            {
                stringstream ss;
                ss << "dataset file " << fname1 << " does not exist";
                throw runtime_error(ss.str());
            }
            section.setValue("path", sys::abspath(fname1));
            section.setValue("name", str::basename(fname1));
        }
    }

    // Merge into cfg
    cfg.mergeInto(section.value("name"), section);
}

#if 0
File* File::create(const ConfigFile& cfg)
{
    string format = str::lower(cfg.value("format"));

    if (format == "arkimet")
        return new ArkimetFile(cfg);
    if (format == "yaml")
        return new YamlFile(cfg);
#ifdef HAVE_GRIBAPI
    if (format == "grib")
        return new RawFile(cfg);
#endif
#ifdef HAVE_DBALLE
    if (format == "bufr")
        return new RawFile(cfg);
#endif
#ifdef HAVE_HDF5
    if (format == "odimh5")
        return new RawFile(cfg);
#endif
#ifdef HAVE_VM2
    if (format == "vm2")
        return new RawFile(cfg);
#endif

    stringstream ss;
    ss << "cannot create a dataset for the unknown file format \"" << format << "\"";
    throw runtime_error(ss.str());
}
#endif

FdFile::FdFile(std::shared_ptr<const FileConfig> config)
    : m_config(config)
{
    const std::string& path = config->pathname;
    if (path == "-")
        fd = new Stdin;
    else
        fd = new arki::File(path, O_RDONLY);
}

FdFile::~FdFile()
{
    delete fd;
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

void ArkimetFile::scan(const dataset::DataQuery& q, metadata_dest_func dest)
{
    auto sorter = wrap_with_query(q, dest);
    Metadata::read_file(*fd, dest);
    if (sorter) sorter->flush();
}


YamlFile::YamlFile(std::shared_ptr<FileConfig> config)
    : FdFile(config), reader(LineReader::from_fd(*fd).release()) {}

YamlFile::~YamlFile() { delete reader; }

void YamlFile::scan(const dataset::DataQuery& q, metadata_dest_func dest)
{
    auto sorter = wrap_with_query(q, dest);

    while (true)
    {
        unique_ptr<Metadata> md(new Metadata);
        if (!md->readYaml(*reader, fd->name()))
            break;
        if (!q.matcher(*md))
            continue;
        if (!dest(move(md))) break;
    }

    if (sorter) sorter->flush();
}


RawFile::RawFile(std::shared_ptr<const FileConfig> config)
    : m_config(config)
{
}

RawFile::~RawFile() {}

void RawFile::scan(const dataset::DataQuery& q, metadata_dest_func dest)
{
    auto sorter = wrap_with_query(q, dest);
    scan::scan(config().pathname, dest, config().format);
    if (sorter) sorter->flush();
}

}
}
