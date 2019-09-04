#include "arki/dataset.h"
#include "arki/libconfig.h"
#include "arki/dataset/file.h"
#include "arki/dataset/ondisk2.h"
#include "arki/dataset/iseg.h"
#include "arki/dataset/simple/reader.h"
#include "arki/dataset/outbound.h"
#include "arki/dataset/empty.h"
#include "arki/dataset/fromfunction.h"
#include "arki/dataset/testlarge.h"
#include "arki/dataset/reporter.h"
#include "arki/metadata.h"
#include "arki/metadata/sort.h"
#include "arki/metadata/postprocess.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/summary.h"
#include "arki/scan.h"
#include "arki/nag.h"

#ifdef HAVE_LIBCURL
#include "arki/dataset/http.h"
#endif

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace dataset {

DataQuery::DataQuery() : with_data(false) {}
DataQuery::DataQuery(const std::string& matcher, bool with_data) : matcher(Matcher::parse(matcher)), with_data(with_data), sorter(0) {}
DataQuery::DataQuery(const Matcher& matcher, bool with_data) : matcher(matcher), with_data(with_data), sorter(0) {}
DataQuery::~DataQuery() {}

Config::Config() {}

Config::Config(const std::string& name) : name(name) {}

Config::Config(const core::cfg::Section& cfg)
    : name(cfg.value("name")), cfg(cfg)
{
}

std::unique_ptr<Reader> Config::create_reader() const { throw std::runtime_error("reader not implemented for dataset " + name); }
std::unique_ptr<Writer> Config::create_writer() const { throw std::runtime_error("writer not implemented for dataset " + name); }
std::unique_ptr<Checker> Config::create_checker() const { throw std::runtime_error("checker not implemented for dataset " + name); }

std::shared_ptr<const Config> Config::create(const core::cfg::Section& cfg)
{
    string type = str::lower(cfg.value("type"));

    if (type == "iseg")
        return dataset::iseg::Config::create(cfg);
    if (type == "ondisk2")
        return dataset::ondisk2::Config::create(cfg);
    if (type == "simple" || type == "error" || type == "duplicates")
        return dataset::simple::Config::create(cfg);
#ifdef HAVE_LIBCURL
    if (type == "remote")
        return dataset::http::Config::create(cfg);
#endif
    if (type == "outbound")
        return outbound::Config::create(cfg);
    if (type == "discard")
        return empty::Config::create(cfg);
    if (type == "file")
        return dataset::FileConfig::create(cfg);
    if (type == "fromfunction")
        return fromfunction::Config::create(cfg);
    if (type == "testlarge")
        return testlarge::Config::create(cfg);

    throw std::runtime_error("cannot use configuration: unknown dataset type \""+type+"\"");
}


std::string Base::name() const
{
    if (m_parent)
        return m_parent->name() + "." + config().name;
    else
        return config().name;
}

void Base::set_parent(Base& p)
{
    m_parent = &p;
}


void Reader::query_summary(const Matcher& matcher, Summary& summary)
{
    query_data(DataQuery(matcher), [&](std::shared_ptr<Metadata> md) { summary.add(*md); return true; });
}

void Reader::query_bytes(const dataset::ByteQuery& q, NamedFileDescriptor& out)
{
    switch (q.type)
    {
        case dataset::ByteQuery::BQ_DATA: {
            bool first = true;
            query_data(q, [&](std::shared_ptr<Metadata> md) {
                if (first)
                {
                    if (q.data_start_hook) q.data_start_hook(out);
                    first = false;
                }
                md->stream_data(out);
                return true;
            });
            break;
        }
        case dataset::ByteQuery::BQ_POSTPROCESS: {
            metadata::Postprocess postproc(q.param);
            postproc.set_output(out);
            postproc.validate(config().cfg);
            postproc.set_data_start_hook(q.data_start_hook);
            postproc.start();
            query_data(q, [&](std::shared_ptr<Metadata> md) { return postproc.process(md); });
            postproc.flush();
            break;
        }
        default:
        {
            stringstream s;
            s << "cannot query dataset: unsupported query type: " << (int)q.type;
            throw std::runtime_error(s.str());
        }
    }
}

void Reader::query_bytes(const dataset::ByteQuery& q, AbstractOutputFile& out)
{
    if (q.data_start_hook)
        throw std::runtime_error("Cannot use data_start_hook on abstract output files");

    switch (q.type)
    {
        case dataset::ByteQuery::BQ_DATA: {
            query_data(q, [&](std::shared_ptr<Metadata> md) {
                md->stream_data(out);
                return true;
            });
            break;
        }
        case dataset::ByteQuery::BQ_POSTPROCESS: {
            metadata::Postprocess postproc(q.param);
            postproc.set_output(out);
            postproc.validate(config().cfg);
            postproc.set_data_start_hook(q.data_start_hook);
            postproc.start();
            query_data(q, [&](std::shared_ptr<Metadata> md) { return postproc.process(move(md)); });
            postproc.flush();
            break;
        }
        default:
        {
            stringstream s;
            s << "cannot query dataset: unsupported query type: " << (int)q.type;
            throw std::runtime_error(s.str());
        }
    }
}

void Reader::expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end)
{
}

std::unique_ptr<Reader> Reader::create(const core::cfg::Section& cfg)
{
    auto config = Config::create(cfg);
    return config->create_reader();
}

core::cfg::Section Reader::read_config(const std::string& path)
{
    if (path == "-")
    {
        // Parse the config file from stdin
        Stdin in;
        return core::cfg::Section::parse(in);
    }

    // Remove trailing slashes, if any
    string fname = path;
    while (!fname.empty() && fname[fname.size() - 1] == '/')
        fname.resize(fname.size() - 1);

    std::unique_ptr<struct stat> st = sys::stat(fname);

    if (st.get() == 0)
    {
        // If it does not exist, it could be a URL or a format:filename URL-like
        size_t pos = path.find(':');
        if (pos == string::npos)
        {
            stringstream ss;
            ss << "cannot read configuration from " << fname << " because it does not exist";
            throw runtime_error(ss.str());
        }

        std::string prefix = path.substr(0, pos);
#ifdef HAVE_LIBCURL
        if (prefix == "http" or prefix == "https")
            return dataset::http::Reader::load_cfg_section(path);
        else
#endif
            return dataset::File::read_config(path);
    }

    if (S_ISDIR(st->st_mode))
        return dataset::LocalReader::read_config(fname);
    else
        return dataset::File::read_config(fname);
}


core::cfg::Sections Reader::read_configs(const std::string& path)
{
    if (path == "-")
    {
        // Parse the config file from stdin
        Stdin in;
        return core::cfg::Sections::parse(in);
    }

    // Remove trailing slashes, if any
    string fname = path;
    while (!fname.empty() && fname[fname.size() - 1] == '/')
        fname.resize(fname.size() - 1);

    std::unique_ptr<struct stat> st = sys::stat(fname);

    if (st.get() == 0)
    {
        // If it does not exist, it could be a URL or a format:filename URL-like
        size_t pos = path.find(':');
        if (pos == string::npos)
        {
            stringstream ss;
            ss << "cannot read configuration from " << fname << " because it does not exist";
            throw runtime_error(ss.str());
        }

        std::string prefix = path.substr(0, pos);
#ifdef HAVE_LIBCURL
        if (prefix == "http" or prefix == "https")
            return dataset::http::Reader::load_cfg_sections(path);
        else
#endif
            return dataset::File::read_configs(path);
    }

    if (S_ISDIR(st->st_mode))
    {
        // A directory, read the dataset config
        return dataset::LocalReader::read_configs(fname);
    }
    else
    {
        // A file, check for known extensions
        std::string format = scan::Scanner::format_from_filename(fname, "");
        if (!format.empty())
            return dataset::File::read_configs(fname);

        // Read the contents as configuration
        sys::File in(fname, O_RDONLY);
        return core::cfg::Sections::parse(in);
    }
}


void WriterBatch::set_all_error(const std::string& note)
{
    for (auto& e: *this)
    {
        e->dataset_name.clear();
        e->md.add_note(note);
        e->result = ACQ_ERROR;
    }
}


void Writer::flush() {}

Pending Writer::test_writelock() { return Pending(); }

std::unique_ptr<Writer> Writer::create(const core::cfg::Section& cfg)
{
    auto config = Config::create(cfg);
    return config->create_writer();
}

void Writer::test_acquire(const core::cfg::Section& cfg, WriterBatch& batch)
{
    string type = str::lower(cfg.value("type"));
    if (type == "remote")
        throw std::runtime_error("cannot simulate dataset acquisition: remote datasets are not writable");
    if (type == "outbound")
        return outbound::Writer::test_acquire(cfg, batch);
    if (type == "discard")
        return empty::Writer::test_acquire(cfg, batch);

    return dataset::LocalWriter::test_acquire(cfg, batch);
}

CheckerConfig::CheckerConfig()
    : reporter(make_shared<NullReporter>())
{
}

CheckerConfig::CheckerConfig(std::shared_ptr<dataset::Reporter> reporter, bool readonly)
    : reporter(reporter), readonly(readonly)
{
}


std::unique_ptr<Checker> Checker::create(const core::cfg::Section& cfg)
{
    auto config = Config::create(cfg);
    return config->create_checker();
}

void Checker::check_issue51(CheckerConfig& opts)
{
    throw std::runtime_error(name() + ": check_issue51 not implemented for this dataset");
}

}
}
