#include "local.h"
#include "segmented.h"
#include "archive.h"
#include "arki/metadata.h"
#include "arki/utils/files.h"
#include "arki/configfile.h"
#include "arki/scan/any.h"
#include "arki/runtime/io.h"
#include "arki/metadata/consumer.h"
#include "arki/metadata/collection.h"
#include "arki/nag.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <iostream>
#include <fstream>
#include <sys/stat.h>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

template<typename Parent, typename Archive>
LocalBase<Parent, Archive>::LocalBase(const ConfigFile& cfg)
    : Parent(cfg.value("name"), cfg), m_path(cfg.value("path"))
{
    string tmp = cfg.value("archive age");
    if (!tmp.empty())
        m_archive_age = std::stoi(tmp);

    tmp = cfg.value("delete age");
    if (!tmp.empty())
        m_delete_age = std::stoi(tmp);
}

template<typename Parent, typename Archive>
LocalBase<Parent, Archive>::~LocalBase()
{
    delete m_archive;
}

template<typename Parent, typename Archive>
bool LocalBase<Parent, Archive>::hasArchive() const
{
    string arcdir = str::joinpath(m_path, ".archive");
    return sys::exists(arcdir);
}

template<typename Parent, typename Archive>
Archive& LocalBase<Parent, Archive>::archive()
{
    if (!m_archive)
    {
        m_archive = new Archive(m_path);
        m_archive->set_parent(*this);
    }
    return *m_archive;
}


LocalReader::LocalReader(const ConfigFile& cfg) : LocalBase(cfg)
{
}

LocalReader::~LocalReader()
{
}

void LocalReader::query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)> dest)
{
    if (hasArchive())
        archive().query_data(q, dest);
}

void LocalReader::query_summary(const Matcher& matcher, Summary& summary)
{
    if (hasArchive())
        archive().query_summary(matcher, summary);
}

void LocalReader::readConfig(const std::string& path, ConfigFile& cfg)
{
	if (path == "-")
	{
		// Parse the config file from stdin
		cfg.parse(cin, path);
		return;
	}

	// Remove trailing slashes, if any
	string fname = path;
	while (!fname.empty() && fname[fname.size() - 1] == '/')
		fname.resize(fname.size() - 1);

    // Check if it's a file or a directory
    std::unique_ptr<struct stat> st = sys::stat(fname);
    if (st.get() == 0)
    {
        stringstream ss;
        ss << "cannot read configuration from " << fname << " because it does not exist";
        throw runtime_error(ss.str());
    }
    if (S_ISDIR(st->st_mode))
    {
        // If it's a directory, merge in its config file
        string name = str::basename(fname);
        string file = str::joinpath(fname, "config");

        ConfigFile section;
        ifstream in;
        in.open(file.c_str(), ios::in);
        if (!in.is_open() || in.fail())
        {
            stringstream ss;
            ss << "cannot open " << file << " for reading";
            throw std::system_error(errno, std::system_category(), ss.str());
        }
        // Parse the config file into a new section
        section.parse(in, file);
        // Fill in missing bits
        section.setValue("name", name);
        section.setValue("path", sys::abspath(fname));
        // Merge into cfg
        cfg.mergeInto(name, section);
    } else {
        // If it's a file, then it's a merged config file
        ifstream in;
        in.open(fname.c_str(), ios::in);
        if (!in.is_open() || in.fail())
        {
            stringstream ss;
            ss << "cannot open config file " << fname << " for reading";
            throw std::system_error(errno, std::system_category(), ss.str());
        }
        // Parse the config file
        cfg.parse(in, fname);
    }
}

LocalWriter::LocalWriter(const ConfigFile& cfg)
    : Writer(cfg.value("name"), cfg), m_path(cfg.value("path"))
{
    // Create the directory if it does not exist
    sys::makedirs(m_path);
}

LocalWriter::~LocalWriter()
{
}

LocalWriter* LocalWriter::create(const ConfigFile& cfg)
{
    return SegmentedWriter::create(cfg);
}

LocalWriter::AcquireResult LocalWriter::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    return SegmentedWriter::testAcquire(cfg, md, out);
}


LocalChecker::LocalChecker(const ConfigFile& cfg) : LocalBase(cfg)
{
    // Create the directory if it does not exist
    sys::makedirs(m_path);
}

LocalChecker::~LocalChecker()
{
}

void LocalChecker::repack(dataset::Reporter& reporter, bool writable)
{
    if (hasArchive())
        archive().repack(reporter, writable);
}

void LocalChecker::check(dataset::Reporter& reporter, bool fix, bool quick)
{
    if (hasArchive())
        archive().check(reporter, fix, quick);
}

LocalChecker* LocalChecker::create(const ConfigFile& cfg)
{
    return SegmentedChecker::create(cfg);
}


template class LocalBase<Reader, ArchivesReader>;
template class LocalBase<Checker, ArchivesChecker>;
}
}
