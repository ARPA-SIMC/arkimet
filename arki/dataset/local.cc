#include "local.h"
#include "segmented.h"
#include "archive.h"
#include "arki/exceptions.h"
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
#include <fcntl.h>
#include <sys/stat.h>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

LocalConfig::LocalConfig(const ConfigFile& cfg)
    : Config(cfg), path(sys::abspath(cfg.value("path"))), lockfile_pathname(str::joinpath(path, "lock"))
{
    string tmp = cfg.value("archive age");
    if (!tmp.empty())
        archive_age = std::stoi(tmp);

    tmp = cfg.value("delete age");
    if (!tmp.empty())
        delete_age = std::stoi(tmp);
}

void LocalConfig::to_shard(const std::string& shard_path)
{
    m_archives_config = std::shared_ptr<ArchivesConfig>();
    path = str::joinpath(path, shard_path);
    lockfile_pathname = str::joinpath(path, "lock");
    //archive_age = -1;
    //delete_age = -1;
}

std::shared_ptr<ArchivesConfig> LocalConfig::archives_config() const
{
    if (!m_archives_config)
        m_archives_config = std::shared_ptr<ArchivesConfig>(new ArchivesConfig(path));
    return m_archives_config;
}


template<typename Parent, typename Archive>
LocalBase<Parent, Archive>::~LocalBase()
{
    delete m_archive;
}

template<typename Parent, typename Archive>
bool LocalBase<Parent, Archive>::hasArchive() const
{
    string arcdir = str::joinpath(path(), ".archive");
    return sys::exists(arcdir);
}

template<typename Parent, typename Archive>
Archive& LocalBase<Parent, Archive>::archive()
{
    if (!m_archive)
    {
        m_archive = new Archive(config().archives_config());
        m_archive->set_parent(*this);
    }
    return *m_archive;
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
        Stdin in;
        cfg.parse(in);
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
        File in(file, O_RDONLY);
        // Parse the config file into a new section
        section.parse(in);
        // Fill in missing bits
        section.setValue("name", name);
        section.setValue("path", sys::abspath(fname));
        // Merge into cfg
        cfg.mergeInto(name, section);
    } else {
        // If it's a file, then it's a merged config file
        File in(fname, O_RDONLY);
        // Parse the config file
        cfg.parse(in);
    }
}

LocalLock::LocalLock(const std::string& pathname)
    : lockfile(pathname)
{
}

LocalLock::~LocalLock()
{
}

void LocalLock::acquire()
{
    if (locked) return;
    if ((int)lockfile == -1) lockfile.open(O_RDWR | O_CREAT, 0777);
    ds_lock.l_type = F_WRLCK;
    ds_lock.l_whence = SEEK_SET;
    ds_lock.l_start = 0;
    ds_lock.l_len = 0;
    ds_lock.l_pid = 0;
    // Use SETLKW, so that if it is already locked, we just wait
#ifdef F_OFD_SETLKW
    if (fcntl(lockfile, F_OFD_SETLKW, &ds_lock) == -1)
#else
// This stops compilation with -Werror, I still have not found a way to just output diagnostics
//#warning "old style locks make concurrency tests unreliable in the test suite"
    if (fcntl(lockfile, F_SETLKW, &ds_lock) == -1)
#endif
        throw_file_error(lockfile.name(), "cannot lock the file for writing");
    locked = true;
}

void LocalLock::release()
{
    if (!locked) return;
    ds_lock.l_type = F_UNLCK;
#ifdef F_OFD_SETLK
    fcntl(lockfile, F_OFD_SETLK, &ds_lock);
#else
// This stops compilation with -Werror, I still have not found a way to just output diagnostics
//#warning "old style locks make concurrency tests unreliable in the test suite"
    fcntl(lockfile, F_SETLK, &ds_lock);
#endif
    locked = false;
}


LocalWriter::~LocalWriter()
{
    delete lock;
}

void LocalWriter::acquire_lock()
{
    if (!lock) lock = new LocalLock(config().lockfile_pathname);
    lock->acquire();
}

void LocalWriter::release_lock()
{
    if (!lock) lock = new LocalLock(config().lockfile_pathname);
    lock->release();
}

LocalWriter::AcquireResult LocalWriter::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    return segmented::Writer::testAcquire(cfg, md, out);
}


LocalChecker::~LocalChecker()
{
    delete lock;
}

void LocalChecker::acquire_lock()
{
    if (!lock) lock = new LocalLock(config().lockfile_pathname);
    lock->acquire();
}

void LocalChecker::release_lock()
{
    if (!lock) lock = new LocalLock(config().lockfile_pathname);
    lock->release();
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


template class LocalBase<Reader, ArchivesReader>;
template class LocalBase<Checker, ArchivesChecker>;
}
}
