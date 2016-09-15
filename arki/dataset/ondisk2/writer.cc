#include "arki/dataset/ondisk2/writer.h"
#include "arki/exceptions.h"
#include "arki/binary.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/archive.h"
#include "arki/dataset/segment.h"
#include "arki/dataset/reporter.h"
#include "arki/types/source/blob.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/scan/dir.h"
#include "arki/scan/any.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/compress.h"
#include "arki/summary.h"
#include "arki/nag.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <system_error>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cassert>

using namespace std;
using namespace arki::utils;
using namespace arki::types;

namespace arki {
namespace dataset {
namespace ondisk2 {


Writer::Writer(std::shared_ptr<const ondisk2::Config> config)
    : m_config(config), idx(new index::WContents(config))
{
    m_idx = idx;

    // Create the directory if it does not exist
    bool dir_created = sys::makedirs(config->path);

    acquire_lock();

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!dir_created and !sys::exists(config->index_pathname))
        files::createDontpackFlagfile(config->path);

    idx->open();

    release_lock();
}

Writer::~Writer()
{
    flush();
}

std::string Writer::type() const { return "ondisk2"; }

Writer::AcquireResult Writer::acquire_replace_never(Metadata& md)
{
    Segment* w = file(md, md.source().format);
    off_t ofs;

    Pending p_idx = idx->beginTransaction();
    Pending p_df = w->append(md, &ofs);
    auto source = types::source::Blob::create(md.source().format, config().path, w->relname, ofs, md.data_size());

    try {
        int id;
        idx->index(md, w->relname, ofs, &id);
        p_df.commit();
        p_idx.commit();
        md.set_source(move(source));
        return ACQ_OK;
    } catch (utils::sqlite::DuplicateInsert& di) {
        md.add_note("Failed to store in dataset '" + name() + "' because the dataset already has the data: " + di.what());
        return ACQ_ERROR_DUPLICATE;
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note("Failed to store in dataset '" + name() + "': " + e.what());
        return ACQ_ERROR;
    }
}

Writer::AcquireResult Writer::acquire_replace_always(Metadata& md)
{
    Segment* w = file(md, md.source().format);
    off_t ofs;

    Pending p_idx = idx->beginTransaction();
    Pending p_df = w->append(md, &ofs);
    auto source = types::source::Blob::create(md.source().format, config().path, w->relname, ofs, md.data_size());

    try {
        int id;
        idx->replace(md, w->relname, ofs, &id);
        // In a replace, we necessarily replace inside the same file,
        // as it depends on the metadata reftime
        //createPackFlagfile(df->pathname);
        p_df.commit();
        p_idx.commit();
        md.set_source(move(source));
        return ACQ_OK;
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note("Failed to store in dataset '" + name() + "': " + e.what());
        return ACQ_ERROR;
    }
}

Writer::AcquireResult Writer::acquire_replace_higher_usn(Metadata& md)
{
    // Try to acquire without replacing
    Segment* w = file(md, md.source().format);
    off_t ofs;

    Pending p_idx = idx->beginTransaction();
    Pending p_df = w->append(md, &ofs);
    auto source = types::source::Blob::create(md.source().format, config().path, w->relname, ofs, md.data_size());

    try {
        int id;
        idx->index(md, w->relname, ofs, &id);
        p_df.commit();
        p_idx.commit();
        md.set_source(move(source));
        return ACQ_OK;
    } catch (utils::sqlite::DuplicateInsert& di) {
        // It already exists, so we keep p_df uncommitted and check Update Sequence Numbers
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note("Failed to store in dataset '" + name() + "': " + e.what());
        return ACQ_ERROR;
    }

    // Read the update sequence number of the new BUFR
    int new_usn;
    if (!scan::update_sequence_number(md, new_usn))
        return ACQ_ERROR_DUPLICATE;

    // Read the metadata of the existing BUFR
    Metadata old_md;
    if (!idx->get_current(md, old_md))
    {
        stringstream ss;
        ss << "cannot acquire into dataset " << name() << ": insert reported a conflict, the index failed to find the original version";
        throw runtime_error(ss.str());
    }

    // Read the update sequence number of the old BUFR
    int old_usn;
    if (!scan::update_sequence_number(old_md, old_usn))
    {
        stringstream ss;
        ss << "cannot acquire into dataset " << name() << ": insert reported a conflict, the new element has an Update Sequence Number but the old one does not, so they cannot be compared";
        throw runtime_error(ss.str());
    }

    // If there is no new Update Sequence Number, report a duplicate
    if (old_usn > new_usn)
        return ACQ_ERROR_DUPLICATE;

    // Replace, reusing the pending datafile transaction from earlier
    try {
        int id;
        idx->replace(md, w->relname, ofs, &id);
        // In a replace, we necessarily replace inside the same file,
        // as it depends on the metadata reftime
        //createPackFlagfile(df->pathname);
        p_df.commit();
        p_idx.commit();
        md.set_source(move(source));
        return ACQ_OK;
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note("Failed to store in dataset '" + name() + "': " + e.what());
        return ACQ_ERROR;
    }
}

Writer::AcquireResult Writer::acquire(Metadata& md, ReplaceStrategy replace)
{
    acquire_lock();

    if (replace == REPLACE_DEFAULT) replace = config().default_replace_strategy;

    // TODO: refuse if md is before "archive age"

    switch (replace)
    {
        case REPLACE_NEVER: return acquire_replace_never(md);
        case REPLACE_ALWAYS: return acquire_replace_always(md);
        case REPLACE_HIGHER_USN: return acquire_replace_higher_usn(md);
        default:
        {
            stringstream ss;
            ss << "cannot acquire into dataset " << name() << ": replace strategy " << (int)replace << " is not supported";
            throw runtime_error(ss.str());
        }
    }
}

void Writer::remove(Metadata& md)
{
    acquire_lock();

    const types::source::Blob* source = md.has_source_blob();
    if (!source)
        throw std::runtime_error("cannot remove metadata from dataset, because it has no Blob source");

    if (source->basedir != config().path)
        throw std::runtime_error("cannot remove metadata from dataset: its basedir is " + source->basedir + " but this dataset is at " + config().path);

    // TODO: refuse if md is in the archive

    // Delete from DB, and get file name
    Pending p_del = idx->beginTransaction();
    idx->remove(source->filename, source->offset);

    // Create flagfile
    //createPackFlagfile(str::joinpath(config().path, file));

    // Commit delete from DB
    p_del.commit();

    // reset source and dataset in the metadata
    md.unset_source();
    md.unset(TYPE_ASSIGNEDDATASET);
}

void Writer::flush()
{
    IndexedWriter::flush();
    idx->flush();
    release_lock();
}

Pending Writer::test_writelock()
{
    return idx->beginExclusiveTransaction();
}


std::string ShardingWriter::type() const { return "ondisk2"; }


Checker::Checker(std::shared_ptr<const ondisk2::Config> config)
    : m_config(config), idx(new index::WContents(config))
{
    m_idx = idx;

    // Create the directory if it does not exist
    bool dir_created = sys::makedirs(config->path);

    acquire_lock();

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!dir_created && !sys::exists(config->index_pathname))
        files::createDontpackFlagfile(config->path);

    idx->open();

    release_lock();
}

std::string Checker::type() const { return "ondisk2"; }

void Checker::removeAll(dataset::Reporter& reporter, bool writable) { acquire_lock(); IndexedChecker::removeAll(reporter, writable); release_lock(); }
void Checker::repack(dataset::Reporter& reporter, bool writable) { acquire_lock(); IndexedChecker::repack(reporter, writable); release_lock(); }

void Checker::check(dataset::Reporter& reporter, bool fix, bool quick)
{
    acquire_lock();

    IndexedChecker::check(reporter, fix, quick);

    if (!idx->checkSummaryCache(*this, reporter) && fix)
    {
        reporter.operation_progress(name(), "check", "rebuilding summary cache");
        idx->rebuildSummaryCache();
    }

    release_lock();
}


namespace {

/// Create unique strings from metadata
struct IDMaker
{
    std::set<types::Code> components;

    IDMaker(const std::set<types::Code>& components) : components(components) {}

    vector<uint8_t> make_string(const Metadata& md) const
    {
        vector<uint8_t> res;
        BinaryEncoder enc(res);
        for (set<types::Code>::const_iterator i = components.begin(); i != components.end(); ++i)
            if (const Type* t = md.get(*i))
                t->encodeBinary(enc);
        return res;
    }
};

}


void Checker::rescanSegment(const std::string& relpath)
{
    string pathname = str::joinpath(config().path, relpath);
    //fprintf(stderr, "Checker::rescanSegment %s\n", pathname.c_str());

    // Temporarily uncompress the file for scanning
    unique_ptr<utils::compress::TempUnzip> tu;
    if (scan::isCompressed(pathname))
        tu.reset(new utils::compress::TempUnzip(pathname));

    // Collect the scan results in a metadata::Collector
    metadata::Collection mds;
    if (!scan::scan(pathname, mds.inserter_func()))
        throw std::runtime_error("cannot rescan " + pathname + ": file format unknown");
    //fprintf(stderr, "SCANNED %s: %zd\n", pathname.c_str(), mds.size());

    // Lock away writes and reads
    Pending p = idx->beginExclusiveTransaction();
    // cerr << "LOCK" << endl;

    // Remove from the index all data about the file
    idx->reset(relpath);
    // cerr << " RESET " << file << endl;

    // Scan the list of metadata, looking for duplicates and marking all
    // the duplicates except the last one as deleted
    IDMaker id_maker(idx->unique_codes());

    map<vector<uint8_t>, const Metadata*> finddupes;
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        vector<uint8_t> id = id_maker.make_string(**i);
        if (id.empty())
            continue;
        auto dup = finddupes.find(id);
        if (dup == finddupes.end())
            finddupes.insert(make_pair(id, *i));
        else
            dup->second = *i;
    }
    // cerr << " DUPECHECKED " << pathname << ": " << finddupes.size() << endl;

    // Send the remaining metadata to the reindexer
    std::string basename = str::basename(relpath);
    for (const auto& i: finddupes)
    {
        const Metadata& md = *i.second;
        const source::Blob& blob = md.sourceBlob();
        try {
            int id;
            if (str::basename(blob.filename) != basename)
                throw std::runtime_error("cannot rescan " + relpath + ": metadata points to the wrong file: " + blob.filename);
            idx->index(md, relpath, blob.offset, &id);
        } catch (utils::sqlite::DuplicateInsert& di) {
            stringstream ss;
            ss << "cannot reindex " << basename << ": data item at offset " << blob.offset << " has a duplicate elsewhere in the dataset: manual fix is required";
            throw runtime_error(ss.str());
        } catch (std::exception& e) {
            stringstream ss;
            ss << "cannot reindex " << basename << ": failed to reindex data item at offset " << blob.offset << ": " << e.what();
            throw runtime_error(ss.str());
            // sqlite will take care of transaction consistency
        }
    }
    // cerr << " REINDEXED " << pathname << endl;

    // TODO: if scan fails, remove all info from the index and rename the
    // file to something like .broken

    // Commit the changes on the database
    p.commit();
    // cerr << " COMMITTED" << endl;

    // TODO: remove relevant summary
}


size_t Checker::repackSegment(const std::string& relpath)
{
    // Lock away writes and reads
    Pending p = idx->beginExclusiveTransaction();

    // Make a copy of the file with the right data in it, sorted by
    // reftime, and update the offsets in the index
    string pathname = str::joinpath(config().path, relpath);

    metadata::Collection mds;
    idx->scan_file(relpath, mds.inserter_func(), "reftime, offset");
    Pending p_repack = segment_manager().repack(relpath, mds);

    // Reindex mds
    idx->reset(relpath);
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        const source::Blob& source = (*i)->sourceBlob();
        idx->index(**i, source.filename, source.offset);
    }

    size_t size_pre = sys::size(pathname);

    // Remove the .metadata file if present, because we are shuffling the
    // data file and it will not be valid anymore
    string mdpathname = pathname + ".metadata";
    if (sys::exists(mdpathname))
        if (unlink(mdpathname.c_str()) < 0)
        {
            stringstream ss;
            ss << "cannot remove obsolete metadata file " << mdpathname;
            throw std::system_error(errno, std::system_category(), ss.str());
        }

    // Prevent reading the still open old file using the new offsets
    Metadata::flushDataReaders();

    // Commit the changes in the file system
    p_repack.commit();

    // Commit the changes in the database
    p.commit();

    size_t size_post = sys::size(pathname);

    return size_pre - size_post;
}

void Checker::indexSegment(const std::string& relpath, metadata::Collection&& contents)
{
    // Add to index
    Pending p_idx = idx->beginTransaction();
    for (auto& md: contents)
    {
        const auto& src = md->sourceBlob();
        int id;
        idx->index(*md, relpath, src.offset, &id);
    }
    p_idx.commit();

    // Remove .metadata and .summary files
    sys::unlink_ifexists(str::joinpath(config().path, relpath) + ".metadata");
    sys::unlink_ifexists(str::joinpath(config().path, relpath) + ".summary");
}

size_t Checker::removeSegment(const std::string& relpath, bool withData)
{
    idx->reset(relpath);
    // TODO: also remove .metadata and .summary files
    return IndexedChecker::removeSegment(relpath, withData);
}

void Checker::releaseSegment(const std::string& relpath, const std::string& destpath)
{
    string pathname = str::joinpath(config().path, relpath);

    // Rebuild the metadata
    metadata::Collection mds;
    idx->scan_file(relpath, mds.inserter_func());
    mds.writeAtomically(pathname + ".metadata");

    // Remove from index
    idx->reset(relpath);

    IndexedChecker::releaseSegment(relpath, destpath);
}

size_t Checker::vacuum()
{
    size_t size_pre = 0, size_post = 0;
    if (sys::size(idx->pathname(), 0) > 0)
    {
        size_pre = sys::size(idx->pathname(), 0)
                 + sys::size(idx->pathname() + "-journal", 0);
        idx->vacuum();
        size_post = sys::size(idx->pathname(), 0)
                  + sys::size(idx->pathname() + "-journal", 0);
    }

    // Rebuild the cached summaries, if needed
    if (!sys::exists(str::joinpath(config().path, ".summaries/all.summary")))
    {
        Summary s;
        idx->summaryForAll(s);
    }

    return size_pre > size_post ? size_pre - size_post : 0;
}

Writer::AcquireResult Writer::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    ReplaceStrategy replace;
    string repl = cfg.value("replace");
    if (repl == "yes" || repl == "true" || repl == "always")
        replace = REPLACE_ALWAYS;
    else if (repl == "USN")
        replace = REPLACE_HIGHER_USN;
    else if (repl == "" || repl == "no" || repl == "never")
        replace = REPLACE_NEVER;
    else
        throw std::runtime_error("Replace strategy '" + repl + "' is not recognised in dataset configuration");

    // TODO: refuse if md is before "archive age"

    if (replace == REPLACE_ALWAYS) return ACQ_OK;

    std::shared_ptr<const ondisk2::Config> config(new ondisk2::Config(cfg));
    index::RContents idx(config);
    idx.open();

    Metadata old_md;
    bool exists = idx.get_current(md, old_md);

    if (!exists) return ACQ_OK;

    if (replace == REPLACE_NEVER) return ACQ_ERROR_DUPLICATE;

    // We are left with the case of REPLACE_HIGHER_USN

    // Read the update sequence number of the new BUFR
    int new_usn;
    Metadata tmpmd(md);
    if (!scan::update_sequence_number(tmpmd, new_usn))
        return ACQ_ERROR_DUPLICATE;

    // Read the update sequence number of the old BUFR
    int old_usn;
    if (!scan::update_sequence_number(old_md, old_usn))
    {
        out << "cannot acquire into dataset: insert reported a conflict, the new element has an Update Sequence Number but the old one does not, so they cannot be compared";
        return ACQ_ERROR;
    }

    // If there is no new Update Sequence Number, report a duplicate
    if (old_usn > new_usn)
        return ACQ_ERROR_DUPLICATE;
    return ACQ_OK;
}

std::string ShardingChecker::type() const { return "ondisk2"; }

}
}
}
