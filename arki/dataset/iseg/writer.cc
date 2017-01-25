#include "arki/dataset/iseg/writer.h"
#include "arki/dataset/iseg/index.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/segment.h"
#include "arki/types/source/blob.h"
#include "arki/summary.h"
#include "arki/types/reftime.h"
#include "arki/matcher.h"
#include "arki/metadata/collection.h"
#include "arki/utils/files.h"
#include "arki/scan/any.h"
#include "arki/postprocess.h"
#include "arki/sort.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <ctime>
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace iseg {

Writer::Writer(std::shared_ptr<const iseg::Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);
}

Writer::~Writer()
{
    flush();
}

std::string Writer::type() const { return "iseg"; }

Segment* Writer::file(const Metadata& md, const std::string& format)
{
    Segment* writer = segmented::Writer::file(md, format);
    if (!writer->payload)
        writer->payload = new WIndex(m_config, writer->relname);
    return writer;
}

Writer::AcquireResult Writer::acquire_replace_never(Metadata& md)
{
    Segment* writer = file(md, md.source().format);
    WIndex* idx = static_cast<WIndex*>(writer->payload);
    Pending p_idx = idx->begin_transaction();

    off_t ofs;
    Pending p_df = writer->append(md, &ofs);
    auto source = types::source::Blob::create(md.source().format, config().path, writer->relname, ofs, md.data_size());

    try {
        idx->index(md, ofs);
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
    Segment* writer = file(md, md.source().format);
    WIndex* idx = static_cast<WIndex*>(writer->payload);
    Pending p_idx = idx->begin_transaction();

    off_t ofs;
    Pending p_df = writer->append(md, &ofs);
    auto source = types::source::Blob::create(md.source().format, config().path, writer->relname, ofs, md.data_size());

    try {
        idx->replace(md, ofs);
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
    Segment* writer = file(md, md.source().format);
    WIndex* idx = static_cast<WIndex*>(writer->payload);
    Pending p_idx = idx->begin_transaction();

    off_t ofs;
    Pending p_df = writer->append(md, &ofs);
    auto source = types::source::Blob::create(md.source().format, config().path, writer->relname, ofs, md.data_size());

    try {
        // Try to acquire without replacing
        idx->index(md, ofs);
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
    throw std::runtime_error("iseg::Writer::acquire_replace_higher_usn not yet implemented");
#if 0
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
        idx->replace(md, ofs);
        p_df.commit();
        p_idx.commit();
        md.set_source(move(source));
        return ACQ_OK;
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note("Failed to store in dataset '" + name() + "': " + e.what());
        return ACQ_ERROR;
    }
#endif
}

Writer::AcquireResult Writer::acquire(Metadata& md, ReplaceStrategy replace)
{
    auto age_check = check_acquire_age(md);
    if (age_check.first) return age_check.second;

    acquire_lock();

    if (replace == REPLACE_DEFAULT) replace = config().default_replace_strategy;

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
    // Nothing to do
    throw std::runtime_error("cannot remove data from iseg dataset: dataset does not support removing items");
}

void Writer::flush()
{
    segmented::Writer::flush();
    //m_mft->flush();
    release_lock();
}

//Pending Writer::test_writelock()
//{
//    //return m_mft->test_writelock();
//}

Writer::AcquireResult Writer::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    // Acquire on iseg datasets always succeeds except in case of envrionment
    // issues like I/O errors and full disks
    return ACQ_OK;
}


Checker::Checker(std::shared_ptr<const iseg::Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);

    //acquire_lock();
    //release_lock();
}

Checker::~Checker()
{
    //m_mft->flush();
}

std::string Checker::type() const { return "iseg"; }

void Checker::removeAll(dataset::Reporter& reporter, bool writable) { acquire_lock(); segmented::Checker::removeAll(reporter, writable); release_lock(); }
segmented::State Checker::scan(dataset::Reporter& reporter, bool quick) { return segmented::State(); }
void Checker::repack(dataset::Reporter& reporter, bool writable) { acquire_lock(); segmented::Checker::repack(reporter, writable); release_lock(); }
void Checker::check(dataset::Reporter& reporter, bool fix, bool quick) { acquire_lock(); segmented::Checker::check(reporter, fix, quick); release_lock(); }

void Checker::indexSegment(const std::string& relname, metadata::Collection&& mds)
{
    string pathname = str::joinpath(config().path, relname);
    time_t mtime = scan::timestamp(pathname);
    if (mtime == 0)
        throw std::runtime_error("cannot acquire " + pathname + ": file does not exist");

    // Iterate the metadata, computing the summary and making the data
    // paths relative
    mds.strip_source_paths();
    Summary sum;
    mds.add_to_summary(sum);

    // Regenerate .metadata
    mds.writeAtomically(pathname + ".metadata");

    // Regenerate .summary
    sum.writeAtomically(pathname + ".summary");

    // Add to manifest
    //m_mft->acquire(relname, mtime, sum);
    //m_mft->flush();
}

void Checker::rescanSegment(const std::string& relpath)
{
    // Delete cached info to force a full rescan
    string pathname = str::joinpath(config().path, relpath);
    sys::unlink_ifexists(pathname + ".metadata");
    sys::unlink_ifexists(pathname + ".summary");

    //m_mft->rescanSegment(config().path, relpath);
}


size_t Checker::repackSegment(const std::string& relpath)
{
    string pathname = str::joinpath(config().path, relpath);

    // Read the metadata
    metadata::Collection mdc;
    scan::scan(pathname, mdc.inserter_func());

    // Sort by reference time
    mdc.sort();

    // Write out the data with the new order
    Pending p_repack = segment_manager().repack(relpath, mdc);

    // Strip paths from mds sources
    mdc.strip_source_paths();

    // Prevent reading the still open old file using the new offsets
    Metadata::flushDataReaders();

    // Remove existing cached metadata, since we scramble their order
    sys::unlink_ifexists(pathname + ".metadata");
    sys::unlink_ifexists(pathname + ".summary");

    size_t size_pre = sys::size(pathname);

    p_repack.commit();

    size_t size_post = sys::size(pathname);

    // Write out the new metadata
    mdc.writeAtomically(pathname + ".metadata");

    // Regenerate the summary. It is unchanged, really, but its timestamp
    // has become obsolete by now
    Summary sum;
    mdc.add_to_summary(sum);
    sum.writeAtomically(pathname + ".summary");

    // Reindex with the new file information
    //time_t mtime = sys::timestamp(pathname);
    //m_mft->acquire(relpath, mtime, sum);

    return size_pre - size_post;
}

size_t Checker::removeSegment(const std::string& relpath, bool withData)
{
    //m_mft->remove(relpath);
    return segmented::Checker::removeSegment(relpath, withData);
}

void Checker::releaseSegment(const std::string& relpath, const std::string& destpath)
{
    // Remove from index
    //m_mft->remove(relpath);

    segmented::Checker::releaseSegment(relpath, destpath);
}

size_t Checker::vacuum()
{
    //return m_mft->vacuum();
    return 0;
}


}
}
}
