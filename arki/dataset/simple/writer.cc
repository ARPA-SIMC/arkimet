/*
 * dataset/simple/writer - Writer for simple datasets with no duplicate checks
 *
 * Copyright (C) 2009--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/dataset/simple/writer.h>
#include <arki/dataset/index/manifest.h>
#include <arki/dataset/simple/datafile.h>
#include <arki/dataset/maintenance.h>
#include <arki/dataset/data.h>
#include <arki/data.h>
#include <arki/types/assigneddataset.h>
#include <arki/types/source/blob.h>
#include <arki/summary.h>
#include <arki/types/reftime.h>
#include <arki/matcher.h>
#include <arki/metadata/collection.h>
#include <arki/utils/files.h>
#include <arki/utils/dataset.h>
#include <arki/scan/any.h>
#include <arki/postprocess.h>
#include <arki/sort.h>
#include <arki/nag.h>

#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <wibble/string.h>

#include <fstream>
#include <ctime>
#include <cstdio>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

Writer::Writer(const ConfigFile& cfg)
	: WritableLocal(cfg), m_mft(0)
{
	m_name = cfg.value("name");

	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(m_path);

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!index::Manifest::exists(m_path))
        files::createDontpackFlagfile(m_path);

    auto_ptr<index::Manifest> mft = index::Manifest::create(m_path, &cfg);
	m_mft = mft.release();
	m_mft->openRW();
}

Writer::~Writer()
{
	flush();
	if (m_mft) delete m_mft;
}

data::Writer* Writer::file(const Metadata& md, const std::string& format)
{
    data::Writer* writer = WritableLocal::file(md, format);
    if (!writer->payload)
        writer->payload = new datafile::MdBuf(writer->absname);
    return writer;
}

WritableDataset::AcquireResult Writer::acquire(Metadata& md, ReplaceStrategy replace)
{
    // TODO: refuse if md is before "archive age"
    data::Writer* writer = file(md, md.source().format);
    datafile::MdBuf* mdbuf = static_cast<datafile::MdBuf*>(writer->payload);

    // Try appending
    const AssignedDataset* oldads = md.get<AssignedDataset>();
    md.set(AssignedDataset::create(m_name, ""));

    try {
        writer->append(md);
        mdbuf->add(md);
        m_mft->acquire(writer->relname, sys::fs::timestamp(mdbuf->pathname, 0), mdbuf->sum);
        return ACQ_OK;
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        if (oldads)
            md.set(*oldads);
        else
            md.unset(TYPE_ASSIGNEDDATASET);
        md.add_note("Failed to store in dataset '"+m_name+"': " + e.what());
        return ACQ_ERROR;
    }

	// After appending, keep updated info in-memory, and update manifest on
	// flush when the Datafile structures are deallocated
}

void Writer::remove(Metadata& id)
{
	// Nothing to do
    throw wibble::exception::Consistency("removing data from simple dataset", "dataset does not support removing items");
}

namespace {
struct Deleter : public maintenance::MaintFileVisitor
{
	std::string name;
	std::ostream& log;
	bool writable;

    Deleter(const std::string& name, std::ostream& log, bool writable)
        : name(name), log(log), writable(writable) {}
    void operator()(const std::string& file, data::FileState state)
    {
        if (writable)
        {
            log << name << ": deleting file " << file << endl;
            sys::fs::deleteIfExists(file);
        } else
            log << name << ": would delete file " << file << endl;
    }
};

struct CheckAge : public maintenance::MaintFileVisitor
{
	maintenance::MaintFileVisitor& next;
    const index::Manifest& idx;
    Time archive_threshold;
    Time delete_threshold;

    CheckAge(MaintFileVisitor& next, const index::Manifest& idx, int archive_age=-1, int delete_age=-1);

    void operator()(const std::string& file, data::FileState state);
};

CheckAge::CheckAge(MaintFileVisitor& next, const index::Manifest& idx, int archive_age, int delete_age)
	: next(next), idx(idx)
{
	time_t now = time(NULL);
	struct tm t;

	// Go to the beginning of the day
	now -= (now % (3600*24));

    if (archive_age != -1)
    {
        time_t arc_thr = now - archive_age * 3600 * 24;
        gmtime_r(&arc_thr, &t);
        archive_threshold.set(t);
    }
    if (delete_age != -1)
    {
        time_t del_thr = now - delete_age * 3600 * 24;
        gmtime_r(&del_thr, &t);
        delete_threshold.set(t);
    }
}

void CheckAge::operator()(const std::string& file, data::FileState state)
{
    if (!archive_threshold.isValid() and !delete_threshold.isValid())
        next(file, state);
    else
    {
        Time start_time;
        Time end_time;
        idx.fileTimespan(file, start_time, end_time);

        //cerr << "TEST " << maxdate << " WITH " << delete_threshold << " AND " << archive_threshold << endl;
        if (delete_threshold.isValid() && delete_threshold > end_time)
        {
            nag::verbose("CheckAge: %s is old enough to be deleted", file.c_str());
            next(file, state + FILE_TO_DELETE);
        }
        else if (archive_threshold.isValid() && archive_threshold > end_time)
        {
            nag::verbose("CheckAge: %s is old enough to be archived", file.c_str());
            next(file, state + FILE_TO_ARCHIVE);
        }
        else
            next(file, state);
    }
}
}

void Writer::maintenance(maintenance::MaintFileVisitor& v, bool quick)
{
	// TODO Detect if data is not in reftime order

	CheckAge ca(v, *m_mft, m_archive_age, m_delete_age);
	m_mft->check(*m_segment_manager, ca, quick);
	WritableLocal::maintenance(v, quick);
}

void Writer::removeAll(std::ostream& log, bool writable)
{
	Deleter deleter(m_name, log, writable);
	m_mft->check(*m_segment_manager, deleter, true);
	// TODO: empty manifest
	WritableLocal::removeAll(log, writable);
}

void Writer::flush()
{
    WritableLocal::flush();
    m_mft->flush();
}

void Writer::rescanFile(const std::string& relpath)
{
	// Delete cached info to force a full rescan
	string pathname = str::joinpath(m_path, relpath);
	sys::fs::deleteIfExists(pathname + ".metadata");
	sys::fs::deleteIfExists(pathname + ".summary");

	m_mft->rescanFile(m_path, relpath);
}


size_t Writer::repackFile(const std::string& relpath)
{
	string pathname = str::joinpath(m_path, relpath);

	// Read the metadata
	metadata::Collection mdc;
	scan::scan(pathname, mdc);

	// Sort by reference time
	mdc.sort();

	// Write out the data with the new order
    Pending p_repack = m_segment_manager->repack(relpath, mdc);

    // Strip paths from mds sources
    for (metadata::Collection::iterator i = mdc.begin(); i != mdc.end(); ++i)
    {
        const source::Blob& source = i->sourceBlob();
        i->set_source(upcast<Source>(source.fileOnly()));
    }

    // Prevent reading the still open old file using the new offsets
    Data::flushDataReaders();

	// Remove existing cached metadata, since we scramble their order
	sys::fs::deleteIfExists(pathname + ".metadata");
	sys::fs::deleteIfExists(pathname + ".summary");

    size_t size_pre = sys::fs::size(pathname);

    p_repack.commit();

    size_t size_post = sys::fs::size(pathname);

	// Write out the new metadata
	mdc.writeAtomically(pathname + ".metadata");

    // Regenerate the summary. It is unchanged, really, but its timestamp
    // has become obsolete by now
    Summary sum;
    metadata::SummarisingObserver mds(sum);
    mdc.sendToObserver(mds);
    sum.writeAtomically(pathname + ".summary");

    // Reindex with the new file information
    time_t mtime = sys::fs::timestamp(pathname);
    m_mft->acquire(relpath, mtime, sum);

	return size_pre - size_post;
}

size_t Writer::removeFile(const std::string& relpath, bool withData)
{
    m_mft->remove(relpath);
    return WritableLocal::removeFile(relpath, withData);
}

void Writer::archiveFile(const std::string& relpath)
{
	// Remove from index
	m_mft->remove(relpath);

	// Delegate the rest to WritableLocal
	WritableLocal::archiveFile(relpath);
}

size_t Writer::vacuum()
{
	// Nothing to do here really
	return 0;
}

Pending Writer::test_writelock()
{
    return m_mft->test_writelock();
}

WritableDataset::AcquireResult Writer::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
	// TODO
#if 0
	wibble::sys::fs::Lockfile lockfile(wibble::str::joinpath(cfg.value("path"), "lock"));

	string name = cfg.value("name");
	try {
		if (ConfigFile::boolValue(cfg.value("replace")))
		{
			if (cfg.value("index") != string())
				ondisk::writer::IndexedRootDirectory::testReplace(cfg, md, out);
			else
				ondisk::writer::RootDirectory::testReplace(cfg, md, out);
			return ACQ_OK;
		} else {
			try {
				if (cfg.value("index") != string())
					ondisk::writer::IndexedRootDirectory::testAcquire(cfg, md, out);
				else
					ondisk::writer::RootDirectory::testAcquire(cfg, md, out);
				return ACQ_OK;
			} catch (Index::DuplicateInsert& di) {
				out << "Source information restored to original value" << endl;
				out << "Failed to store in dataset '"+name+"' because the dataset already has the data: " + di.what() << endl;
				return ACQ_ERROR_DUPLICATE;
			}
		}
	} catch (std::exception& e) {
		out << "Source information restored to original value" << endl;
		out << "Failed to store in dataset '"+name+"': " + e.what() << endl;
		return ACQ_ERROR;
	}
#endif
}


}
}
}

// vim:set ts=4 sw=4:
