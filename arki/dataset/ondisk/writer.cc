/*
 * dataset/ondisk/writer - Local on disk dataset writer
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/ondisk/writer.h>
#include <arki/dataset/ondisk/maintenance.h>
#include <arki/dataset/ondisk/writer/datafile.h>
#include <arki/dataset/ondisk/writer/directory.h>
#include <arki/dataset/ondisk/maint/datafile.h>
#include <arki/dataset/ondisk/maint/directory.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/utils.h>
#include <arki/summary.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <wibble/sys/lockfile.h>

#include <fstream>
#include <sstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

using namespace std;
using namespace wibble;

namespace arki {
namespace dataset {
namespace ondisk {

Writer::Writer(const ConfigFile& cfg)
	: m_cfg(cfg), m_root(0), m_replace(false)
{
	m_name = cfg.value("name");

	// If there is no 'index' in the config file, don't index anything
	if (cfg.value("index") != string())
		m_root = new ondisk::writer::IndexedRootDirectory(cfg);
	else
		m_root = new ondisk::writer::RootDirectory(cfg);
	
	m_replace = ConfigFile::boolValue(cfg.value("replace"), false);
}

Writer::~Writer()
{
	if (m_root) delete m_root;
}

std::string Writer::path() const
{
	return m_root->fullpath();
}

std::string Writer::id(const Metadata& md) const
{
	return m_root->id(md);
}

WritableDataset::AcquireResult Writer::acquire(Metadata& md)
{
	// If replace is on in the configuration file, do a replace instead
	if (m_replace)
		return replace(md) ? ACQ_OK : ACQ_ERROR;

	UItem<> origDataset = md.get(types::TYPE_ASSIGNEDDATASET);
	UItem<types::Source> origSource = md.source;

	try {
		m_root->acquire(md);
		return ACQ_OK;
	} catch (index::DuplicateInsert& di) {
		if (origDataset.defined())
			md.set(origDataset);
		else
			md.unset(types::TYPE_ASSIGNEDDATASET);
		md.source = origSource;
		md.notes.push_back(types::Note::create("Failed to store in dataset '"+m_name+"' because the dataset already has the data: " + di.what()));
		return ACQ_ERROR_DUPLICATE;
	} catch (std::exception& e) {
		// TODO: do something so that a following flush won't remove the
		// flagfile of the Datafile that failed here
		if (origDataset.defined())
			md.set(origDataset);
		else
			md.unset(types::TYPE_ASSIGNEDDATASET);
		md.source = origSource;
		md.notes.push_back(types::Note::create("Failed to store in dataset '"+m_name+"': " + e.what()));
		return ACQ_ERROR;
	}
}

bool Writer::replace(Metadata& md)
{
	UItem<> origDataset = md.get(types::TYPE_ASSIGNEDDATASET);
	UItem<types::Source> origSource = md.source;

	try {
		m_root->replace(md);
		return true;
	} catch (std::exception& e) {
		if (origDataset.defined())
			md.set(origDataset);
		else
			md.unset(types::TYPE_ASSIGNEDDATASET);
		md.source = origSource;
		md.notes.push_back(types::Note::create("Failed to store in dataset '"+m_name+"': " + e.what()));
		return false;
	}
}

void Writer::remove(const std::string& id)
{
	m_root->remove(id);
}

void Writer::flush()
{
	m_root->flush();
}

void Writer::maintenance(MaintenanceAgent& a)
{
	auto_ptr<maint::RootDirectory> maint_root(maint::RootDirectory::create(m_cfg));

	a.start(*this);

	// See if the main index is ok, eventually changing the indexing behaviour
	// in the next steps
	bool full_reindex = maint_root->checkMainIndex(a);
	// See that all data files are ok or if they need rebuild
	maint_root->checkDataFiles(a, full_reindex);
	// See that all the index info are ok (summaries and index)
	maint_root->checkDirectories(a);
	// Commit database changes
	maint_root->commit();

	a.end();
}

void Writer::repack(std::ostream& log, bool writable)
{
	auto_ptr<maint::RootDirectory> maint_root(maint::RootDirectory::create(m_cfg));
	auto_ptr<RepackAgent> repacker;

	if (writable)
		repacker.reset(new FullRepack(log));
	else
		repacker.reset(new RepackReport(log));

	maint_root->checkForRepack(*repacker);
	repacker->end();
	// Commit database changes
	maint_root->commit();
}

void Writer::check(std::ostream& log, bool fix, bool quick)
{
	if (fix)
	{
		dataset::ondisk::FullMaintenance ma(log);
		maintenance(ma);
	} else {
		MaintenanceReport ma(log);
		maintenance(ma);
		ma.report();
	}
}

void Writer::depthFirstVisit(Visitor& v)
{
	auto_ptr<maint::RootDirectory> maint_root(maint::RootDirectory::create(m_cfg));
	v.enterDataset(*this);
	maint_root->depthFirstVisit(v);
	v.leaveDataset(*this);
}

void Writer::invalidateAll()
{
	auto_ptr<maint::RootDirectory> maint_root(maint::RootDirectory::create(m_cfg));

	maint_root->invalidateAll();
}

WritableDataset::AcquireResult Writer::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
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
			} catch (index::DuplicateInsert& di) {
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
}

}
}
}
// vim:set ts=4 sw=4:
