/*
 * dispatcher - Dispatch data into dataset
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dispatcher.h>

#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <wibble/string.h>
#include <arki/configfile.h>
#include <arki/metadata/consumer.h>
#include <arki/matcher.h>
#include <arki/dataset.h>

using namespace std;
using namespace wibble;

namespace arki {

static inline Matcher getFilter(const ConfigFile* cfg)
{
	try {
		return Matcher::parse(cfg->value("filter"));
	} catch (wibble::exception::Generic& e) {
		const ConfigFile::FilePos* fp = cfg->valueInfo("filter");
		if (fp)
			e.addContext("in file " + fp->pathname + ":" + str::fmt(fp->lineno));
		throw;
	}
}

Dispatcher::Dispatcher(const ConfigFile& cfg)
	: m_can_continue(true), m_outbound_failures(0)
{
	// Validate the configuration, and split normal datasets from outbound
	// datasets
	for (ConfigFile::const_section_iterator i = cfg.sectionBegin();
			i != cfg.sectionEnd(); ++i)
	{
		if (i->first == "error" or i->first == "duplicates")
			continue;
		else if (i->second->value("type") == "outbound")
		{
			if (i->second->value("filter").empty())
				throw wibble::exception::Consistency(
						"configuration of dataset '"+i->first+"' does not have a 'filter' directive",
						"reading dataset configuration");
			outbounds.push_back(make_pair(i->first, getFilter(i->second)));
		}
		else {
			if (i->second->value("filter").empty())
				throw wibble::exception::Consistency(
						"configuration of dataset '"+i->first+"' does not have a 'filter' directive",
						"reading dataset configuration");
			datasets.push_back(make_pair(i->first, getFilter(i->second)));
		}
	}
}

Dispatcher::~Dispatcher()
{
}


RealDispatcher::RealDispatcher(const ConfigFile& cfg)
	: Dispatcher(cfg), pool(cfg), dserror(0), dsduplicates(0)
{
	// Instantiate the error dataset in the cache
	dserror = pool.get("error");
	if (!dserror)
		throw wibble::exception::Consistency(
			"no [error] dataset found", "reading dataset configuration");

	// Instantiate the duplicates dataset in the cache
	dsduplicates = pool.get("duplicates");
}

RealDispatcher::~RealDispatcher()
{
	// The error and duplicates datasets do not want deallocation, as they are
	// a reference to the version inside the DatasetPool cache
}

Dispatcher::Outcome RealDispatcher::dispatch(Metadata& md, metadata::Consumer& mdc)
{
	try {
		// Fetch the data into memory here, so that if problems arise we do not
		// fail in bits of code that are more critical
		md.getData();
	} catch (std::exception& e) {
		md.add_note(types::Note::create(
			string("Failed to read the data associated with the metadata: ") + e.what()));
		return DISP_NOTWRITTEN;
	}

	vector<string> found;
	// See what outbound datasets match this metadata
	for (vector< pair<string, Matcher> >::const_iterator i = outbounds.begin();
			i != outbounds.end(); ++i)
		if (i->second(md))
		{
			// Operate on a copy
			Metadata md1 = md;
			// File it to the outbound dataset right away
			WritableDataset* target = pool.get(i->first);
			if (target->acquire(md1) != WritableDataset::ACQ_OK)
			{
				// What do we do in case of error?
				// The dataset will already have added a note to the dataset
				// explaining what was wrong.  The best we can do is keep a
				// count of failures.
				++m_outbound_failures;
			}
			if (m_can_continue)
				m_can_continue = mdc(md);
		}
	// See how many datasets match this metadata
	for (vector< pair<string, Matcher> >::const_iterator i = datasets.begin();
			i != datasets.end(); ++i)
		if (i->second(md))
			found.push_back(i->first);

	UItem<types::Source> origSource = md.source;
	bool inErrorDS = false;
	
	// If we found only one dataset, acquire it in that dataset; else,
	// acquire it in the error dataset
	string tgname;
	WritableDataset* target = 0;
	if (found.size() == 1)
	{
		tgname = found[0];
	}
	else if (found.empty())
	{
		md.add_note(types::Note::create("Message could not be assigned to any dataset"));
		target = dserror;
		inErrorDS = true;
	}
	else if (found.size() > 1)
	{
		string msg = "Message matched multiple datasets: ";
		for (vector<string>::const_iterator i = found.begin();
				i != found.end(); ++i)
			if (i == found.begin())
				msg += *i;
			else
				msg += ", " + *i;
		md.add_note(types::Note::create(msg));
		target = dserror;
		inErrorDS = true;
	}

	// Acquire into the dataset
	if (!target)
		target = pool.get(tgname);
	Dispatcher::Outcome result;
	switch (target->acquire(md))
	{
		case WritableDataset::ACQ_OK:
			result = inErrorDS ? DISP_ERROR : DISP_OK;
			break;
		case WritableDataset::ACQ_ERROR_DUPLICATE:
			// If insertion in the designed dataset failed, insert in the
			// error dataset
			if (dsduplicates)
				target = dsduplicates;
			else
				target = dserror;
			result = target->acquire(md) == WritableDataset::ACQ_OK ? DISP_DUPLICATE_ERROR : DISP_NOTWRITTEN;
			break;
		case WritableDataset::ACQ_ERROR:
		default:
			// If insertion in the designed dataset failed, insert in the
			// error dataset
			target = dserror;
			result = target->acquire(md) == WritableDataset::ACQ_OK ? DISP_ERROR : DISP_NOTWRITTEN;
			break;
	}
	if (m_can_continue)
		m_can_continue = mdc(md);
	return result;
}

void RealDispatcher::flush() { pool.flush(); }



TestDispatcher::TestDispatcher(const ConfigFile& cfg, std::ostream& out)
	: Dispatcher(cfg), cfg(cfg), out(out), m_count(0)
{
	if (!cfg.section("error"))
		throw wibble::exception::Consistency(
			"no [error] dataset found", "reading dataset configuration");
}
TestDispatcher::~TestDispatcher() {}

Dispatcher::Outcome TestDispatcher::dispatch(Metadata& md, metadata::Consumer& mdc)
{
	// Increment the metadata counter, so that we can refer to metadata in the
	// messages
	++m_count;
	string prefix = "Message " + str::fmt(md.source);

	try {
		// Fetch the data into memory here, so that if problems arise we do not
		// fail in bits of code that are more critical
		md.getData();
	} catch (std::exception& e) {
		out << prefix << ": Failed to read the data associated with the metadata: " << e.what() << endl;
		return DISP_NOTWRITTEN;
	}

	vector<string> found;

	// See what outbound datasets match this metadata
	for (vector< pair<string, Matcher> >::const_iterator i = outbounds.begin();
			i != outbounds.end(); ++i)
	{
		out << prefix << ": output to " << i->first << " outbound dataset" << endl;
		if (WritableDataset::testAcquire(*cfg.section(i->first), md, out) != WritableDataset::ACQ_OK)
		{
			// What do we do in case of error?
			// The dataset will already have added a note to the dataset
			// explaining what was wrong.  The best we can do is keep a
			// count of failures.
			++m_outbound_failures;
		}
	}

	// See how many proper datasets match this metadata
	for (vector< pair<string, Matcher> >::const_iterator i = datasets.begin();
			i != datasets.end(); ++i)
		if (i->second(md))
			found.push_back(i->first);

	bool inErrorDS = false;
	
	// If we found only one dataset, acquire it in that dataset; else,
	// acquire it in the error dataset
	string tgname;
	ConfigFile* target = 0;
	if (found.size() == 1)
	{
		tgname = found[0];
	}
	else if (found.empty())
	{
		out << prefix << ": not matched by any dataset" << endl;
		tgname = "error";
		inErrorDS = true;
	}
	else if (found.size() > 1)
	{
		out << prefix << ": matched by multiple datasets: ";
		for (vector<string>::const_iterator i = found.begin();
				i != found.end(); ++i)
			if (i == found.begin())
				out << *i;
			else
				out << ", " << *i;
		out << endl;
		tgname = "error";
		inErrorDS = true;
	}
	out << prefix << ": acquire to " << tgname << " dataset" << endl;

	// Test acquire into the dataset
	if (!target) target = cfg.section(tgname);

	Dispatcher::Outcome result;
	switch (WritableDataset::testAcquire(*target, md, out))
	{
		case WritableDataset::ACQ_OK:
			result = inErrorDS ? DISP_ERROR : DISP_OK;
			break;
		case WritableDataset::ACQ_ERROR_DUPLICATE:
			// If insertion in the designed dataset failed, insert in the
			// error dataset
			target = cfg.section("duplicates");
			if (!target) target = cfg.section("error");
			result = WritableDataset::testAcquire(*target, md, out) == WritableDataset::ACQ_OK ? DISP_DUPLICATE_ERROR : DISP_NOTWRITTEN;
			break;
		case WritableDataset::ACQ_ERROR:
		default:
			// If insertion in the designed dataset failed, insert in the
			// error dataset
			target = cfg.section("error");
			result = WritableDataset::testAcquire(*target, md, out) == WritableDataset::ACQ_OK ? DISP_ERROR : DISP_NOTWRITTEN;
			break;
	}
	return result;
}

void TestDispatcher::flush()
{
	// Reset the metadata counter
	m_count = 0;
}

}
// vim:set ts=4 sw=4:
