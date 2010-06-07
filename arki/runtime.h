#ifndef ARKI_RUNTIME_H
#define ARKI_RUNTIME_H

/*
 * runtime - Common code used in most arkimet executables
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <wibble/commandline/parser.h>
#include <arki/runtime/io.h>
#include <arki/runtime/config.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/configfile.h>
#include <string>
#include <memory>
#include <sys/time.h>

namespace arki {

class Dataset;
class ReadonlyDataset;
class Summary;
class Dispatcher;
class Formatter;
class Targetfile;

namespace runtime {
class MetadataDispatch;

/**
 * Initialise the libraries that we use and parse the matcher alias database.
 */
void init();

struct DatasetProcessor
{
	virtual ~DatasetProcessor() {}

	virtual void process(ReadonlyDataset& ds, const std::string& name) = 0;
	virtual void end() {}
};


struct CommandLine : public wibble::commandline::StandardParserWithManpage
{
	wibble::commandline::OptionGroup* infoOpts;
	wibble::commandline::OptionGroup* inputOpts;
	wibble::commandline::OptionGroup* outputOpts;
	wibble::commandline::OptionGroup* dispatchOpts;

	wibble::commandline::BoolOption* verbose;
	wibble::commandline::BoolOption* debug;
	wibble::commandline::BoolOption* status;
	wibble::commandline::BoolOption* yaml;
	wibble::commandline::BoolOption* annotate;
	wibble::commandline::BoolOption* dataInline;
	wibble::commandline::BoolOption* dataOnly;
	wibble::commandline::BoolOption* summary;
	wibble::commandline::BoolOption* merged;
	wibble::commandline::BoolOption* ignore_duplicates;
	wibble::commandline::StringOption* restr;
	wibble::commandline::StringOption* exprfile;
	wibble::commandline::StringOption* qmacro;
	wibble::commandline::StringOption* outfile;
	wibble::commandline::StringOption* targetfile;
	wibble::commandline::StringOption* postprocess;
	wibble::commandline::StringOption* report;
	wibble::commandline::StringOption* sort;
	wibble::commandline::StringOption* files;
	wibble::commandline::StringOption* moveok;
	wibble::commandline::StringOption* moveko;
	wibble::commandline::StringOption* movework;
	wibble::commandline::VectorOption<wibble::commandline::ExistingFile>* postproc_data;
	wibble::commandline::VectorOption<wibble::commandline::String>* cfgfiles;
	wibble::commandline::VectorOption<wibble::commandline::String>* dispatch;
	wibble::commandline::VectorOption<wibble::commandline::String>* testdispatch;

	ConfigFile inputInfo;
	ConfigFile dispatchInfo;
	std::string strquery;
	Matcher query;
	Output* output;
	DatasetProcessor* processor;
	MetadataDispatch* dispatcher;

	CommandLine(const std::string& name, int mansection = 1);
	~CommandLine();

	/// Add scan-type options (--files, --moveok, --movework, --moveko)
	void addScanOptions();

	/// Add query-type options (--merged, --file, --cfgfiles)
	void addQueryOptions();

	/**
	 * Parse the command line
	 */
	bool parse(int argc, const char* argv[]);

	/**
	 * Set up processing after the command line has been parsed and
	 * additional tweaks have been applied
	 */
	void setupProcessing();

	/**
	 * End processing and flush partial data
	 */
	void doneProcessing();

	/**
	 * Open the next data source to process
	 *
	 * @return the pointer to the datasource, or 0 for no more datasets
	 */
	std::auto_ptr<ReadonlyDataset> openSource(ConfigFile& info);

	/**
	 * Process one data source
	 *
	 * If everything went perfectly well, returns true, else false. It can
	 * still throw an exception if things go wrong.
	 */
	bool processSource(ReadonlyDataset& ds, const std::string& name);

	/**
	 * Done working with one data source
	 *
	 * @param successful
	 *   true if everything went well, false if there were issues
	 * FIXME: put something that contains a status report instead, for
	 * FIXME: --status, as well as a boolean for moveok/moveko
	 */
	void closeSource(std::auto_ptr<ReadonlyDataset> ds, bool successful = true);
};

/**
 * Dispatch metadata
 */
struct MetadataDispatch : public metadata::Consumer
{
	const ConfigFile& cfg;
	Dispatcher* dispatcher;
	metadata::Collection results;
	DatasetProcessor& next;
	bool ignore_duplicates;
	bool reportStatus;

	// Used for timings. Read with gettimeofday at the beginning of a task,
	// and summarySoFar will report the elapsed time
	struct timeval startTime;

	// Incremented when a metadata is imported in the destination dataset.
	// Feel free to reset it to 0 anytime.
	int countSuccessful;

	// Incremented when a metadata is imported in the error dataset because it
	// had already been imported.  Feel free to reset it to 0 anytime.
	int countDuplicates;

	// Incremented when a metadata is imported in the error dataset.  Feel free
	// to reset it to 0 anytime.
	int countInErrorDataset;

	// Incremented when a metadata is not imported at all.  Feel free to reset
	// it to 0 anytime.
	int countNotImported;

	MetadataDispatch(const ConfigFile& cfg, DatasetProcessor& next, bool test=false);
	virtual ~MetadataDispatch();

	/**
	 * Dispatch the data from one source
	 *
	 * @returns true if all went well, false if any problem happend.
	 * It can still throw in case of big trouble.
	 */
	bool process(ReadonlyDataset& ds, const std::string& name);

	// Note: used only internally, but needs to be public
	virtual bool operator()(Metadata& md);

	// Flush all imports done so far
	void flush();

	// Format a summary of the import statistics so far
	std::string summarySoFar() const;

	// Set startTime to the current time
	void setStartTime();
};


}
}

// vim:set ts=4 sw=4:
#endif
