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
#include <arki/metadata.h>
#include <arki/utils/metadata.h>
#include <arki/matcher.h>
#include <arki/configfile.h>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <sys/time.h>

namespace arki {

class Matcher;
class Dataset;
class ReadonlyDataset;
class Summary;
class Dispatcher;
class Formatter;

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
	wibble::commandline::StringOption* restr;
	wibble::commandline::StringOption* exprfile;
	wibble::commandline::StringOption* outfile;
	wibble::commandline::StringOption* postprocess;
	wibble::commandline::StringOption* report;
	wibble::commandline::StringOption* sort;
	wibble::commandline::StringOption* files;
	wibble::commandline::StringOption* gridspace;
	wibble::commandline::StringOption* moveok;
	wibble::commandline::StringOption* moveko;
	wibble::commandline::StringOption* movework;
	wibble::commandline::VectorOption<wibble::commandline::String>* cfgfiles;
	wibble::commandline::VectorOption<wibble::commandline::String>* dispatch;
	wibble::commandline::VectorOption<wibble::commandline::String>* testdispatch;

	ConfigFile inputInfo;
	ConfigFile dispatchInfo;
	Matcher query;	// TODO
	Output* output;
	DatasetProcessor* processor;
	MetadataDispatch* dispatcher;
	std::stringstream gridspace_def;

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
 * Parse the config file with the given name into the ConfigFile object 'cfg'.
 *
 * Note: use ReadonlyDataset::readConfig to read dataset configuration
 */
void parseConfigFile(ConfigFile& cfg, const std::string& fileName);

/**
 * Parse the config files from the remaining commandline arguments
 *
 * Return true if at least one config file was found in \a opts
 */
bool parseConfigFiles(ConfigFile& cfg, wibble::commandline::Parser& opts);

/**
 * Parse the config files indicated by the given commandline option.
 *
 * Return true if at least one config file was found in \a files
 */
bool parseConfigFiles(ConfigFile& cfg, const wibble::commandline::VectorOption<wibble::commandline::String>& files);

/**
 * Parse a comma separated restrict list into a set of strings
 */
std::set<std::string> parseRestrict(const std::string& str);

struct Restrict
{
	std::set<std::string> wanted;

	Restrict(const std::string& str) : wanted(parseRestrict(str)) {}

	bool is_allowed(const std::string& str);
	bool is_allowed(const std::set<std::string>& names);
	bool is_allowed(const ConfigFile& cfg);
	void remove_unallowed(ConfigFile& cfg);
};

/**
 * Read the Matcher alias database.
 *
 * The file named in the given StringOption (if any) is tried first.
 * Otherwise the file given in the environment variable XGAR_ALIASES is tried.
 * Else, $(sysconfdir)/arkimet/match-alias.conf is tried.
 * Else, nothing is loaded.
 *
 * The alias database is kept statically for all the lifetime of the program,
 * and is automatically used by readQuery.
 */
void readMatcherAliasDatabase(wibble::commandline::StringOption* file = 0);

/**
 * Read the content of an rc directory, returning all the files found, sorted
 * alphabetically by name.
 */
std::vector<std::string> rcFiles(const std::string& nameInConfdir, const std::string& nameInEnv, wibble::commandline::StringOption* dir = 0);

/**
 * Read the content of an rc directory, by concatenating all non-hidden,
 * non-backup files in it, sorted alphabetically by name
 */
std::string readRcDir(const std::string& nameInConfdir, const std::string& nameInEnv, wibble::commandline::StringOption* dir = 0);

struct FileInfo
{
	std::string pathname;
	size_t length;

	FileInfo(const std::string& pathname, const size_t& length):
		pathname(pathname), length(length) {}
};

struct SourceCode : public std::vector<FileInfo>
{
	std::string code;
};

/**
 * Read the content of an rc directory, by concatenating all non-hidden,
 * non-backup files in it, sorted alphabetically by name.  It also returns
 * information about what parts of the string belong to what file, which can be
 * used to map a string location to its original position.
 */
SourceCode readSourceFromRcDir(const std::string& nameInConfdir, const std::string& nameInEnv, wibble::commandline::StringOption* dir = 0);

/**
 * Dispatch metadata
 */
struct MetadataDispatch : public MetadataConsumer
{
	const ConfigFile& cfg;
	Dispatcher* dispatcher;
	arki::utils::metadata::Collector results;
	DatasetProcessor& next;
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
