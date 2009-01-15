#ifndef ARKI_RUNTIME_H
#define ARKI_RUNTIME_H

/*
 * runtime - Common code used in most xgribarch executables
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

#include <wibble/commandline/parser.h>
#include <wibble/stream/posix.h>
#include <arki/metadata.h>
#include <arki/configfile.h>
#include <fstream>
#include <string>
#include <vector>
#include <map>

namespace arki {

class Matcher;
class Dataset;
class ReadonlyDataset;
class Summary;
class Dispatcher;
class Formatter;

namespace runtime {
class MetadataDispatch;

struct FlushableMetadataConsumer : public MetadataConsumer
{
	size_t countMetadata;
	size_t countSummary;

	FlushableMetadataConsumer();
	virtual ~FlushableMetadataConsumer();
	virtual bool outputSummary(Summary& s) = 0;
	virtual void flush();
};

/**
 * Open an output file.
 *
 * If there is a commandline parameter available in the parser, use that as a
 * file name; else use the standard output.
 *
 * If a file is used, in case it already exists it will be overwritten.
 */
class Output
{
	wibble::stream::PosixBuf posixBuf;
	std::ostream *m_out;
	std::string m_name;

	void openFile(const std::string& fname);
	void openStdout();

public:
	Output(const std::string& fileName);
	Output(wibble::commandline::Parser& opts);
	Output(wibble::commandline::StringOption& opt);
	~Output();

	std::ostream& stream() { return *m_out; }
	const std::string& name() const { return m_name; }
};

struct OutputOptions : public wibble::commandline::StandardParserWithManpage
{
	enum ConfigType {
		/// Do not attempt to read dataset configuration from anywhere
		NONE,
		/// Read dataset configuration from --config/-C switches
		SWITCH,
		/// Read dataset configuration from --config/-C switches and from
		/// non-switch commandline arguments
		PARMS
	};

	wibble::commandline::BoolOption* yaml;
	wibble::commandline::BoolOption* annotate;
	wibble::commandline::BoolOption* dataInline;
	wibble::commandline::BoolOption* dataOnly;
	wibble::commandline::BoolOption* summary;
	wibble::commandline::StringOption* outfile;
	wibble::commandline::StringOption* postprocess;
	wibble::commandline::StringOption* report;
	wibble::commandline::StringOption* sort;
	wibble::commandline::VectorOption<wibble::commandline::String>* cfgfiles;
	ConfigFile cfg;
	ConfigType cfgtype;

	OutputOptions(const std::string& name, int mansection = 1, ConfigType cfgtype = NONE);
	~OutputOptions();

	// Parse leading parameters before we consume all config file parameters if cfgtype = PARMS
	virtual void parseLeadingParameters() {}

	bool parse(int argc, const char* argv[]);

	void createConsumer();

	void processDataset(ReadonlyDataset& ds, const Matcher& m);

	// Consumer to use for output of metadata
	FlushableMetadataConsumer& consumer()
	{
		if (!m_consumer) createConsumer();
		return *m_consumer;
	}
	// Output stream
	Output& output() { return *m_output; }

	/// Return true if the selected output style is raw binary metadata
	bool isBinaryMetadataOutput() const;

protected:
	Formatter* m_formatter;
	Output* m_output;
	FlushableMetadataConsumer* m_consumer;
};

struct ScanOptions : public OutputOptions
{
	wibble::commandline::BoolOption* verbose;
	wibble::commandline::StringOption* files;
	wibble::commandline::VectorOption<wibble::commandline::String>* dispatch;
	wibble::commandline::VectorOption<wibble::commandline::String>* testdispatch;

	ConfigFile* cfg;
	MetadataDispatch* mdispatch;

	ScanOptions(const std::string& name, int mansection = 1);
	~ScanOptions();

	bool parse(int argc, const char* argv[]);

	// Consumer to use for the scanned metadata
	MetadataConsumer& consumer();

	// Flush the imports done so far
	void flush();
};


/**
 * Open an input file.
 *
 * If there is a commandline parameter available in the parser, use that as a
 * file name; else use the standard input.
 */
class Input
{
	std::istream* m_in;
	std::ifstream m_file_in;
	std::string m_name;

public:
	Input(wibble::commandline::Parser& opts);
	Input(const std::string& file);

	std::istream& stream() { return *m_in; }
	const std::string& name() const { return m_name; }
};

/**
 * Parse the config file with the given name into the ConfigFile object 'cfg'.
 *
 * Note: use ReadonlyDataset::readConfig to read dataset configuration
 */
void parseConfigFile(ConfigFile& cfg, const std::string& fileName);

/**
 * Parse the config files indicated by the given commandline option.
 *
 * Return true if at least one config file was found in \a files
 */
bool parseConfigFiles(ConfigFile& cfg, const wibble::commandline::VectorOption<wibble::commandline::String>& files);

/**
 * Parse the config files from the remaining commandline arguments
 *
 * Return true if at least one config file was found in \a opts
 */
bool parseConfigFiles(ConfigFile& cfg, wibble::commandline::Parser& opts);

/**
 * Read the Matcher alias database.
 *
 * The file named in the given StringOption (if any) is tried first.
 * Otherwise the file given in the environment variable XGAR_ALIASES is tried.
 * Else, $(sysconfdir)/xgribarch/match-alias.conf is tried.
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
 * Parse a Matcher expression, either from a file specified in a commandline
 * switch (optional, ignored if fromFile == 0), or from a commandline argument.
 */
void readQuery(Matcher& expr, wibble::commandline::Parser& opts, wibble::commandline::StringOption* fromFile = 0);

/**
 * Dispatch metadata
 */
struct MetadataDispatch : public MetadataConsumer
{
	const ConfigFile& cfg;
	Dispatcher* dispatcher;
	MetadataConsumer& next;

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

	MetadataDispatch(const ConfigFile& cfg, MetadataConsumer& next, bool test=false);
	virtual ~MetadataDispatch();

	virtual bool operator()(Metadata& md);

	// Flush all imports done so far
	void flush();

	// Format a summary of the import statistics so far
	std::string summarySoFar() const;
};


}
}

// vim:set ts=4 sw=4:
#endif
