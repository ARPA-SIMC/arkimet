/**
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
#ifndef ARKI_DATASET_TESTUTILS_H
#define ARKI_DATASET_TESTUTILS_H

#include <arki/tests/test-utils.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/dataset/maintenance.h>
#include <arki/sort.h>
#include <vector>
#include <string>
#include <sstream>

namespace arki {
struct Metadata;
struct Dispatcher;
struct ReadonlyDataset;
struct WritableDataset;

namespace dataset {
struct Local;
struct WritableLocal;

namespace ondisk2 {
struct Reader;
struct Writer;
}

namespace simple {
struct Reader;
struct Writer;
}
}

namespace tests {
#define ensure_dispatches(x, y, z) arki::tests::impl_ensure_dispatches(wibble::tests::Location(__FILE__, __LINE__, #x ", " #y), (x), (y), (z))
void impl_ensure_dispatches(const wibble::tests::Location& loc, Dispatcher& dispatcher, Metadata& md, metadata::Consumer& mdc);

struct OutputChecker : public std::stringstream
{
	std::vector<std::string> lines;
	bool split;

	// Split the output into lines if it has not been done yet
	void splitIfNeeded();

	// Join the split and marked lines
	std::string join() const;
	
	OutputChecker();

	void ignore_line_containing(const std::string& needle);

#define ensure_line_contains(x) impl_ensure_line_contains(wibble::tests::Location(__FILE__, __LINE__, "look for " #x), (x))
#define inner_ensure_line_contains(x) impl_ensure_line_contains(wibble::tests::Location(loc, __FILE__, __LINE__, "look for " #x), (x))
	void impl_ensure_line_contains(const wibble::tests::Location& loc, const std::string& needle);

#define ensure_all_lines_seen() impl_ensure_all_lines_seen(wibble::tests::Location(__FILE__, __LINE__, "all lines seen"))
#define inner_ensure_all_lines_seen() impl_ensure_all_lines_seen(wibble::tests::Location(loc, __FILE__, __LINE__, "all lines seen"))
	void impl_ensure_all_lines_seen(const wibble::tests::Location& loc);
};

struct ForceSqlite
{
	bool old;

	ForceSqlite(bool val = true);
	~ForceSqlite();
};

// Base class for dataset tests
struct DatasetTest : public dataset::maintenance::MaintFileVisitor
{
	// Default dataset configuration (to be filled by subclasser)
	ConfigFile cfg;

	// Little dirty hack: implement MaintFileVisitor so we can conveniently
        // access State constants
	virtual void operator()(const std::string& file, State state) {}

	// Return the file name of the index of the current dataset
	std::string idxfname(const ConfigFile* wcfg = 0) const;
	// Return the file name of the archive index of the current dataset
	std::string arcidxfname() const;

	ReadonlyDataset* makeReader(const ConfigFile* wcfg = 0);
	WritableDataset* makeWriter(const ConfigFile* wcfg = 0);
	dataset::Local* makeLocalReader(const ConfigFile* wcfg = 0);
	dataset::WritableLocal* makeLocalWriter(const ConfigFile* wcfg = 0);
	dataset::ondisk2::Reader* makeOndisk2Reader(const ConfigFile* wcfg = 0);
	dataset::ondisk2::Writer* makeOndisk2Writer(const ConfigFile* wcfg = 0);
	dataset::simple::Reader* makeSimpleReader(const ConfigFile* wcfg = 0);
	dataset::simple::Writer* makeSimpleWriter(const ConfigFile* wcfg = 0);
	
	// Return the number of days passed from the given date until today
	int days_since(int year, int month, int day);

	// Clean the dataset directory
	void clean(const ConfigFile* wcfg = 0);

	// Import a file
	void import(const ConfigFile* wcfg = 0, const std::string& testfile = "inbound/test.grib1");

	// Recreate the dataset importing data into it
	void clean_and_import(const ConfigFile* wcfg = 0, const std::string& testfile = "inbound/test.grib1");

#define ensure_maint_clean(...) impl_ensure_maint_clean(wibble::tests::Location(__FILE__, __LINE__, #__VA_ARGS__), ##__VA_ARGS__)
	void impl_ensure_maint_clean(const wibble::tests::Location& loc, size_t filecount, const ConfigFile* wcfg = 0);

#define ensure_localds_clean(...) impl_ensure_localds_clean(wibble::tests::Location(__FILE__, __LINE__, #__VA_ARGS__), ##__VA_ARGS__)
	void impl_ensure_localds_clean(const wibble::tests::Location& loc, size_t filecount, size_t resultcount, const ConfigFile* wcfg = 0);
};

struct TempConfig
{
	ConfigFile& cfg;
	ConfigFile backup;

	TempConfig(ConfigFile& cfg)
		: cfg(cfg), backup(cfg)
	{
	}

	TempConfig(ConfigFile& cfg, const std::string& key, const std::string& val)
		: cfg(cfg), backup(cfg)
	{
		override(key, val);
	}

	~TempConfig()
	{
		cfg = backup;
	}

	void override(const std::string& key, const std::string& val)
	{
		cfg.setValue(key, val);
	}
};

}

struct MaintenanceCollector : public dataset::maintenance::MaintFileVisitor
{
	std::map <std::string, State> fileStates;
	size_t counts[STATE_MAX];
	static const char* names[];
	std::set<State> checked;

	MaintenanceCollector();

	void clear();
	bool isClean() const;
	virtual void operator()(const std::string& file, State state);
	void dump(std::ostream& out) const;
	size_t count(State s);
	std::string remaining() const;
};

struct OrderCheck : public metadata::Consumer
{
    refcounted::Pointer<sort::Compare> order;
    Metadata old;
    bool first;

    OrderCheck(const std::string& order);
    virtual ~OrderCheck();
    virtual bool operator()(Metadata& md);
};

}

#endif
