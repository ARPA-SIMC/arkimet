#ifndef ARKI_DATASET_FILE_H
#define ARKI_DATASET_FILE_H

/*
 * dataset/file - Dataset on a single file
 *
 * Copyright (C) 2008--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset.h>
#include <fstream>
#include <string>

namespace arki {
class ConfigFile;
class Metadata;
class Matcher;

namespace dataset {

/**
 * Dataset on a single file
 */
class File : public ReadonlyDataset
{
protected:
	std::string m_pathname;
	std::string m_format;

    void queryData(const dataset::DataQuery& q, metadata::Eater& consumer) override;

public:
	File(const ConfigFile& cfg);

	const std::string& pathname() const { return m_pathname; }

    virtual void scan(const dataset::DataQuery& q, metadata::Eater& consumer) = 0;

    void querySummary(const Matcher& matcher, Summary& summary) override;

	static void readConfig(const std::string& path, ConfigFile& cfg);

	/**
	 * Instantiate an appropriate File dataset for the given configuration
	 */
	static File* create(const ConfigFile& cfg);
};

class IfstreamFile : public File
{
protected:
	std::istream* m_file;
	bool m_close;

public:
	IfstreamFile(const ConfigFile& cfg);
	virtual ~IfstreamFile();

};

class ArkimetFile : public IfstreamFile
{
public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	ArkimetFile(const ConfigFile& cfg);
	virtual ~ArkimetFile();

    void scan(const dataset::DataQuery& q, metadata::Eater& consumer) override;
};

class YamlFile : public IfstreamFile
{
public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	YamlFile(const ConfigFile& cfg);
	virtual ~YamlFile();

    void scan(const dataset::DataQuery& q, metadata::Eater& consumer) override;
};

class RawFile : public File
{
public:
	// Initialise the dataset with the information from the configuration in 'cfg'
	RawFile(const ConfigFile& cfg);
	virtual ~RawFile();

    void scan(const dataset::DataQuery& q, metadata::Eater& consumer) override;
};

}
}

// vim:set ts=4 sw=4:
#endif
