#ifndef ARKI_DATASET_FILE_H
#define ARKI_DATASET_FILE_H

/*
 * dataset/file - Dataset on a single file
 *
 * Copyright (C) 2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
class MetadataConsumer;
class Matcher;

namespace dataset {

/**
 * Dataset on a single file
 */
class File : public ReadonlyDataset
{
protected:
	std::string m_pathname;

public:
	File(const ConfigFile& cfg);

	const std::string& pathname() const { return m_pathname; }

	virtual void scan(const Matcher& matcher, MetadataConsumer& consumer, bool inlineData = false) = 0;

	virtual void queryMetadata(const Matcher& matcher, bool withData, MetadataConsumer& consumer);
	virtual void querySummary(const Matcher& matcher, Summary& summary);
	virtual void queryBytes(const Matcher& matcher, std::ostream& out, ByteQuery qtype = BQ_DATA, const std::string& param = std::string());

	static void readConfig(const std::string& path, ConfigFile& cfg);

	/**
	 * Instantiate an appropriate File dataset for the given configuration
	 */
	static File* create(const ConfigFile& cfg);
};

class IfstreamFile : public File
{
protected:
	std::ifstream m_file;

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

	virtual void scan(const Matcher& matcher, MetadataConsumer& consumer, bool inlineData = false);
};

class YamlFile : public IfstreamFile
{
public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	YamlFile(const ConfigFile& cfg);
	virtual ~YamlFile();

	virtual void scan(const Matcher& matcher, MetadataConsumer& consumer, bool inlineData = false);
};

template<typename Scanner>
class RawFile : public File
{
public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	RawFile(const ConfigFile& cfg);
	virtual ~RawFile();

	virtual void scan(const Matcher& matcher, MetadataConsumer& consumer, bool inlineData = false);
};

}
}

// vim:set ts=4 sw=4:
#endif
