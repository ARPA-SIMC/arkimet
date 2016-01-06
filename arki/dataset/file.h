#ifndef ARKI_DATASET_FILE_H
#define ARKI_DATASET_FILE_H

/// dataset/file - Dataset on a single file

#include <arki/defs.h>
#include <arki/file.h>
#include <arki/dataset.h>
#include <string>

namespace arki {
class ConfigFile;
class Metadata;
class Matcher;

namespace dataset {

/**
 * Dataset on a single file
 */
class File : public Reader
{
protected:
    std::string m_format;

public:
    File(const ConfigFile& cfg);

    std::string type() const override { return "file"; }

    virtual std::string pathname() const = 0;

    virtual void scan(const dataset::DataQuery& q, metadata_dest_func dest) = 0;

    void query_data(const dataset::DataQuery& q, metadata_dest_func) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;

	static void readConfig(const std::string& path, ConfigFile& cfg);

	/**
	 * Instantiate an appropriate File dataset for the given configuration
	 */
	static File* create(const ConfigFile& cfg);
};

class FdFile : public File
{
protected:
    NamedFileDescriptor* fd = nullptr;

public:
    FdFile(const ConfigFile& cfg);
    virtual ~FdFile();

    std::string pathname() const override;
};

class ArkimetFile : public FdFile
{
public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	ArkimetFile(const ConfigFile& cfg);
	virtual ~ArkimetFile();

    void scan(const dataset::DataQuery& q, metadata_dest_func consumer) override;
};

class YamlFile : public FdFile
{
protected:
    LineReader* reader;

public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	YamlFile(const ConfigFile& cfg);
	virtual ~YamlFile();

    void scan(const dataset::DataQuery& q, metadata_dest_func consumer) override;
};

class RawFile : public File
{
protected:
    std::string m_pathname;

public:
	// Initialise the dataset with the information from the configuration in 'cfg'
	RawFile(const ConfigFile& cfg);
	virtual ~RawFile();

    void scan(const dataset::DataQuery& q, metadata_dest_func consumer) override;

    std::string pathname() const override;
};

}
}
#endif
