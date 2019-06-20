#ifndef ARKI_DATASET_FILE_H
#define ARKI_DATASET_FILE_H

/// dataset/file - Dataset on a single file

#include <arki/defs.h>
#include <arki/core/file.h>
#include <arki/dataset.h>
#include <string>

namespace arki {
class ConfigFile;
class Metadata;
class Matcher;

namespace dataset {

class FileConfig : public dataset::Config
{
public:
    FileConfig(const ConfigFile& cfg);

    std::string pathname;
    std::string format;

    std::unique_ptr<Reader> create_reader() const override;

    static std::shared_ptr<const FileConfig> create(const ConfigFile& cfg);
};

/**
 * Dataset on a single file
 */
class File : public Reader
{
protected:
    virtual bool scan(const dataset::DataQuery& q, metadata_dest_func dest) = 0;

public:
    std::string type() const override { return "file"; }
    const FileConfig& config() const override = 0;

    bool query_data(const dataset::DataQuery& q, metadata_dest_func) override;

    static void read_config(const std::string& path, ConfigFile& cfg);
};

class FdFile : public File
{
protected:
    std::shared_ptr<const FileConfig> m_config;
    core::File fd;

public:
    FdFile(std::shared_ptr<const FileConfig> config);
    virtual ~FdFile();

    const FileConfig& config() const override { return *m_config; }
};

class ArkimetFile : public FdFile
{
public:
    using FdFile::FdFile;

    virtual ~ArkimetFile();

    bool scan(const dataset::DataQuery& q, metadata_dest_func consumer) override;
};

class YamlFile : public FdFile
{
protected:
    core::LineReader* reader;

public:
    // Initialise the dataset with the information from the configurationa in 'cfg'
    YamlFile(std::shared_ptr<const FileConfig> config);
    virtual ~YamlFile();

    bool scan(const dataset::DataQuery& q, metadata_dest_func consumer) override;
};

class RawFile : public File
{
protected:
    std::shared_ptr<const FileConfig> m_config;

public:
    // Initialise the dataset with the information from the configuration in 'cfg'
    RawFile(std::shared_ptr<const FileConfig> config);
    virtual ~RawFile();

    const FileConfig& config() const override { return *m_config; }

    bool scan(const dataset::DataQuery& q, metadata_dest_func consumer) override;
};

}
}
#endif
