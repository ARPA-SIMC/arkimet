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

class FileConfig : public dataset::Config
{
public:
    FileConfig(const ConfigFile& cfg);

    std::string pathname;
    std::string format;

    Reader* create_reader() const override;

    static std::shared_ptr<const FileConfig> create(const ConfigFile& cfg);
};

/**
 * Dataset on a single file
 */
class File : public Reader
{
protected:
    virtual void scan(const dataset::DataQuery& q, metadata_dest_func dest) = 0;

public:
    std::string type() const override { return "file"; }
    const FileConfig& config() const override = 0;

    void query_data(const dataset::DataQuery& q, metadata_dest_func) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;

    static void readConfig(const std::string& path, ConfigFile& cfg);
};

class FdFile : public File
{
protected:
    std::shared_ptr<const FileConfig> m_config;
    NamedFileDescriptor* fd = nullptr;

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

    void scan(const dataset::DataQuery& q, metadata_dest_func consumer) override;
};

class YamlFile : public FdFile
{
protected:
    LineReader* reader;

public:
    using FdFile::FdFile;

    // Initialise the dataset with the information from the configurationa in 'cfg'
    YamlFile(std::shared_ptr<FileConfig> config);
    virtual ~YamlFile();

    void scan(const dataset::DataQuery& q, metadata_dest_func consumer) override;
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

    void scan(const dataset::DataQuery& q, metadata_dest_func consumer) override;
};

}
}
#endif
