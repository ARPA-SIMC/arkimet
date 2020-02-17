#ifndef ARKI_DATASET_FILE_H
#define ARKI_DATASET_FILE_H

/// dataset/file - Dataset on a single file

#include <arki/defs.h>
#include <arki/core/file.h>
#include <arki/dataset.h>
#include <string>

namespace arki {
namespace dataset {
namespace file {

core::cfg::Section read_config(const std::string& path);
core::cfg::Sections read_configs(const std::string& path);

class Dataset : public dataset::Dataset
{
public:
    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::string pathname;
    std::string format;

    std::shared_ptr<Reader> create_reader() override;
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
    const Dataset& config() const override = 0;
    const Dataset& dataset() const override = 0;
    Dataset& dataset() override = 0;

    bool query_data(const dataset::DataQuery& q, metadata_dest_func) override;

    static core::cfg::Section read_config(const std::string& path) { return file::read_config(path); }
    static core::cfg::Sections read_configs(const std::string& path) { return file::read_configs(path); }
};

class FdFile : public File
{
protected:
    std::shared_ptr<Dataset> m_config;
    core::File fd;

public:
    FdFile(std::shared_ptr<Dataset> config);
    virtual ~FdFile();

    const Dataset& config() const override { return *m_config; }
    const Dataset& dataset() const override { return *m_config; }
    Dataset& dataset() override { return *m_config; }
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
    YamlFile(std::shared_ptr<Dataset> config);
    virtual ~YamlFile();

    bool scan(const dataset::DataQuery& q, metadata_dest_func consumer) override;
};

class RawFile : public File
{
protected:
    std::shared_ptr<Dataset> m_config;

public:
    // Initialise the dataset with the information from the configuration in 'cfg'
    RawFile(std::shared_ptr<Dataset> config);
    virtual ~RawFile();

    const Dataset& config() const override { return *m_config; }
    const Dataset& dataset() const override { return *m_config; }
    Dataset& dataset() override { return *m_config; }

    bool scan(const dataset::DataQuery& q, metadata_dest_func consumer) override;
};

}
}
}
#endif
