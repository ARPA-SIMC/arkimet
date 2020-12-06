#ifndef ARKI_DATASET_FILE_H
#define ARKI_DATASET_FILE_H

/// dataset/file - Dataset on a single file

#include <arki/defs.h>
#include <arki/core/file.h>
#include <arki/dataset.h>
#include <arki/dataset/impl.h>
#include <string>

namespace arki {
namespace dataset {
namespace file {

std::shared_ptr<core::cfg::Section> read_config(const std::string& path);
std::shared_ptr<core::cfg::Sections> read_configs(const std::string& path);

/// Dataset on a single file
class Dataset : public dataset::Dataset
{
public:
    std::string pathname;
    std::string format;

    Dataset(std::weak_ptr<Session> session, const core::cfg::Section& cfg);

    std::shared_ptr<dataset::Reader> create_reader() override;

    virtual bool scan(const dataset::DataQuery& q, metadata_dest_func dest) = 0;

    static std::shared_ptr<Dataset> from_config(std::shared_ptr<Session> session, const core::cfg::Section& cfg);
};

class Reader : public DatasetAccess<file::Dataset, dataset::Reader>
{
protected:
    bool impl_query_data(const dataset::DataQuery& q, metadata_dest_func) override;

public:
    using DatasetAccess::DatasetAccess;

    std::string type() const override { return "file"; }

    core::Interval get_stored_time_interval() override;
};

class FdFile : public Dataset
{
protected:
    core::File fd;

public:
    FdFile(std::shared_ptr<Session> session, const core::cfg::Section& cfg);
    virtual ~FdFile();
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
    YamlFile(std::shared_ptr<Session> session, const core::cfg::Section& cfg);
    virtual ~YamlFile();

    bool scan(const dataset::DataQuery& q, metadata_dest_func consumer) override;
};

class RawFile : public Dataset
{
public:
    using Dataset::Dataset;

    bool scan(const dataset::DataQuery& q, metadata_dest_func consumer) override;
};

}
}
}
#endif
