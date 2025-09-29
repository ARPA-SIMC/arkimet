#ifndef ARKI_DATASET_FILE_H
#define ARKI_DATASET_FILE_H

/// dataset/file - Dataset on a single file

#include <arki/core/file.h>
#include <arki/dataset.h>
#include <arki/dataset/impl.h>
#include <arki/defs.h>
#include <arki/segment.h>
#include <string>

namespace arki::dataset::file {

struct SegmentSession : public segment::Session
{
protected:
    std::shared_ptr<segment::Reader> create_segment_reader(
        std::shared_ptr<const Segment> segment,
        std::shared_ptr<const core::ReadLock> lock) const override;

public:
    using segment::Session::Session;

    std::shared_ptr<segment::Writer>
    segment_writer(std::shared_ptr<const Segment> segment,
                   std::shared_ptr<core::AppendLock> lock) const override;
    std::shared_ptr<segment::Checker>
    segment_checker(std::shared_ptr<const Segment> segment,
                    std::shared_ptr<core::CheckLock> lock) const override;
};

std::shared_ptr<core::cfg::Section>
read_config(const std::filesystem::path& path);
std::shared_ptr<core::cfg::Section>
read_config(const std::filesystem::path& path, DataFormat format);
std::shared_ptr<core::cfg::Sections>
read_configs(const std::filesystem::path& path);
std::shared_ptr<core::cfg::Section>
read_config(const std::string& prefix, const std::filesystem::path& path);
std::shared_ptr<core::cfg::Sections>
read_configs(const std::string& prefix, const std::filesystem::path& path);

/// Dataset on a single file
class Dataset : public dataset::Dataset
{
public:
    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::shared_ptr<dataset::Reader> create_reader() override;

    virtual bool scan(const query::Data& q, metadata_dest_func dest) = 0;

    static std::shared_ptr<Dataset>
    from_config(std::shared_ptr<Session> session,
                const core::cfg::Section& cfg);
};

class SegmentDataset : public Dataset
{
public:
    std::shared_ptr<segment::Session> segment_session;
    std::shared_ptr<Segment> segment;

    SegmentDataset(std::shared_ptr<Session> session,
                   const core::cfg::Section& cfg);

    bool scan(const query::Data& q, metadata_dest_func consumer) override;
};

class Reader : public DatasetAccess<file::Dataset, dataset::Reader>
{
protected:
    bool impl_query_data(const query::Data& q, metadata_dest_func) override;

public:
    using DatasetAccess::DatasetAccess;

    std::string type() const override { return "file"; }

    core::Interval get_stored_time_interval() override;
};

class FdFile : public Dataset
{
protected:
    std::shared_ptr<segment::Session> segment_session;
    core::File fd;

    void ensure_data_is_accessible(Metadata& md);

public:
    std::filesystem::path path;

    FdFile(std::shared_ptr<Session> session, const core::cfg::Section& cfg);
    virtual ~FdFile();
};

class ArkimetFile : public FdFile
{
public:
    using FdFile::FdFile;

    virtual ~ArkimetFile();

    bool scan(const query::Data& q, metadata_dest_func consumer) override;
};

class YamlFile : public FdFile
{
protected:
    core::LineReader* reader;

public:
    // Initialise the dataset with the information from the configurationa in
    // 'cfg'
    YamlFile(std::shared_ptr<Session> session, const core::cfg::Section& cfg);
    virtual ~YamlFile();

    bool scan(const query::Data& q, metadata_dest_func consumer) override;
};

} // namespace arki::dataset::file
#endif
