#ifndef ARKI_DATASET_SEGMENT_MANAGERS_H
#define ARKI_DATASET_SEGMENT_MANAGERS_H

#include <arki/dataset/segment.h>
#include <arki/segment/fwd.h>

namespace arki {

namespace dataset {
class Reporter;

struct BaseManager : public SegmentManager
{
    bool mockdata;

    BaseManager(const std::string& root, bool mockdata=false);
};

/// Segment manager that picks the right readers/writers based on file types
struct AutoManager : public BaseManager
{
    AutoManager(const std::string& root, bool mockdata=false);

    std::shared_ptr<segment::Writer> create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname);

    std::shared_ptr<segment::Checker> create_checker_for_format(const std::string& format, const std::string& relname, const std::string& absname);

    void scan_dir(std::function<void(const std::string& relname)> dest) override;
};

/// Segment manager that always picks directory segments
struct ForceDirManager : public BaseManager
{
    ForceDirManager(const std::string& root);

    std::shared_ptr<segment::Writer> create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname) override;

    std::shared_ptr<segment::Checker> create_checker_for_format(const std::string& format, const std::string& relname, const std::string& absname) override;

    void scan_dir(std::function<void(const std::string& relname)> dest) override;
};

/// Segment manager that always uses hole file segments
struct HoleDirManager : public ForceDirManager
{
    HoleDirManager(const std::string& root);

    std::shared_ptr<segment::Writer> create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname) override;
};

}
}
#endif

