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

    // Instantiate the right Segment implementation for a segment that already
    // exists. Returns 0 if the segment does not exist.
    std::shared_ptr<segment::Writer> create_writer_for_existing_segment(const std::string& format, const std::string& relname, const std::string& absname, bool nullptr_on_error=false);

    // Instantiate the right Segment implementation for a segment that already
    // exists. Returns 0 if the segment does not exist.
    std::shared_ptr<segment::Checker> create_checker_for_existing_segment(const std::string& format, const std::string& relname, const std::string& absname, bool nullptr_on_error=false);

    Pending repack(const std::string& relname, metadata::Collection& mds, unsigned test_flags=0);

    segment::State check(dataset::Reporter& reporter, const std::string& ds, const std::string& relname, const metadata::Collection& mds, bool quick=true);

    void test_truncate(const std::string& relname, size_t offset);
};

/// Segment manager that picks the right readers/writers based on file types
struct AutoManager : public BaseManager
{
    AutoManager(const std::string& root, bool mockdata=false);

    std::shared_ptr<segment::Writer> create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname);

    std::shared_ptr<segment::Checker> create_checker_for_format(const std::string& format, const std::string& relname, const std::string& absname);

    bool exists(const std::string& relpath) const override;

    void scan_dir(std::function<void(const std::string& relname)> dest) override;
};

/// Segment manager that always picks directory segments
struct ForceDirManager : public BaseManager
{
    ForceDirManager(const std::string& root);

    std::shared_ptr<segment::Writer> create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname) override;

    std::shared_ptr<segment::Checker> create_checker_for_format(const std::string& format, const std::string& relname, const std::string& absname) override;

    bool exists(const std::string& relpath) const override;

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

