#ifndef ARKI_DATASET_SEGMENT_H
#define ARKI_DATASET_SEGMENT_H

/// Dataset segment read/write functions

#include <arki/libconfig.h>
#include <arki/defs.h>
#include <arki/core/fwd.h>
#include <arki/transaction.h>
#include <arki/segment.h>
#include <string>
#include <iosfwd>
#include <memory>

namespace arki {
class ConfigFile;

namespace scan {
class Validator;
}

namespace dataset {
class Reporter;
class Segment;

/// Manage instantiating the right readers/writers for a dataset
class SegmentManager
{
protected:
    std::string root;

    virtual std::shared_ptr<segment::Writer> create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname) = 0;
    virtual std::shared_ptr<segment::Checker> create_checker_for_format(const std::string& format, const std::string& relname, const std::string& absname) = 0;

public:
    SegmentManager(const std::string& root);
    virtual ~SegmentManager();

    std::shared_ptr<segment::Writer> get_writer(const std::string& relname);
    std::shared_ptr<segment::Writer> get_writer(const std::string& format, const std::string& relname);

    std::shared_ptr<segment::Checker> get_checker(const std::string& relname);
    std::shared_ptr<segment::Checker> get_checker(const std::string& format, const std::string& relname);

    /**
     * Repack the file relname, so that it contains only the data in mds, in
     * the same order as in mds.
     *
     * The source metadata in mds will be updated to point to the new file.
     */
    virtual Pending repack(const std::string& relname, metadata::Collection& mds, unsigned test_flags=0) = 0;

    /**
     * Check the given file against its expected set of contents.
     *
     * @returns the State with the state of the file
     */
    virtual segment::State check(dataset::Reporter& reporter, const std::string& ds, const std::string& relname, const metadata::Collection& mds, bool quick=true) = 0;

    /**
     * Truncate a file at the given offset
     *
     * This function is useful for implementing unit tests.
     */
    virtual void test_truncate(const std::string& relname, size_t offset) = 0;

    /**
     * Given the relative path of a segment, return true if it exists on disk
     */
    virtual bool exists(const std::string& relpath) const = 0;

    /**
     * Scan a dataset for data files, returning a set of pathnames relative to
     * root.
     */
    virtual void scan_dir(std::function<void(const std::string& relname)> dest) = 0;

    /// Create a Manager
    static std::unique_ptr<SegmentManager> get(const std::string& root, bool force_dir=false, bool mock_data=false);
};

}
}
#endif
