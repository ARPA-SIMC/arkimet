#ifndef ARKI_DATASET_SEGMENT_LINES_H
#define ARKI_DATASET_SEGMENT_LINES_H

/// Read/write functions for data blobs with newline separators
#include <arki/libconfig.h>
#include <arki/dataset/segment/fd.h>
#include <string>

namespace arki {
namespace dataset {
namespace segment {
namespace lines {

class Writer : public fd::Writer
{
public:
    Writer(const std::string& root, const std::string& relname, const std::string& absname, std::shared_ptr<core::lock::Policy> lock_policy, int mode=0);
};

class Checker : public fd::Checker
{
protected:
    std::unique_ptr<fd::Writer> make_tmp_segment(const std::string& relname, const std::string& absname) override;
    void open() override;

public:
    using fd::Checker::Checker;

    State check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick=true) override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) override;
};

}
}
}
}
#endif
