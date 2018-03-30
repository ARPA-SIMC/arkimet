#ifndef ARKI_DATASET_SEGMENT_TAR_H
#define ARKI_DATASET_SEGMENT_TAR_H

/// Base class for unix fd based read/write functions

#include <arki/libconfig.h>
#include <arki/dataset/segment.h>
#include <arki/core/file.h>
#include <string>
#include <vector>

namespace arki {
struct Reader;

namespace dataset {
namespace segment {
namespace tar {

class Checker : public dataset::segment::Checker
{
protected:
    std::string tarabsname;
    void validate(Metadata& md, const scan::Validator& v) override;

    /**
     * If skip_validation is true, repack will skip validating the data that is
     * being read.
     *
     * This is only used during tests to support repacking files with mock data
     * inside. The files are made of filesystem holes, so the data that is read
     * from them is always zeroes.
     */
    Pending repack_impl(
            const std::string& rootdir,
            metadata::Collection& mds,
            bool skip_validation=false,
            unsigned test_flags=0);

public:
    Checker(const std::string& root, const std::string& relname, const std::string& absname);

    bool exists_on_disk() override;

    State check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick=true) override;
    size_t remove() override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) override;

    void test_truncate(size_t offset) override;
    void test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx) override;
    void test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx) override;
    void test_corrupt(const metadata::Collection& mds, unsigned data_idx) override;

    /**
     * Create a tar segment with the data in mds
     */
    static void create(const std::string& rootdir, const std::string& tarrelname, const std::string& tarabsname, metadata::Collection& mds, unsigned test_flags=0);
};

bool can_store(const std::string& format);

}
}
}
}
#endif

