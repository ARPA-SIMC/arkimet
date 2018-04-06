#ifndef ARKI_SEGMENT_GZIDX_H
#define ARKI_SEGMENT_GZIDX_H

/// Base class for unix fd based read/write functions

#include <arki/segment.h>
#include <string>

namespace arki {
struct Reader;

namespace segment {
namespace gzidx {

class Checker : public segment::Checker
{
protected:
    std::string gzabspath;
    std::string gzidxabspath;

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
    void move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) override;

public:
    Checker(const std::string& root, const std::string& relpath, const std::string& abspath);

    const char* type() const override;
    bool single_file() const override;

    bool exists_on_disk() override;
    time_t timestamp() override;
    size_t size() override;

    State check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick=true) override;
    size_t remove() override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) override;

    void test_truncate(size_t offset) override;
    void test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx) override;
    void test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx) override;
    void test_corrupt(const metadata::Collection& mds, unsigned data_idx) override;

    /**
     * Create a gz segment with the data in mds
     */
    static void create(const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags=0);
};

bool can_store(const std::string& format);

}

namespace gzidxlines {

class Checker : public gzidx::Checker
{
public:
    using gzidx::Checker::Checker;

    /**
     * Create a gz lines segment with the data in mds
     */
    static void create(const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags=0);
};

bool can_store(const std::string& format);

}

}
}
#endif


