#ifndef ARKI_DATASET_DATA_FD_H
#define ARKI_DATASET_DATA_FD_H

/// Base class for unix fd based read/write functions

#include <arki/libconfig.h>
#include <arki/dataset/segment.h>
#include <arki/utils/sys.h>
#include <string>

namespace arki {
namespace dataset {
namespace segment {
namespace fd {

class Segment : public dataset::Segment
{
protected:
    utils::sys::File fd;

public:
    Segment(const std::string& relname, const std::string& absname);
    ~Segment();

    void open();
    void truncate_and_open();
    void close();
    void lock();
    void unlock();
    off_t wrpos();
    virtual void write(const std::vector<uint8_t>& buf);
    void fdtruncate(off_t pos);
    State check_fd(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, unsigned max_gap=0, bool quick=true);

    size_t remove() override;
    void truncate(size_t offset) override;
    void validate(Metadata& md, const scan::Validator& v) override;

    /**
     * If skip_validation is true, repack will skip validating the data that is
     * being read.
     *
     * This is only used during tests to support repacking files with mock data
     * inside. The files are made of filesystem holes, so the data that is read
     * from them is always zeroes.
     */
    static Pending repack(
            const std::string& rootdir,
            const std::string& relname,
            metadata::Collection& mds,
            fd::Segment* make_repack_segment(const std::string&, const std::string&),
            bool skip_validation=false);
};

}
}
}
}

#endif
