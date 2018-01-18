#ifndef ARKI_DATASET_SEGMENT_CONCAT_H
#define ARKI_DATASET_SEGMENT_CONCAT_H

#include <arki/libconfig.h>
#include <arki/dataset/segment/fd.h>
#include <string>

namespace arki {
namespace dataset {
namespace segment {
namespace concat {

class Writer : public fd::Writer
{
public:
    Writer(const std::string& root, const std::string& relname, const std::string& absname, int mode=0);
};

class Checker : public fd::Checker
{
protected:
    void open() override;

public:
    using fd::Checker::Checker;

    State check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick=true) override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) override;
    std::unique_ptr<fd::Writer> make_tmp_segment(const std::string& relname, const std::string& absname) override;
};

class HoleWriter : public fd::Writer
{
public:
    HoleWriter(const std::string& root, const std::string& relname, const std::string& absname, int mode=0);
};

class HoleChecker : public Checker
{
protected:
    void open() override;

    std::unique_ptr<fd::Writer> make_tmp_segment(const std::string& relname, const std::string& absname) override;

public:
    using Checker::Checker;

    Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) override;
};

}
}
}
}
#endif

