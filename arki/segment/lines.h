#ifndef ARKI_SEGMENT_LINES_H
#define ARKI_SEGMENT_LINES_H

/// Read/write functions for data blobs with newline separators
#include <arki/libconfig.h>
#include <arki/segment/fd.h>
#include <string>

namespace arki {
namespace segment {
namespace lines {

class Writer : public fd::Writer
{
public:
    Writer(const std::string& root, const std::string& relpath, const std::string& abspath, int mode=0);
    const char* type() const override;
    std::unique_ptr<fd::File> open_file(const std::string& pathname, int flags, mode_t mode) override;
};

class Checker : public fd::Checker
{
protected:
    std::unique_ptr<fd::File> open_file(const std::string& pathname, int flags, mode_t mode) override;
    std::unique_ptr<fd::File> open(const std::string& pathname) override;

public:
    using fd::Checker::Checker;
    const char* type() const override;
    State check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick=true) override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) override;
    std::shared_ptr<segment::Checker> compress(metadata::Collection& mds) override;
    static bool can_store(const std::string& format);
    static std::shared_ptr<Checker> create(const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds);
};

}
}
}
#endif
