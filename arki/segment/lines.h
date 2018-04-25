#ifndef ARKI_SEGMENT_LINES_H
#define ARKI_SEGMENT_LINES_H

/// Read/write functions for data blobs with newline separators
#include <arki/libconfig.h>
#include <arki/segment/fd.h>
#include <string>

namespace arki {
namespace segment {
namespace lines {

struct Segment : public fd::Segment
{
    using fd::Segment::Segment;

    const char* type() const override;
    bool single_file() const override;
    std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::Checker> checker() const override;

    static std::shared_ptr<Checker> make_checker(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath);
    static std::shared_ptr<Checker> create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds);
    static bool can_store(const std::string& format);
};

class Reader : public fd::Reader
{
protected:
    Segment m_segment;

public:
    Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock);
    const Segment& segment() const override;
};

class Writer : public fd::Writer
{
protected:
    Segment m_segment;

public:
    Writer(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, int mode=0);
    const Segment& segment() const override;
    std::unique_ptr<fd::File> open_file(const std::string& pathname, int flags, mode_t mode) override;
};

class Checker : public fd::Checker
{
protected:
    Segment m_segment;
    std::unique_ptr<fd::File> open_file(const std::string& pathname, int flags, mode_t mode) override;
    std::unique_ptr<fd::File> open(const std::string& pathname) override;

public:
    Checker(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath);
    const Segment& segment() const override;
    State check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick=true) override;
};

}
}
}
#endif
