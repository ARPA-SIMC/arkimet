#ifndef ARKI_SCAN_BASE_H
#define ARKI_SCAN_BASE_H

#include <arki/core/fwd.h>
#include <arki/segment/fwd.h>
#include <string>
#include <memory>

namespace arki {
namespace scan {

struct Scanner
{
    std::string filename;
    std::string basedir;
    std::string relpath;
    std::shared_ptr<segment::Reader> reader;

    virtual ~Scanner();

    virtual void open(const std::string& filename, const std::string& basedir, const std::string& relpath, std::shared_ptr<core::Lock> lock);

    virtual void close();

    /**
     * Open a pathname to scan.
     *
     * Use this only in unit tests, as it makes assumptions that might not be
     * valid in normal code
     */
    void test_open(const std::string& filename);
};

}
}

#endif
