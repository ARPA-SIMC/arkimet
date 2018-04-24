#ifndef ARKI_SCAN_BASE_H
#define ARKI_SCAN_BASE_H

#include <arki/core/fwd.h>
#include <arki/segment/fwd.h>
#include <string>
#include <memory>

namespace arki {
namespace scan {
struct Validator;

struct Scanner
{
    std::string filename;
    std::shared_ptr<segment::Reader> reader;

    virtual ~Scanner();

    virtual void open(const std::string& filename, std::shared_ptr<segment::Reader> reader);

    /**
     * Scan the next data in the file.
     *
     * @returns
     *   true if it found a new data item
     *   false if there are no more data items in the file
     */
    virtual bool next(Metadata& md) = 0;

    /**
     * Close the input file.
     *
     * This is optional: the file will be closed by the destructor if needed.
     */
    virtual void close();

    /**
     * Open a pathname to scan.
     *
     * Use this only in unit tests, as it makes assumptions that might not be
     * valid in normal code
     */
    void test_open(const std::string& filename);

    /**
     * Open a file, scan it, send results to dest, and close it.
     *
     * Returns true if dest always returned true, else false.
     */
    bool scan_file(const std::string& abspath, std::shared_ptr<segment::Reader> reader, metadata_dest_func dest);

    /**
     * Create a scanner for the given format
     */
    static std::unique_ptr<Scanner> get_scanner(const std::string& format);

    static const Validator& get_validator(const std::string& format);
};

}
}

#endif
