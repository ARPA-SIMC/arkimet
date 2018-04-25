#ifndef ARKI_SCAN_H
#define ARKI_SCAN_H

#include <arki/core/fwd.h>
#include <arki/segment/fwd.h>
#include <arki/scan/fwd.h>
#include <arki/types/fwd.h>
#include <string>
#include <memory>
#include <vector>

namespace arki {
namespace scan {

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
     * Open a file, scan it, send results to dest, and close it.
     *
     * Scanned metadata will have inline sources
     *
     * Returns true if dest always returned true, else false.
     */
    bool scan_metadata(const std::string& abspath, metadata_dest_func dest);

    /**
     * Create a scanner for the given format
     */
    static std::unique_ptr<Scanner> get_scanner(const std::string& format);

    static const Validator& get_validator(const std::string& format);

    /**
     * Normalise a file format string using the most widely used version
     *
     * This currently normalises:
     *  - grib1 and grib2 to grib
     *  - all of h5, hdf5, odim and odimh5 to odimh5
     *
     * If the format is unsupported, it throws an exception if defaut_format is
     * nullptr. Else it returns default_format.
     */
    static std::string normalise_format(const std::string& format, const char* default_format=nullptr);

    /**
     * Guess a file format from its extension.
     *
     * If defaut_format is nullptr, it throws an exception if the file has no
     * extension, or an unknown extension
     */
    static std::string format_from_filename(const std::string& fname, const char* default_format=nullptr);

    /**
     * Return the update sequence number for this data
     *
     * The data associated to the metadata is read and scanned if needed.
     *
     * @retval
     *   The update sequence number found. This is left untouched if the function
     *   returns false.
     * @returns
     *   true if the update sequence number could be found, else false
     *
     */
    static bool update_sequence_number(Metadata& md, int& usn);

    /**
     * Return the update sequence number for this data
     *
     * @retval
     *   The update sequence number found. This is left untouched if the function
     *   returns false.
     * @returns
     *   true if the update sequence number could be found, else false
     *
     */
    static bool update_sequence_number(const types::source::Blob& source, int& usn);

    /**
     * Reconstruct raw data based on a metadata and a value
     */
    static std::vector<uint8_t> reconstruct(const std::string& format, const Metadata& md, const std::string& value);

};

}
}

#endif
