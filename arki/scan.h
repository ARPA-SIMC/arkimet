#ifndef ARKI_SCAN_H
#define ARKI_SCAN_H

#include <arki/core/fwd.h>
#include <arki/segment/fwd.h>
#include <arki/scan/fwd.h>
#include <arki/types/fwd.h>
#include <arki/metadata/fwd.h>
#include <string>
#include <memory>
#include <vector>

namespace arki {
namespace scan {


struct Scanner
{
    virtual ~Scanner();

    /// Return a name identifying this type of scanner
    virtual std::string name() const = 0;

    /**
     * Open a file, scan it, send results to dest, and close it.
     *
     * Returns true if dest always returned true, else false.
     */
    virtual bool scan_segment(std::shared_ptr<segment::Reader> reader, metadata_dest_func dest) = 0;

    /**
     * Open a pathname to scan.
     *
     * Use this only in unit tests, as it makes assumptions that might not be
     * valid in normal code
     */
    bool test_scan_file(const std::string& filename, metadata_dest_func dest);

    /**
     * Scan data from a non-seekable pipe
     */
    virtual bool scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest) = 0;

    /**
     * Scan a memory buffer.
     *
     * Returns a Metadata with inline source.
     */
    virtual std::shared_ptr<Metadata> scan_data(const std::vector<uint8_t>& data) = 0;

    /**
     * Open a file, scan it, send results to dest, and close it.
     *
     * Scanned metadata will have no source set.
     */
    virtual std::shared_ptr<Metadata> scan_singleton(const std::string& abspath) = 0;

    /**
     * Create a scanner for the given format
     */
    static std::shared_ptr<Scanner> get_scanner(const std::string& format);

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

    /**
     * Register the scanner factory function for the given format
     */
    static void register_factory(const std::string& name, std::function<std::shared_ptr<Scanner>()> factory);
};

/// Initialize scanner registry
void init();

}
}

#endif
