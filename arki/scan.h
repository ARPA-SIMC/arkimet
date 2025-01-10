#ifndef ARKI_SCAN_H
#define ARKI_SCAN_H

#include <arki/core/fwd.h>
#include <arki/segment/fwd.h>
#include <arki/scan/fwd.h>
#include <arki/types/fwd.h>
#include <arki/metadata/fwd.h>
#include <filesystem>
#include <string>
#include <memory>
#include <vector>
#include <optional>
#include <cstdint>

namespace arki {
namespace scan {


class Scanner
{
public:
    virtual ~Scanner();

    /// Return a name identifying this type of scanner
    virtual DataFormat name() const = 0;

    /**
     * Open a file, scan it, send results to dest, and close it.
     *
     * Returns true if dest always returned true, else false.
     */
    virtual bool scan_segment(std::shared_ptr<segment::data::Reader> reader, metadata_dest_func dest) = 0;

    /**
     * Open a pathname to scan.
     *
     * Use this only in unit tests, as it makes assumptions that might not be
     * valid in normal code
     */
    bool test_scan_file(const std::filesystem::path& filename, metadata_dest_func dest);

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
    virtual std::shared_ptr<Metadata> scan_singleton(const std::filesystem::path& abspath) = 0;

    /**
     * Normalize metadata and data before dispatch, if required.
     *
     * For most formats, this function does nothing.
     *
     * When a format has a use for a normalization pass before dispatching to a
     * dataset, and such a normalization pass actually changes the data the
     * metadata is turned into an inline metadata, with the normalised data
     * attached.
     */
    virtual void normalize_before_dispatch(Metadata& md);

    /**
     * Create a scanner for the given format
     */
    static std::shared_ptr<Scanner> get_scanner(DataFormat format);

    static const Validator& get_validator(DataFormat format);

    /**
     * Guess a file format from its extension.
     */
    static DataFormat format_from_filename(const std::filesystem::path& fname);

    /**
     * Guess a file format from its extension.
     *
     * Return no value if the file format was not recognized.
     */
    static std::optional<DataFormat> detect_format(const std::filesystem::path& path);

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
    static std::vector<uint8_t> reconstruct(DataFormat format, const Metadata& md, const std::string& value);

    /**
     * Register the scanner factory function for the given format
     */
    static void register_factory(DataFormat format, std::function<std::shared_ptr<Scanner>()> factory);
};

/// Initialize scanner registry
void init();

}
}

#endif
