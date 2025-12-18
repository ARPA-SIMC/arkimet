#ifndef ARKI_DATA_H
#define ARKI_DATA_H

#include <arki/core/fwd.h>
#include <arki/data/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/segment/fwd.h>
#include <arki/types/fwd.h>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace arki::data {

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
    virtual bool scan_segment(std::shared_ptr<segment::Reader> reader,
                              metadata_dest_func dest) = 0;

    /**
     * Scan data from a non-seekable pipe
     */
    virtual bool scan_pipe(core::NamedFileDescriptor& in,
                           metadata_dest_func dest) = 0;

    /**
     * Scan a memory buffer.
     *
     * Returns a Metadata with inline source.
     */
    virtual std::shared_ptr<Metadata>
    scan_data(const std::vector<uint8_t>& data) = 0;

    /**
     * Open a file, scan it, send results to dest, and close it.
     *
     * Scanned metadata will have no source set.
     */
    virtual std::shared_ptr<Metadata>
    scan_singleton(const std::filesystem::path& abspath) = 0;

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
     * Return the update sequence number for this data
     *
     * The data associated to the metadata is read and scanned if needed.
     *
     * @retval
     *   The update sequence number found. This is left untouched if the
     * function returns false.
     * @returns
     *   true if the update sequence number could be found, else false
     *
     */
    virtual bool update_sequence_number(Metadata& md, int& usn) const;

    /**
     * Return the update sequence number for this data
     *
     * @retval
     *   The update sequence number found. This is left untouched if the
     * function returns false.
     * @returns
     *   true if the update sequence number could be found, else false
     *
     */
    virtual bool update_sequence_number(const types::source::Blob& source,
                                        int& usn) const;

    /**
     * Create a scanner for the given format
     */
    static std::shared_ptr<Scanner> get(DataFormat format);

    /**
     * Guess a file format from its extension.
     */
    static DataFormat format_from_filename(const std::filesystem::path& fname);

    /**
     * Guess a file format from its extension.
     *
     * Return no value if the file format was not recognized.
     */
    static std::optional<DataFormat>
    detect_format(const std::filesystem::path& path);

    /**
     * Reconstruct raw data based on a metadata and a value
     */
    static std::vector<uint8_t> reconstruct(DataFormat format,
                                            const Metadata& md,
                                            const std::string& value);

    /**
     * Register the scanner factory function for the given format
     */
    static void
    register_factory(DataFormat format,
                     std::function<std::shared_ptr<Scanner>()> factory);
};

/**
 * Validate data
 */
class Validator
{
public:
    virtual ~Validator() {}

    /// Return the format checked by this validator
    virtual DataFormat format() const = 0;

    // Validate data found in a file
    virtual void validate_file(core::NamedFileDescriptor& fd, off_t offset,
                               size_t size) const = 0;

    // Validate a memory buffer
    virtual void validate_buf(const void* buf, size_t size) const = 0;

    // Validate a metadata::Data
    virtual void validate_data(const metadata::Data& data) const;

    /**
     * Get the validator for a given file name
     *
     * @returns
     *   a pointer to a static object, which should not be deallocated.
     */
    static const Validator& by_filename(const std::filesystem::path& filename);

    /**
     * Get the validator for a given foramt
     *
     * @returns
     *   a pointer to a static object, which should not be deallocated.
     */
    static const Validator& get(DataFormat format);

protected:
    [[noreturn]] void throw_check_error(core::NamedFileDescriptor& fd,
                                        off_t offset,
                                        const std::string& msg) const;
    [[noreturn]] void throw_check_error(const std::string& msg) const;
};

/// Initialize scanner registry
void init();

} // namespace arki::data

#endif
