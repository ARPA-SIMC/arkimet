#ifndef ARKI_SCAN_VALIDATOR_H
#define ARKI_SCAN_VALIDATOR_H

#include <arki/core/fwd.h>
#include <arki/metadata/fwd.h>
#include <string>

namespace arki {
namespace scan {

/**
 * Validate data
 */
class Validator
{
public:
    virtual ~Validator() {}

    /// Return the format checked by this validator
    virtual std::string format() const = 0;

    // Validate data found in a file
    virtual void validate_file(core::NamedFileDescriptor& fd, off_t offset, size_t size) const = 0;

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
	static const Validator& by_filename(const std::string& filename);

    /**
     * Get the validator for a given foramt
     *
     * @returns
     *   a pointer to a static object, which should not be deallocated.
     */
    static const Validator& by_format(const std::string& format);

protected:
    [[noreturn]] void throw_check_error(core::NamedFileDescriptor& fd, off_t offset, const std::string& msg) const;
    [[noreturn]] void throw_check_error(const std::string& msg) const;
};


}
}

#endif
