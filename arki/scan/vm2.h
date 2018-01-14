#ifndef ARKI_SCAN_VM2_H
#define ARKI_SCAN_VM2_H

/// Scan a VM2 file for metadata

#include <arki/scan/base.h>
#include <string>
#include <vector>
#include <unistd.h>

namespace meteo {
namespace vm2 {
class Parser;
}
}

namespace arki {
class Metadata;

namespace scan {
struct Validator;

namespace vm2 {
const Validator& validator();
}

class Vm2 : public Scanner
{
protected:
    std::istream* in;
    unsigned lineno;

    meteo::vm2::Parser* parser;

public:
	Vm2();
	virtual ~Vm2();

    /// Alternate version with explicit basedir/relname separation
    void open(const std::string& filename, const std::string& basedir, const std::string& relname, const core::lock::Policy* lock_policy) override;

    /**
     * Close the input file.
     *
     * This is optional: the file will be closed by the destructor if needed.
     */
    void close() override;

	/**
	 * Scan the next VM2 in the file.
	 *
	 * @returns
	 *   true if it found a VM2 message,
	 *   false if there are no more VM2 messages in the file
	 */
	bool next(Metadata& md);

    /// Reconstruct a VM2 based on metadata and a string value
    static std::vector<uint8_t> reconstruct(const Metadata& md, const std::string& value);
};

}
}
#endif
