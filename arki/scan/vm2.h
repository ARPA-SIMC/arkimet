#ifndef ARKI_SCAN_VM2_H
#define ARKI_SCAN_VM2_H

/// Scan a VM2 file for metadata

#include <string>
#include <vector>
#include <unistd.h>
#include <fstream>

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

class Vm2
{
protected:
    std::istream* in;
    std::string filename;
    std::string basedir;
    std::string relname;
    unsigned lineno;

    meteo::vm2::Parser* parser;

public:
	Vm2();
	virtual ~Vm2();

	/**
	 * Access a file with VM2 data
	 */
	void open(const std::string& filename);

    /// Alternate version with explicit basedir/relname separation
    void open(const std::string& filename, const std::string& basedir, const std::string& relname);

	/**
	 * Close the input file.
	 *
	 * This is optional: the file will be closed by the destructor if needed.
	 */
	void close();

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
