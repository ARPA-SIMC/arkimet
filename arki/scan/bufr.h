#ifndef ARKI_SCAN_BUFR_H
#define ARKI_SCAN_BUFR_H

#include <arki/scan/base.h>
#include <string>
#include <map>

namespace dballe {
struct File;

namespace msg {
struct Importer;
}

}

namespace arki {
class Metadata;
class ValueBag;

namespace scan {
struct Validator;

namespace bufr {
const Validator& validator();
struct BufrLua;
}

/**
 * Scan files for BUFR messages
 */
class Bufr : public Scanner
{
    dballe::File* file;
    dballe::msg::Importer* importer;
    bufr::BufrLua* extras;

	void read_info_base(char* buf, ValueBag& area);
	void read_info_fixed(char* buf, Metadata& md);
	void read_info_mobile(char* buf, Metadata& md);

    bool do_scan(Metadata& md);

public:
	Bufr();
	~Bufr();

    /// Alternate version with explicit basedir/relpath separation
    void open(const std::string& filename, const std::string& basedir, const std::string& relpath, std::shared_ptr<core::Lock> lock) override;

    /**
     * Close the input file.
     *
     * This is optional: the file will be closed by the destructor if needed.
     */
    void close() override;

	/**
	 * Scan the next BUFR in the file.
	 *
	 * @returns
	 *   true if it found a BUFR message,
	 *   false if there are no more BUFR messages in the file
	 */
	bool next(Metadata& md);

    /// Return the update sequence number for a BUFR
    static int update_sequence_number(const std::string& buf);
};

}
}

// vim:set ts=4 sw=4:
#endif
