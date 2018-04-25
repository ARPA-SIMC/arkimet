#ifndef ARKI_SCAN_BUFR_H
#define ARKI_SCAN_BUFR_H

#include <arki/scan.h>
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

    void open(const std::string& filename, std::shared_ptr<segment::Reader> reader) override;

    void close() override;
    bool next(Metadata& md) override;

    /// Return the update sequence number for a BUFR
    static int update_sequence_number(const std::string& buf);
};

}
}

// vim:set ts=4 sw=4:
#endif
