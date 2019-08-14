#ifndef ARKI_SCAN_BUFR_H
#define ARKI_SCAN_BUFR_H

#include <arki/scan.h>
#include <string>
#include <map>

namespace dballe {
struct File;
struct BinaryMessage;
struct Message;
struct Importer;
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

class BufrScanner : public Scanner
{
    dballe::Importer* importer = nullptr;

protected:
    virtual void scan_extra(dballe::Message& msg, Metadata& md) = 0;

    void do_scan(dballe::BinaryMessage& rmsg, Metadata& md);

public:
    BufrScanner();
    ~BufrScanner();

    std::string name() const override { return "bufr"; }

    std::shared_ptr<Metadata> scan_data(const std::vector<uint8_t>& data) override;
    bool scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest) override;
    bool scan_segment(std::shared_ptr<segment::Reader> reader, metadata_dest_func dest) override;
    std::shared_ptr<Metadata> scan_singleton(const std::string& abspath) override;

    /// Return the update sequence number for a BUFR
    static int update_sequence_number(const std::string& buf);
};

/**
 * Scan files for BUFR messages
 */
class Bufr : public BufrScanner
{
    bufr::BufrLua* extras = nullptr;

    void scan_extra(dballe::Message& msg, Metadata& md) override;

public:
    Bufr();
    ~Bufr();
};

}
}
#endif
