#ifndef ARKI_SCAN_BUFR_H
#define ARKI_SCAN_BUFR_H

#include <arki/scan.h>
#include <arki/metadata/fwd.h>
#include <string>

namespace dballe {
struct BinaryMessage;
struct Message;
struct Importer;
}

namespace arki {
namespace scan {
class MockEngine;

namespace bufr {
const Validator& validator();
}

class BufrScanner : public Scanner
{
    dballe::Importer* importer = nullptr;

protected:
    virtual void scan_extra(dballe::BinaryMessage& rmsg, std::shared_ptr<dballe::Message> msg, std::shared_ptr<Metadata> md) = 0;

    void do_scan(dballe::BinaryMessage& rmsg, std::shared_ptr<Metadata> md);

public:
    BufrScanner();
    ~BufrScanner();

    DataFormat name() const override { return DataFormat::BUFR; }

    std::shared_ptr<Metadata> scan_data(const std::vector<uint8_t>& data) override;
    bool scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest) override;
    bool scan_segment(std::shared_ptr<segment::data::Reader> reader, metadata_dest_func dest) override;
    std::shared_ptr<Metadata> scan_singleton(const std::filesystem::path& abspath) override;

    /// Return the update sequence number for a BUFR
    static int update_sequence_number(const std::string& buf);
};

class MockBufrScanner : public BufrScanner
{
protected:
    MockEngine* engine;

    void scan_extra(dballe::BinaryMessage& rmsg, std::shared_ptr<dballe::Message> msg, std::shared_ptr<Metadata> md) override;

public:
    MockBufrScanner();
    virtual ~MockBufrScanner();
};

}
}
#endif
