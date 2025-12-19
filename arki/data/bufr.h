#ifndef ARKI_DATA_BUFR_H
#define ARKI_DATA_BUFR_H

#include <arki/data.h>
#include <arki/metadata/fwd.h>
#include <string>

namespace dballe {
struct BinaryMessage;
struct Message;
struct Importer;
} // namespace dballe

namespace arki::data::bufr {

const Validator& validator();

class Scanner : public data::Scanner
{
    dballe::Importer* importer = nullptr;

protected:
    virtual void scan_extra(dballe::BinaryMessage& rmsg,
                            std::shared_ptr<dballe::Message> msg,
                            std::shared_ptr<Metadata> md) = 0;

    void do_scan(dballe::BinaryMessage& rmsg, std::shared_ptr<Metadata> md);

public:
    Scanner();
    ~Scanner();

    DataFormat name() const override { return DataFormat::BUFR; }

    std::shared_ptr<Metadata>
    scan_data(const std::vector<uint8_t>& data) override;
    bool scan_pipe(core::NamedFileDescriptor& in,
                   metadata_dest_func dest) override;
    bool scan_segment(std::shared_ptr<segment::Reader> reader,
                      metadata_dest_func dest) override;
    std::shared_ptr<Metadata>
    scan_singleton(const std::filesystem::path& abspath) override;

    /// Return the update sequence number for a BUFR
    int update_sequence_number_raw(const std::string& buf) const;

    bool update_sequence_number(Metadata& md, int& usn) const override;
    bool update_sequence_number(const types::source::Blob& source,
                                int& usn) const override;
};

class MockScanner : public Scanner
{
protected:
    void scan_extra(dballe::BinaryMessage& rmsg,
                    std::shared_ptr<dballe::Message> msg,
                    std::shared_ptr<Metadata> md) override;
};

} // namespace arki::data::bufr
#endif
