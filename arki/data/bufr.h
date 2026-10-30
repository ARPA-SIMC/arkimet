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

class MockScanner : public Scanner
{
protected:
    void do_scan(dballe::BinaryMessage& rmsg, std::shared_ptr<Metadata> md);

public:
    DataFormat name() const override { return DataFormat::BUFR; }

    bool scan_file(const std::filesystem::path& abspath,
                   metadata_dest_func dest) override;

    bool scan_pipe(core::NamedFileDescriptor& in,
                   metadata_dest_func dest) override;

    std::shared_ptr<Metadata>
    scan_data(const std::vector<uint8_t>& data) override;

    bool update_sequence_number(std::shared_ptr<Metadata> md,
                                int& usn) const override;
};

} // namespace arki::data::bufr
#endif
