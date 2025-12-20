#ifndef ARKI_SCAN_VM2_H
#define ARKI_SCAN_VM2_H

/// Scan a VM2 file for metadata

#include <arki/data.h>
#include <cstdint>
#include <string>
#include <vector>

namespace arki::data::vm2 {
const Validator& validator();

class Scanner : public data::Scanner
{
public:
    DataFormat name() const override { return DataFormat::VM2; }
    std::shared_ptr<Metadata>
    scan_data(const std::vector<uint8_t>& data) override;
    bool scan_segment(std::shared_ptr<segment::Reader> reader,
                      metadata_dest_func dest) override;
    std::shared_ptr<Metadata>
    scan_file_single(const std::filesystem::path& abspath) override;
    bool scan_pipe(core::NamedFileDescriptor& in,
                   metadata_dest_func dest) override;
    void normalize_before_dispatch(Metadata& md) override;
    std::vector<uint8_t> reconstruct(const Metadata& md,
                                     const std::string& value) const override;
};

} // namespace arki::data::vm2
#endif
