#ifndef ARKI_DATA_MOCK_H
#define ARKI_DATA_MOCK_H

#include <arki/data.h>
#include <arki/metadata/fwd.h>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace arki::utils::sqlite {
class SQLiteDB;
class Query;
} // namespace arki::utils::sqlite

namespace arki::data {

class MockEngine
{
protected:
    std::filesystem::path db_pathname;
    utils::sqlite::SQLiteDB* db        = nullptr;
    utils::sqlite::Query* by_sha256sum = nullptr;

    MockEngine();

public:
    MockEngine(const MockEngine&) = delete;
    MockEngine(MockEngine&&)      = delete;
    ~MockEngine();
    MockEngine& operator=(const MockEngine&) = delete;
    MockEngine& operator=(MockEngine&&)      = delete;

    std::shared_ptr<Metadata> lookup(const uint8_t* data, size_t size);
    std::shared_ptr<Metadata> lookup(const std::vector<uint8_t>& data);
    std::shared_ptr<Metadata> lookup(const std::string& data);

    std::shared_ptr<Metadata> by_checksum(const std::string& checksum);

    static MockEngine& get();
};

class MockScanner : public SingleFileScanner
{
protected:
    DataFormat m_format;

    std::shared_ptr<Metadata> scan_file(const std::filesystem::path& pathname);

public:
    explicit MockScanner(DataFormat format);
    ~MockScanner() override;

    DataFormat name() const override { return m_format; }

    std::shared_ptr<Metadata>
    scan_data(const std::vector<uint8_t>& data) override;
    bool scan_pipe(core::NamedFileDescriptor& in,
                   metadata_dest_func dest) override;
    std::shared_ptr<Metadata>
    scan_file_single(const std::filesystem::path& abspath) override;
};

} // namespace arki::data
#endif
