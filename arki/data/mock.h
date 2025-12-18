#ifndef ARKI_DATA_MOCK_H
#define ARKI_DATA_MOCK_H

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

public:
    MockEngine();
    MockEngine(const MockEngine&) = delete;
    MockEngine(MockEngine&&)      = delete;
    ~MockEngine();
    MockEngine& operator=(const MockEngine&) = delete;
    MockEngine& operator=(MockEngine&&)      = delete;

    std::shared_ptr<Metadata> lookup(const uint8_t* data, size_t size);
    std::shared_ptr<Metadata> lookup(const std::vector<uint8_t>& data);
    std::shared_ptr<Metadata> lookup(const std::string& data);

    std::shared_ptr<Metadata> by_checksum(const std::string& checksum);
};

} // namespace arki::data
#endif
