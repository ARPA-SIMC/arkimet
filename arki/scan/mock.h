#ifndef ARKI_SCAN_MOCK_H
#define ARKI_SCAN_MOCK_H

#include <arki/metadata/fwd.h>
#include <filesystem>
#include <string>
#include <vector>
#include <memory>
#include <cstdint>

namespace arki {
namespace utils {
namespace sqlite {
class SQLiteDB;
class Query;
}
}

namespace scan {

class MockEngine
{
protected:
    std::filesystem::path db_pathname;
    utils::sqlite::SQLiteDB* db = nullptr;
    utils::sqlite::Query* by_sha256sum = nullptr;

public:
    MockEngine();
    MockEngine(const MockEngine&) = delete;
    MockEngine(MockEngine&&) = delete;
    ~MockEngine();
    MockEngine& operator=(const MockEngine&) = delete;
    MockEngine& operator=(MockEngine&&) = delete;

    std::shared_ptr<Metadata> lookup(const uint8_t* data, size_t size);
    std::shared_ptr<Metadata> lookup(const std::vector<uint8_t>& data);
    std::shared_ptr<Metadata> lookup(const std::string& data);

    std::shared_ptr<Metadata> by_checksum(const std::string& checksum);
};

}
}
#endif

