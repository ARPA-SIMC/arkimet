#ifndef ARKI_DATASET_HTTP_TESTUTILS_H
#define ARKI_DATASET_HTTP_TESTUTILS_H

#include <arki/dataset/tests.h>
#include <string>
#include <vector>
#include <map>

namespace arki {
namespace utils {
namespace net {
namespace http {
struct Request;
}
}
}
}

namespace arki {
namespace tests {

struct FakeRequest
{
    char* fname;
    int fd;
    size_t response_offset;
    std::string response_method;
    std::map<std::string, std::string> response_headers;

    FakeRequest(const std::string& tmpfname = "fakereq.XXXXXX");
    ~FakeRequest();

    void write(const std::string& str);
    void write_get(const std::string& query);

    void reset();
    void setup_request(arki::utils::net::http::Request& req);
    void read_response();

    void dump_headers(std::ostream& out);

protected:
    virtual void append_body(const uint8_t* buf, size_t len) = 0;
};

struct StringFakeRequest : public FakeRequest
{
    std::string response_body;

protected:
    void append_body(const uint8_t* buf, size_t len) override;
};

struct BufferFakeRequest : public FakeRequest
{
    std::vector<uint8_t> response_body;

protected:
    void append_body(const uint8_t* buf, size_t len) override;
};

}
}

#endif
