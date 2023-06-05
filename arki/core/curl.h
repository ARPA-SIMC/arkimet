#ifndef ARKI_CORE_CURL_H
#define ARKI_CORE_CURL_H

#include <curl/curl.h>
#include <stdexcept>
#include <sstream>
#include <cstdint>

namespace arki {
namespace core {
namespace curl {

class Exception : public std::runtime_error
{
public:
    Exception(CURLcode code, const std::string& context);
    Exception(CURLcode code, const std::string& extrainfo, const std::string& context);
};

struct Request;


/**
 * Wrapper for the CURL easy API
 */
struct CurlEasy
{
    CURL* m_curl = nullptr;
    char* m_errbuf;

    CurlEasy();
    CurlEasy(const CurlEasy&) = delete;
    CurlEasy(CurlEasy&&) = delete;
    ~CurlEasy();

    CurlEasy& operator=(const CurlEasy&) = delete;
    CurlEasy& operator=(CurlEasy&&) = delete;

    void reset();

    operator CURL*() { return m_curl; }
    operator const CURL*() const { return m_curl; }
};


/**
 * Builds curl_httppost POST data
 */
class CurlForm
{
protected:
    curl_mime* post = nullptr;

public:
    CurlForm(CurlEasy& curl);
    ~CurlForm();

    void clear();
    void add_string(const std::string& key, const std::string& val);
    void add_file(const std::string& key, const std::string& pathname);

    curl_mime* get() { return post; }
};


/**
 * CURL HTTP request
 */
struct Request
{
    CurlEasy& curl;
    std::string method;
    std::string url;
    CurlForm post_data;
    long response_code = -1;
    /**
     * If the final request is the result of a redirect, this is the last
     * redirect url in the chain, that is, the one that provided the actual
     * response
     */
    std::string actual_url;
    std::stringstream response_error;
    std::string arkimet_exception_message;
    std::exception_ptr callback_exception;

    Request(CurlEasy& curl);
    virtual ~Request();

    void set_url(const std::string& url);
    void set_method(const std::string& method);

    /// Set up \a curl to process this request
    virtual void perform();

    void throw_if_response_code_not_ok();

protected:
    /// Process one full line of headers
    virtual void process_header_line(const std::string& line);

    /// Process a chunk of response body
    virtual size_t process_body_chunk(void *ptr, size_t size, size_t nmemb, void *stream) = 0;

    // Curl callback to process header data
    static size_t headerfunc(void *ptr, size_t size, size_t nmemb, void *stream);

    // Curl callback to process response body data
    static size_t writefunc(void *ptr, size_t size, size_t nmemb, void *stream);
};


/**
 * Process CURL results, inserting them into a container
 */
template<typename Container>
struct BufState : public Request
{
    using Request::Request;

    Container buf;

    size_t process_body_chunk(void *ptr, size_t size, size_t nmemb, void *stream) override
    {
        buf.insert(buf.end(), (uint8_t*)ptr, (uint8_t*)ptr + size * nmemb);
        return size * nmemb;
    }
};


}
}
}

#endif
