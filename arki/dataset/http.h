#ifndef ARKI_DATASET_HTTP_H
#define ARKI_DATASET_HTTP_H

/// Remote HTTP dataset access

#include <arki/dataset/local.h>
#include <curl/curl.h>
#include <string>
#include <sstream>

namespace arki {
class Metadata;
class Matcher;

namespace dataset {

namespace http {
class Exception : public std::runtime_error
{
public:
    Exception(CURLcode code, const std::string& context);
    Exception(CURLcode code, const std::string& extrainfo, const std::string& context);
};

struct Request;


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


class CurlForm
{
protected:
    curl_httppost* post = nullptr;
    curl_httppost* last = nullptr;

public:
    ~CurlForm();

    void clear();
    void add_string(const std::string& key, const std::string& val);
    void add_file(const std::string& key, const std::string& pathname);

    curl_httppost* get() { return post; }
};


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


struct Config : public dataset::Config
{
    std::string baseurl;
    std::string qmacro;

    Config(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    static std::shared_ptr<const Config> create(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::unique_ptr<dataset::Reader> create_reader() const override;
};

/**
 * Remote HTTP access to a Dataset.
 *
 * This dataset implements all the dataset query functions through HTTP
 * requests to an Arkimet web server.
 *
 * The dataset is read only: remote import of new data is not supported.
 */
class Reader : public dataset::Reader
{
protected:
    std::shared_ptr<const Config> m_config;
    http::CurlEasy m_curl;
    void set_post_query(Request& request, const std::string& query);
    void set_post_query(Request& request, const dataset::DataQuery& q);

public:
    // Initialise the dataset with the information from the configurationa in 'cfg'
    Reader(std::shared_ptr<const Config> config);
    virtual ~Reader();

    const Config& config() const override { return *m_config; }
    std::string type() const override;

    bool query_data(const dataset::DataQuery& q, metadata_dest_func) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
    void query_bytes(const dataset::ByteQuery& q, core::NamedFileDescriptor& out) override;
    void query_bytes(const dataset::ByteQuery& q, core::AbstractOutputFile& out) override;

    static core::cfg::Sections load_cfg_sections(const std::string& path);
    static core::cfg::Section load_cfg_section(const std::string& path);

    /**
     * Expand the given matcher expression using the aliases on this server
     */
    static std::string expandMatcher(const std::string& matcher, const std::string& server);

    /**
     * Read the alias database from the given remote dataset
     */
    static core::cfg::Sections getAliasDatabase(const std::string& server);

    static std::string expand_remote_query(const core::cfg::Sections& remotes, const std::string& query);

    /**
     * Check if all the datasets in the given config are remote and from
     * the same server.
     *
     * @return the common server URL, or the empty string if some datasets
     * are local or from different servers
     */
    static std::string allSameRemoteServer(const core::cfg::Sections& cfg);
};

class HTTPInbound
{
    std::string m_baseurl;
    mutable http::CurlEasy m_curl;

public:
    HTTPInbound(const std::string& baseurl);
    ~HTTPInbound();

    /// Scan a previously uploaded file
    void list(std::vector<std::string>& files);

    /// Scan a previously uploaded file
    void scan(const std::string& fname, const std::string& format, metadata_dest_func dest);

    /// Run a testdispatch on a previously uploaded file
    void testdispatch(const std::string& fname, const std::string& format, core::NamedFileDescriptor& out);

    /// Run a dispatch on a previously uploaded file
    void dispatch(const std::string& fname, const std::string& format, metadata_dest_func consumer);
};

}
}
}
#endif
