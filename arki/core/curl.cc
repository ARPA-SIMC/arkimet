#include "curl.h"
#include "arki/utils/string.h"

using namespace arki::utils;

namespace arki {
namespace core {
namespace curl {

#define checked(context, stmt) do {\
        CURLcode errcode = stmt; \
        if (errcode != CURLE_OK) \
            throw core::curl::Exception(errcode, context); \
    } while (0)


Exception::Exception(CURLcode code, const std::string& context)
    : std::runtime_error("while " + context + ": " + curl_easy_strerror(code)) {}
Exception::Exception(CURLcode code, const std::string& extrainfo, const std::string& context)
    : std::runtime_error("while " + context + ": " + curl_easy_strerror(code) + "(" + extrainfo + ")") {}

CurlEasy::CurlEasy()
{
    m_errbuf = new char[CURL_ERROR_SIZE];

    // FIXME:
    // curl_easy_init automatically calls curl_global_init
    // curl_global_init is not thread safe, so the documentation suggests it is
    // called not automatically here, but somewhere at program startup
    m_curl = curl_easy_init();
    if (!m_curl)
        throw std::runtime_error("cannot initialize CURL: curl_easy_init returned NULL");
}

CurlEasy::~CurlEasy()
{
    if (m_curl)
        curl_easy_cleanup(m_curl);
    delete[] m_errbuf;
}

void CurlEasy::reset()
{
    // See file:///usr/share/doc/libcurl4-gnutls-dev/html/libcurl/curl_easy_setopt.html
    curl_easy_reset(m_curl);
    checked("setting error buffer", curl_easy_setopt(m_curl, CURLOPT_ERRORBUFFER, m_errbuf));
    //checked("setting verbose", curl_easy_setopt(m_curl, CURLOPT_VERBOSE, 1));
    checked("setting user agent", curl_easy_setopt(m_curl, CURLOPT_USERAGENT, "arkimet"));
    // CURLOPT_PROGRESSFUNCTION / CURLOPT_PROGRESSDATA ?
}


CurlForm::~CurlForm()
{
    if (post) curl_formfree(post);
}

void CurlForm::clear()
{
    if (post) curl_formfree(post);
    post = last = nullptr;
}

void CurlForm::add_string(const std::string& key, const std::string& val)
{
    curl_formadd(&post, &last,
            CURLFORM_COPYNAME, key.c_str(),
            CURLFORM_COPYCONTENTS, val.c_str(),
            CURLFORM_END);
}

void CurlForm::add_file(const std::string& key, const std::string& pathname)
{
    curl_formadd(&post, &last,
            CURLFORM_COPYNAME, key.c_str(),
            CURLFORM_FILE, pathname.c_str(),
            CURLFORM_END);
}


Request::Request(CurlEasy& curl)
    : curl(curl), method("GET")
{
}

Request::~Request() {}

void Request::set_url(const std::string& url)
{
    this->url = url;
}

void Request::set_method(const std::string& method)
{
    this->method = method;
}

void Request::perform()
{
    actual_url = url;
    while (true)
    {
        arkimet_exception_message.clear();
        response_code = -1;
        checked("setting url", curl_easy_setopt(curl, CURLOPT_URL, actual_url.c_str()));
        if (method == "POST")
        {
            checked("selecting POST method", curl_easy_setopt(curl, CURLOPT_POST, 1));
            checked("setting POST data", curl_easy_setopt(curl, CURLOPT_HTTPPOST, post_data.get()));
        }
        else if (method == "GET")
            checked("selecting GET method", curl_easy_setopt(curl, CURLOPT_HTTPGET, 1));
        else
            throw std::runtime_error("requested unsupported HTTP method '" + method + "'");
        checked("setting HTTP authentication method", curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_ANY));
        checked("setting netrc usage", curl_easy_setopt(curl, CURLOPT_NETRC, CURL_NETRC_OPTIONAL));
        checked("setting header function", curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, &Request::headerfunc));
        checked("setting header function data", curl_easy_setopt(curl, CURLOPT_WRITEHEADER, this));
        checked("setting write function", curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &Request::writefunc));
        checked("setting write function data", curl_easy_setopt(curl, CURLOPT_WRITEDATA, this));
        // CURLOPT_PROGRESSFUNCTION / CURLOPT_PROGRESSDATA ?

        CURLcode code = curl_easy_perform(curl);
        if (code != CURLE_OK)
        {
            if (callback_exception)
                std::rethrow_exception(callback_exception);
            throw core::curl::Exception(code, curl.m_errbuf, "Cannot query " + actual_url);
        }

        if (response_code == -1)
            checked("reading response code", curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code));

        if (response_code >= 400)
        {
            std::stringstream msg;
            msg << method << " " << actual_url << " got response code " << response_code << ": ";
            if (!arkimet_exception_message.empty())
                msg << arkimet_exception_message;
            else
                msg << response_error.str();
            throw std::runtime_error(msg.str());
        } else if (response_code >= 300) {
            char *url = nullptr;
            checked("reading redirect url", curl_easy_getinfo(curl, CURLINFO_REDIRECT_URL, &url));
            if (url)
                actual_url = url;
            else
                throw std::runtime_error("redirect code " + std::to_string(response_code) + " received without a redirect url available");
        } else if (response_code >= 200) {
            return;
        } else if (response_code >= 100)
            throw std::runtime_error("received unsupported HTTP code " + std::to_string(response_code));
    }
}

void Request::process_header_line(const std::string& line)
{
    // Look for special headers to get raw exception messages
    if (str::startswith(line, "Arkimet-Exception: "))
        arkimet_exception_message = line.substr(19);
}

size_t Request::headerfunc(void *ptr, size_t size, size_t nmemb, void *stream)
{
    // From curl_easy_setopt(3):
    /*
     * The header callback  will  be  called  once for each header and only
     * complete header lines are passed on to the callback.  Parsing headers
     * should be easy enough using this. The size of the data pointed to by ptr
     * is size multiplied with nmemb. Do not assume that the header line is
     * zero terminated!
     *
     * It's important to note that the callback will be invoked for the headers
     * of all responses received after initiating a request and not just the
     * final response. This includes all responses which occur during
     * authentication negotiation. If you need to operate on only the headers
     * from the final response, you will need to collect headers in the
     * callback yourself and use HTTP status lines, for example, to delimit
     * response boundaries.
     *
     * Since 7.14.1: When a server sends a chunked encoded transfer, it may
     * contain a trailer. That trailer is identical to a HTTP header and if
     * such a trailer is received it is passed to the application using this
     * callback as well. There are several ways to detect it being a trailer
     * and not an ordinary header: 1) it comes after the response-body. 2) it
     * comes after the final header line (CR LF) 3) a Trailer: header among the
     * response-headers mention what header to expect in the trailer.
     */
    Request& req = *(Request*)stream;
    std::string line((const char*)ptr, size*nmemb);
    req.process_header_line(line);
    return size * nmemb;
}

size_t Request::writefunc(void *ptr, size_t size, size_t nmemb, void *stream)
{
    Request& req = *(Request*)stream;

    // See if we have a response code
    if (req.response_code == -1)
        checked("reading response code", curl_easy_getinfo(req.curl, CURLINFO_RESPONSE_CODE, &req.response_code));

    if (req.response_code >= 300)
    {
        // If we have an error, save it in response_error
        req.response_error.write((const char*)ptr, size * nmemb);
        return size * nmemb;
    }

    try {
        return req.process_body_chunk(ptr, size, nmemb, stream);
    } catch (...) {
        req.callback_exception = std::current_exception();
        return 0;
    }
}

}
}
}
