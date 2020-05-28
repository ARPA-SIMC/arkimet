#include "config.h"
#include "arki/dataset/http.h"
#include "arki/dataset/session.h"
#include "arki/dataset/progress.h"
#include "arki/dataset/query.h"
#include "arki/dataset/querymacro.h"
#include "arki/core/file.h"
#include "arki/metadata.h"
#include "arki/metadata/stream.h"
#include "arki/metadata/sort.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/core/binary.h"
#include "arki/nag.h"
#include <cstdlib>
#include <sstream>

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace dataset {

#define checked(context, stmt) do {\
        CURLcode errcode = stmt; \
        if (errcode != CURLE_OK) \
            throw http::Exception(errcode, context); \
    } while (0)


namespace http {

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
            throw http::Exception(code, curl.m_errbuf, "Cannot query " + actual_url);
        }

        if (response_code == -1)
            checked("reading response code", curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code));

        if (response_code >= 400)
        {
            stringstream msg;
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
    string line((const char*)ptr, size*nmemb);
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



Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : dataset::Dataset(session, cfg),
      baseurl(cfg.value("path")),
      qmacro(cfg.value("qmacro"))
{
}

std::shared_ptr<dataset::Reader> Dataset::create_reader()
{
    return std::make_shared<Reader>(static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<dataset::Reader> Dataset::create_querymacro_reader(
        std::shared_ptr<Session> session,
        const core::cfg::Sections& datasets,
        const std::string& macro_name,
        const std::string& macro_query)
{
    // If all the datasets are on the same server, we can run the macro remotely
    std::string baseurl = all_same_remote_server(datasets);

    // TODO: macro_arg seems to be ignored (and lost) here

    if (baseurl.empty())
    {
        // Either all datasets are local, or they are on different servers: run the macro locally
        arki::nag::verbose("Running query macro %s locally", macro_name.c_str());

        // TODO: download and merge alias databases from all the servers

        auto dataset = std::make_shared<arki::dataset::QueryMacro>(session, datasets, macro_name, macro_query);
        return dataset->create_reader();
    } else {
        // Create the remote query macro
        arki::nag::verbose("Running query macro %s remotely on %s", macro_name.c_str(), baseurl.c_str());
        arki::core::cfg::Section cfg;
        cfg.set("name", macro_name);
        cfg.set("type", "remote");
        cfg.set("path", baseurl);
        cfg.set("qmacro", macro_query);
        return session->dataset(cfg)->create_reader();
    }
}

static string geturlprefix(const std::string& s)
{
    // Take until /dataset/
    size_t pos = s.rfind("/dataset/");
    if (pos == string::npos) return string();
    return s.substr(0, pos);
}

std::string Dataset::all_same_remote_server(const core::cfg::Sections& cfg)
{
    string base;
    for (const auto& si: cfg)
    {
        string type = str::lower(si.second.value("type"));
        if (type != "remote") return string();
        string urlprefix = geturlprefix(si.second.value("path"));
        if (urlprefix.empty()) return string();
        if (base.empty())
            base = urlprefix;
        else if (base != urlprefix)
            return string();
    }
    return base;
}

std::string Reader::type() const { return "http"; }

struct OstreamState : public Request
{
    NamedFileDescriptor& out;
    std::function<void(NamedFileDescriptor&)> data_start_hook;
    std::shared_ptr<dataset::QueryProgress> progress;

    OstreamState(CurlEasy& curl, NamedFileDescriptor& out, std::function<void(NamedFileDescriptor&)> data_start_hook=0)
        : Request(curl), out(out), data_start_hook(data_start_hook)
    {
    }

    void perform()
    {
        if (progress) progress->start();
        Request::perform();
    }

    size_t process_body_chunk(void *ptr, size_t size, size_t nmemb, void *stream) override
    {
        if (data_start_hook && size > 0)
        {
            data_start_hook(out);
            data_start_hook = nullptr;
        }
        size_t res = out.write((const char*)ptr, size * nmemb);
        if (progress) progress->update(0, size * nmemb);
        return res;
    }
};

struct AbstractOutputState : public Request
{
    AbstractOutputFile& out;
    std::shared_ptr<dataset::QueryProgress> progress;

    AbstractOutputState(CurlEasy& curl, AbstractOutputFile& out)
        : Request(curl), out(out)
    {
    }

    void perform()
    {
        if (progress) progress->start();
        Request::perform();
    }

    size_t process_body_chunk(void *ptr, size_t size, size_t nmemb, void *stream) override
    {
        out.write((const char*)ptr, size * nmemb);
        if (progress) progress->update(0, size * nmemb);
        return size * nmemb;
    }
};

struct MDStreamState : public Request
{
    metadata::Stream mdc;

    MDStreamState(CurlEasy& curl, metadata_dest_func dest, const std::string& baseurl)
        : Request(curl), mdc(dest, baseurl)
    {
    }

    size_t process_body_chunk(void *ptr, size_t size, size_t nmemb, void *stream) override
    {
        mdc.readData(ptr, size * nmemb);
        return size * nmemb;
    }
};

void Reader::set_post_query(Request& request, const std::string& query)
{
    if (dataset().qmacro.empty())
        request.post_data.add_string("query", query);
    else
    {
        request.post_data.add_string("query", dataset().qmacro);
        request.post_data.add_string("qmacro", name());
    }
}

void Reader::set_post_query(Request& request, const dataset::DataQuery& q)
{
    set_post_query(request, q.matcher.toStringExpanded());
    if (q.sorter)
        request.post_data.add_string("sort", q.sorter->toString());
}

bool Reader::impl_query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    dataset::TrackProgress track(q.progress);
    dest = track.wrap(dest);

    m_curl.reset();

    MDStreamState request(m_curl, dest, dataset().baseurl);
    request.set_url(str::joinpath(dataset().baseurl, "query"));
    request.set_method("POST");
    set_post_query(request, q);
    if (q.with_data)
        request.post_data.add_string("style", "inline");
    request.perform();

    return track.done(!request.mdc.consumer_canceled());
}

void Reader::impl_query_summary(const Matcher& matcher, Summary& summary)
{
    m_curl.reset();

    BufState<std::vector<uint8_t>> request(m_curl);
    request.set_url(str::joinpath(dataset().baseurl, "summary"));
    request.set_method("POST");
    set_post_query(request, matcher.toStringExpanded());
    request.perform();

    BinaryDecoder dec(request.buf);
    summary.read(dec, request.url);
}

void Reader::impl_fd_query_bytes(const dataset::ByteQuery& q, NamedFileDescriptor& out)
{
    m_curl.reset();

    OstreamState request(m_curl, out, q.data_start_hook);
    request.set_url(str::joinpath(dataset().baseurl, "query"));
    request.set_method("POST");
    request.progress = q.progress;
    set_post_query(request, q);

    const char* toupload = getenv("ARKI_POSTPROC_FILES");
    if (toupload != NULL)
    {
        unsigned count = 0;
        // Split by ':'
        str::Split splitter(toupload, ":");
        for (str::Split::const_iterator i = splitter.begin(); i != splitter.end(); ++i)
            request.post_data.add_file("postprocfile" + to_string(++count), *i);
    }
    switch (q.type)
    {
        case dataset::ByteQuery::BQ_DATA:
            request.post_data.add_string("style", "data");
            break;
        case dataset::ByteQuery::BQ_POSTPROCESS:
            request.post_data.add_string("style", "postprocess");
            request.post_data.add_string("command", q.param);
            break;
        default: {
            stringstream ss;
            ss << "cannot query dataset: unsupported query type: " << (int)q.type;
            throw std::runtime_error(ss.str());
        }
    }
    request.perform();
    if (q.progress) q.progress->done();
}

void Reader::impl_abstract_query_bytes(const dataset::ByteQuery& q, AbstractOutputFile& out)
{
    if (q.data_start_hook)
        throw std::runtime_error("Cannot use data_start_hook on abstract output files");

    m_curl.reset();

    AbstractOutputState request(m_curl, out);
    request.set_url(str::joinpath(dataset().baseurl, "query"));
    request.set_method("POST");
    request.progress = q.progress;
    set_post_query(request, q);

    const char* toupload = getenv("ARKI_POSTPROC_FILES");
    if (toupload != NULL)
    {
        unsigned count = 0;
        // Split by ':'
        str::Split splitter(toupload, ":");
        for (str::Split::const_iterator i = splitter.begin(); i != splitter.end(); ++i)
            request.post_data.add_file("postprocfile" + to_string(++count), *i);
    }
    switch (q.type)
    {
        case dataset::ByteQuery::BQ_DATA:
            request.post_data.add_string("style", "data");
            break;
        case dataset::ByteQuery::BQ_POSTPROCESS:
            request.post_data.add_string("style", "postprocess");
            request.post_data.add_string("command", q.param);
            break;
        default: {
            stringstream ss;
            ss << "cannot query dataset: unsupported query type: " << (int)q.type;
            throw std::runtime_error(ss.str());
        }
    }
    request.perform();
    if (q.progress) q.progress->done();
}

core::cfg::Sections Reader::load_cfg_sections(const std::string& path)
{
    using namespace http;

    CurlEasy m_curl;
    m_curl.reset();

    BufState<std::string> request(m_curl);
    request.set_url(str::joinpath(path, "config"));
    request.perform();

    auto res = core::cfg::Sections::parse(request.buf, request.url);
    // Make sure name=* is present in each section
    for (auto& si: res)
        si.second.set("name", si.first);
    return res;
}

core::cfg::Section Reader::load_cfg_section(const std::string& path)
{
    using namespace http;

    CurlEasy m_curl;
    m_curl.reset();

    BufState<std::string> request(m_curl);
    request.set_url(str::joinpath(path, "config"));
    request.perform();

    auto sections = core::cfg::Sections::parse(request.buf, request.url);
    if (sections.size() != 1)
        throw std::runtime_error(request.url + ": only 1 section expected in resulting configuration, found " + std::to_string(sections.size()));

    auto res = sections.begin()->second;
    res.set("name", sections.begin()->first);
    return res;
}

std::string Reader::expandMatcher(const std::string& matcher, const std::string& server)
{
    using namespace http;

    CurlEasy m_curl;
    m_curl.reset();

    BufState<std::string> request(m_curl);
    request.set_url(str::joinpath(server, "qexpand"));
    request.set_method("POST");
    request.post_data.add_string("query", matcher);
    request.perform();

    return str::strip(request.buf);
}

core::cfg::Sections Reader::getAliasDatabase(const std::string& server)
{
    using namespace http;

    CurlEasy m_curl;
    m_curl.reset();

    BufState<string> request(m_curl);
    request.set_url(str::joinpath(server, "aliases"));
    request.perform();
    return core::cfg::Sections::parse(request.buf, server);
}

std::string Reader::expand_remote_query(const core::cfg::Sections& remotes, const std::string& query)
{
    // Resolve the query on each server (including the local system, if
    // queried). If at least one server can expand it, send that
    // expanded query to all servers. If two servers expand the same
    // query in different ways, raise an error.
    set<string> servers_seen;
    string expanded;
    string resolved_by;
    bool first = true;
    for (auto si: remotes)
    {
        string server = si.second.value("server");
        if (servers_seen.find(server) != servers_seen.end()) continue;
        string got;
        try {
            if (server.empty())
            {
                got = Matcher::parse(query).toStringExpanded();
                resolved_by = "local system";
            } else {
                got = dataset::http::Reader::expandMatcher(query, server);
                resolved_by = server;
            }
        } catch (std::exception& e) {
            // If the server cannot expand the query, we're
            // ok as we send it expanded. What we are
            // checking here is that the server does not
            // have a different idea of the same aliases
            // that we use
            continue;
        }
        if (!first && got != expanded)
        {
            nag::warning("%s expands the query as %s", server.c_str(), got.c_str());
            nag::warning("%s expands the query as %s", resolved_by.c_str(), expanded.c_str());
            throw std::runtime_error("cannot check alias consistency: two systems queried disagree about the query alias expansion");
        } else if (first)
            expanded = got;
        first = false;
    }

    if (!first)
        return expanded;
    return query;
}


HTTPInbound::HTTPInbound(const std::string& baseurl)
    : m_baseurl(baseurl)
{
}

HTTPInbound::~HTTPInbound()
{
}

void HTTPInbound::list(std::vector<std::string>& files)
{
    m_curl.reset();

    BufState<string> request(m_curl);
    request.set_url(str::joinpath(m_baseurl, "inbound/list"));
    request.perform();

    // Parse the results
    str::Split splitter(request.buf, "\n");
    for (str::Split::const_iterator i = splitter.begin(); i != splitter.end(); ++i)
        files.push_back(*i);
}

void HTTPInbound::scan(const std::string& fname, const std::string& format, metadata_dest_func dest)
{
    m_curl.reset();

    MDStreamState request(m_curl, dest, m_baseurl);
    request.set_url(str::joinpath(m_baseurl, "inbound/scan"));
    request.set_method("POST");
    request.post_data.add_string("file", fname);
    if (!format.empty())
        request.post_data.add_string("format", format);
    request.perform();
}

void HTTPInbound::testdispatch(const std::string& fname, const std::string& format, NamedFileDescriptor& out)
{
    m_curl.reset();

    OstreamState request(m_curl, out);
    request.set_url(str::joinpath(m_baseurl, "inbound/testdispatch"));
    request.set_method("POST");
    request.post_data.add_string("file", fname);
    if (!format.empty())
        request.post_data.add_string("format", format);
    request.perform();
}

void HTTPInbound::dispatch(const std::string& fname, const std::string& format, metadata_dest_func consumer)
{
    m_curl.reset();

    MDStreamState request(m_curl, consumer, m_baseurl);
    request.set_url(str::joinpath(m_baseurl, "inbound/dispatch"));
    request.set_method("POST");
    request.post_data.add_string("file", fname);
    if (!format.empty())
        request.post_data.add_string("format", format);
    request.perform();
}

}
}
}
