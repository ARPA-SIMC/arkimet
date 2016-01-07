#include "config.h"
#include <arki/dataset/http.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/stream.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/sort.h>
#include <arki/utils/string.h>
#include <arki/utils/sys.h>
#include <arki/binary.h>
#include <cstdlib>
#include <sstream>

using namespace std;
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

CurlEasy::CurlEasy() : m_curl(0)
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

struct CurlForm
{
	curl_httppost* post;
	curl_httppost* last;

	CurlForm() : post(0), last(0) {}
	~CurlForm()
	{
		if (post) curl_formfree(post);
	}

	void addstring(const std::string& key, const std::string& val)
	{
		curl_formadd(&post, &last,
				CURLFORM_COPYNAME, key.c_str(),
				CURLFORM_COPYCONTENTS, val.c_str(),
				CURLFORM_END);
	}

	void addfile(const std::string& key, const std::string& pathname)
	{
		curl_formadd(&post, &last,
				CURLFORM_COPYNAME, key.c_str(),
				CURLFORM_FILE, pathname.c_str(),
				CURLFORM_END);
	}

	curl_httppost* ptr() { return post; }
};
	

}

HTTP::HTTP(const ConfigFile& cfg)
    : Reader(cfg), m_mischief(false)
{
    m_baseurl = cfg.value("path");
    m_qmacro = cfg.value("qmacro");
}

HTTP::~HTTP()
{
}

std::string HTTP::type() const { return "http"; }

struct ReqState
{
    http::CurlEasy& m_curl;
    long response_code;
    std::stringstream response_error;
    std::string exception_message;

    ReqState(http::CurlEasy& curl) : m_curl(curl), response_code(-1)
    {
        // TODO: register header function
        checked("setting header function", curl_easy_setopt(m_curl, CURLOPT_HEADERFUNCTION, &ReqState::headerfunc));
        checked("setting header function data", curl_easy_setopt(m_curl, CURLOPT_WRITEHEADER, this));
    }

    static size_t headerfunc(void *ptr, size_t size, size_t nmemb, void *stream)
    {
        // From curl_easy_setopt(3):
        /*
         * The header callback  will  be  called  once for each header and only
         * complete header lines are passed on to the callback.  Parsing
         * headers should be easy enough using this. The size of the data
         * pointed to by ptr  is  size  multiplied with nmemb. Do not assume
         * that the header line is zero terminated!
         *
         * It's  important to note that the callback will be invoked for the
         * headers of all responses received after initiating a request and
         * not just the final response. This includes all responses which occur
         * during  authentication negotiation. If you need to operate on only
         * the headers from the final response, you will need to collect
         * headers in the callback yourself and use HTTP status lines, for
         * example, to delimit response boundaries.
         *
         * Since 7.14.1: When a server sends a chunked encoded transfer, it may
         * contain a trailer. That trailer is  identical  to  a HTTP header and
         * if such a trailer is received it is passed to the application using
         * this callback as well. There are several ways to detect it being a
         * trailer and not an ordinary header: 1) it comes after the
         * response-body. 2) it comes after the final header line (CR LF) 3) a
         * Trailer: header among the response-headers mention what header to
         * expect in the trailer.
         */
        ReqState& s = *(ReqState*)stream;
        string header((const char*)ptr, size*nmemb);
        // Look for special headers to get raw exception messages
        if (str::startswith(header, "Arkimet-Exception: "))
            s.exception_message = header.substr(19);
        return size * nmemb;
    }

	size_t check_error(void* ptr, size_t size, size_t nmemb)
	{
		// See if we have a response code
		if (response_code == -1)
			checked("reading response code", curl_easy_getinfo(m_curl, CURLINFO_RESPONSE_CODE, &response_code));
		// If we do not have an error, we are fine
		if (response_code < 400)
			return 0;
		// If we have an error, save it in response_error
		response_error.write((const char*)ptr, size * nmemb);
		return size * nmemb;
	}

    void throwError(const std::string& context)
    {
        stringstream msg;
        msg << "Server gave status " << response_code << ": ";
        if (!exception_message.empty())
            msg << exception_message;
        else
            msg << response_error.str();
        msg << " while " << context;
        throw std::runtime_error(msg.str());
    }
};

template<typename Container>
struct BufState : public ReqState
{
    Container buf;

    BufState(http::CurlEasy& curl) : ReqState(curl)
    {
        checked("setting write function", curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, &BufState::writefunc));
        checked("setting write function data", curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, this));
    }

    static size_t writefunc(void *ptr, size_t size, size_t nmemb, void *stream)
    {
        BufState& s = *(BufState*)stream;
        if (size_t res = s.check_error(ptr, size, nmemb)) return res;
        s.buf.insert(s.buf.end(), (uint8_t*)ptr, (uint8_t*)ptr + size * nmemb);
        return size * nmemb;
    }
};

struct OstreamState : public ReqState
{
    NamedFileDescriptor& out;
    std::function<void()> data_start_hook;

    OstreamState(http::CurlEasy& curl, NamedFileDescriptor& out, std::function<void()> data_start_hook = 0)
        : ReqState(curl), out(out), data_start_hook(data_start_hook)
    {
        checked("setting write function", curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, OstreamState::writefunc));
        checked("setting write function data", curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, this));
    }

    static size_t writefunc(void *ptr, size_t size, size_t nmemb, void *stream)
    {
        OstreamState& s = *(OstreamState*)stream;
        if (s.data_start_hook && size > 0)
        {
            s.data_start_hook();
            s.data_start_hook = nullptr;
        }
        if (size_t res = s.check_error(ptr, size, nmemb)) return res;
        return s.out.write((const char*)ptr, size * nmemb);
    }
};

struct MDStreamState : public ReqState
{
	metadata::Stream mdc;

    MDStreamState(http::CurlEasy& curl, metadata_dest_func dest, const std::string& baseurl)
        : ReqState(curl), mdc(dest, baseurl)
    {
        checked("setting write function", curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, MDStreamState::writefunc));
        checked("setting write function data", curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, this));
    }

	static size_t writefunc(void *ptr, size_t size, size_t nmemb, void *stream)
	{
		MDStreamState& s = *(MDStreamState*)stream;
		if (size_t res = s.check_error(ptr, size, nmemb)) return res;
		s.mdc.readData(ptr, size * nmemb);
		return size * nmemb;
	}
};


void HTTP::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    m_curl.reset();

    string url = str::joinpath(m_baseurl, "query");
    checked("setting url", curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str()));
    checked("selecting POST method", curl_easy_setopt(m_curl, CURLOPT_POST, 1));
    string postdata;
    if (m_qmacro.empty())
        postdata = "query=" + str::encode_url(q.matcher.toStringExpanded());
    else
        postdata = "query=" + str::encode_url(m_qmacro) + "&qmacro=" + str::encode_url(m_name);
    if (q.sorter)
        postdata += "&sort=" + str::encode_url(q.sorter->toString());
    if (m_mischief)
    {
        postdata += str::encode_url(";MISCHIEF");
        m_mischief = false;
    }
    if (q.with_data)
      postdata += "&style=inline";
	//fprintf(stderr, "URL: %s  POSTDATA: %s\n", url.c_str(), postdata.c_str());
	//fprintf(stderr, "POSTDATA \"%s\"", postdata.c_str());
	checked("setting POST data", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDS, postdata.c_str()));
	// Size of postfields argument if it's non text
	checked("setting POST data size", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDSIZE, postdata.size()));

    MDStreamState s(m_curl, dest, m_baseurl);
    // CURLOPT_PROGRESSFUNCTION / CURLOPT_PROGRESSDATA ?

	CURLcode code = curl_easy_perform(m_curl);
	if (code != CURLE_OK)
		throw http::Exception(code, m_curl.m_errbuf, "Performing query at " + url);

	if (s.response_code >= 400)
		s.throwError("querying summary from " + url);
}

void HTTP::query_summary(const Matcher& matcher, Summary& summary)
{
    m_curl.reset();

    string url = str::joinpath(m_baseurl, "summary");
    checked("setting url", curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str()));
    checked("selecting POST method", curl_easy_setopt(m_curl, CURLOPT_POST, 1));
    string postdata;
    if (m_qmacro.empty())
        postdata = "query=" + str::encode_url(matcher.toStringExpanded());
    else
        postdata = "query=" + str::encode_url(m_qmacro) + "&qmacro=" + str::encode_url(m_name);
    if (m_mischief)
    {
        postdata += str::encode_url(";MISCHIEF");
        m_mischief = false;
    }
	//fprintf(stderr, "URL: %s  POSTDATA: %s\n", url.c_str(), postdata.c_str());
	//fprintf(stderr, "POSTDATA \"%s\"", postdata.c_str());
	checked("setting POST data", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDS, postdata.c_str()));
	// Size of postfields argument if it's non text
	checked("setting POST data size", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDSIZE, postdata.size()));

    BufState<std::vector<uint8_t>> s(m_curl);
    // CURLOPT_PROGRESSFUNCTION / CURLOPT_PROGRESSDATA ?

	CURLcode code = curl_easy_perform(m_curl);
	if (code != CURLE_OK)
		throw http::Exception(code, m_curl.m_errbuf, "Performing query at " + url);

	if (s.response_code >= 400)
		s.throwError("querying summary from " + url);

    BinaryDecoder dec(s.buf);
    summary.read(dec, url);
}

void HTTP::query_bytes(const dataset::ByteQuery& q, NamedFileDescriptor& out)
{
    m_curl.reset();
    http::CurlForm form;
    string url = str::joinpath(m_baseurl, "query");
    checked("setting url", curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str()));
    checked("selecting POST method", curl_easy_setopt(m_curl, CURLOPT_POST, 1));
	if (m_qmacro.empty())
	{
		if (m_mischief)
		{
			form.addstring("query", q.matcher.toStringExpanded() + ";MISCHIEF");
			m_mischief = false;
		} else
			form.addstring("query", q.matcher.toStringExpanded());
	} else {
		form.addstring("query", m_qmacro);
		form.addstring("qmacro", m_name);
	}
	if (q.sorter)
		form.addstring("sort", q.sorter->toString());

    const char* toupload = getenv("ARKI_POSTPROC_FILES");
    if (toupload != NULL)
    {
        // Split by ':'
        str::Split splitter(toupload, ":");
        for (str::Split::const_iterator i = splitter.begin(); i != splitter.end(); ++i)
            form.addfile("postprocfile", *i);
    }
	switch (q.type)
	{
		case dataset::ByteQuery::BQ_DATA:
			form.addstring("style", "data");
			break;
		case dataset::ByteQuery::BQ_POSTPROCESS:
			form.addstring("style", "postprocess");
			form.addstring("command", q.param);
			break;
		case dataset::ByteQuery::BQ_REP_METADATA:
			form.addstring("style", "rep_metadata");
			form.addstring("command", q.param);
			break;
		case dataset::ByteQuery::BQ_REP_SUMMARY:
			form.addstring("style", "rep_summary");
			form.addstring("command", q.param);
			break;
		default: {
            stringstream ss;
            ss << "cannot query dataset: unsupported query type: " << (int)q.type;
            throw std::runtime_error(ss.str());
        }
    }
	//fprintf(stderr, "URL: %s  POSTDATA: %s\n", url.c_str(), postdata.c_str());
	//fprintf(stderr, "POSTDATA \"%s\"", postdata.c_str());
	// Set the form info 
	curl_easy_setopt(m_curl, CURLOPT_HTTPPOST, form.ptr());
	OstreamState s(m_curl, out, q.data_start_hook);
	// CURLOPT_PROGRESSFUNCTION / CURLOPT_PROGRESSDATA ?

	CURLcode code = curl_easy_perform(m_curl);
	if (code != CURLE_OK)
		throw http::Exception(code, m_curl.m_errbuf, "Performing query at " + url);

	if (s.response_code >= 400)
		s.throwError("querying data from " + url);
}

void HTTP::readConfig(const std::string& path, ConfigFile& cfg)
{
	using namespace http;

	CurlEasy m_curl;
	m_curl.reset();

    string url = str::joinpath(path, "config");
    BufState<std::string> content(m_curl);
    checked("setting url", curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str()));
    // CURLOPT_PROGRESSFUNCTION / CURLOPT_PROGRESSDATA ?

	CURLcode code = curl_easy_perform(m_curl);
	if (code != CURLE_OK)
		throw http::Exception(code, m_curl.m_errbuf, "Performing query at " + url);

	if (content.response_code >= 400)
		content.throwError("querying configuration from " + url);

    cfg.parse(content.buf, url);
}

void HTTP::produce_one_wrong_query()
{
	m_mischief = true;
}

std::string HTTP::expandMatcher(const std::string& matcher, const std::string& server)
{
	using namespace http;

	CurlEasy m_curl;
	m_curl.reset();

    string url = str::joinpath(server, "qexpand");
    checked("setting url", curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str()));
    checked("selecting POST method", curl_easy_setopt(m_curl, CURLOPT_POST, 1));
    string postdata = "query=" + str::encode_url(matcher) + "\n";
    checked("setting POST data", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDS, postdata.c_str()));
    checked("setting POST data size", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDSIZE, postdata.size()));
    BufState<std::string> content(m_curl);
    // CURLOPT_PROGRESSFUNCTION / CURLOPT_PROGRESSDATA ?

	CURLcode code = curl_easy_perform(m_curl);
	if (code != CURLE_OK)
		throw http::Exception(code, m_curl.m_errbuf, "Performing query at " + url);

	if (content.response_code >= 400)
		content.throwError("expanding query at " + url);

    return str::strip(content.buf);
}

void HTTP::getAliasDatabase(const std::string& server, ConfigFile& cfg)
{
	using namespace http;

	CurlEasy m_curl;
	m_curl.reset();

    string url = str::joinpath(server, "aliases");
    checked("setting url", curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str()));
    BufState<string> content(m_curl);
    // CURLOPT_PROGRESSFUNCTION / CURLOPT_PROGRESSDATA ?

	CURLcode code = curl_easy_perform(m_curl);
	if (code != CURLE_OK)
		throw http::Exception(code, m_curl.m_errbuf, "Performing query at " + url);

	if (content.response_code >= 400)
		content.throwError("expanding query at " + url);

    cfg.parse(content.buf, server);
}

static string geturlprefix(const std::string& s)
{
	// Take until /dataset/
	size_t pos = s.find("/dataset/");
	if (pos == string::npos) return string();
	return s.substr(0, pos);
}

std::string HTTP::allSameRemoteServer(const ConfigFile& cfg)
{
    string base;
    for (ConfigFile::const_section_iterator i = cfg.sectionBegin(); i != cfg.sectionEnd(); ++i)
    {
        string type = str::lower(i->second->value("type"));
		if (type != "remote") return string();
		string urlprefix = geturlprefix(i->second->value("path"));
		if (urlprefix.empty()) return string();
		if (base.empty())
			base = urlprefix;
		else if (base != urlprefix)
			return string();
	}
	return base;
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

    string url = str::joinpath(m_baseurl, "inbound/list");
    checked("setting url", curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str()));
    checked("selecting GET method", curl_easy_setopt(m_curl, CURLOPT_HTTPGET, 1));

    // Store the results in memory
    BufState<string> s(m_curl);

    CURLcode code = curl_easy_perform(m_curl);
    if (code != CURLE_OK)
        throw http::Exception(code, m_curl.m_errbuf, "Performing query at " + url);

    if (s.response_code >= 400)
        s.throwError("querying inbound/list from " + url);

    // Parse the results
    str::Split splitter(s.buf, "\n");
    for (str::Split::const_iterator i = splitter.begin(); i != splitter.end(); ++i)
        files.push_back(*i);
}

void HTTPInbound::scan(const std::string& fname, const std::string& format, metadata_dest_func dest)
{
    m_curl.reset();

    string url = str::joinpath(m_baseurl, "inbound/scan");
    checked("setting url", curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str()));
    checked("selecting POST method", curl_easy_setopt(m_curl, CURLOPT_POST, 1));
    string postdata;
    if (format.empty())
        postdata = "file=" + str::encode_url(fname);
    else
        postdata = "file=" + str::encode_url(fname) + "&format=" + str::encode_url(format);

    //fprintf(stderr, "URL: %s  POSTDATA: %s\n", url.c_str(), postdata.c_str());
    //fprintf(stderr, "POSTDATA \"%s\"", postdata.c_str());
    checked("setting POST data", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDS, postdata.c_str()));
    // Size of postfields argument if it's non text
    checked("setting POST data size", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDSIZE, postdata.size()));

    MDStreamState s(m_curl, dest, m_baseurl);
    // CURLOPT_PROGRESSFUNCTION / CURLOPT_PROGRESSDATA ?

    CURLcode code = curl_easy_perform(m_curl);
    if (code != CURLE_OK)
        throw http::Exception(code, m_curl.m_errbuf, "Performing query at " + url);

    if (s.response_code >= 400)
        s.throwError("querying inbound/scan from " + url);
}

void HTTPInbound::testdispatch(const std::string& fname, const std::string& format, NamedFileDescriptor& out)
{
    m_curl.reset();

    string url = str::joinpath(m_baseurl, "inbound/testdispatch");
    checked("setting url", curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str()));
    checked("selecting POST method", curl_easy_setopt(m_curl, CURLOPT_POST, 1));
    string postdata;
    if (format.empty())
        postdata = "file=" + str::encode_url(fname);
    else
        postdata = "file=" + str::encode_url(fname) + "&format=" + str::encode_url(format);

    //fprintf(stderr, "URL: %s  POSTDATA: %s\n", url.c_str(), postdata.c_str());
    //fprintf(stderr, "POSTDATA \"%s\"", postdata.c_str());
    checked("setting POST data", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDS, postdata.c_str()));
    // Size of postfields argument if it's non text
    checked("setting POST data size", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDSIZE, postdata.size()));

    OstreamState s(m_curl, out);
    // CURLOPT_PROGRESSFUNCTION / CURLOPT_PROGRESSDATA ?

    CURLcode code = curl_easy_perform(m_curl);
    if (code != CURLE_OK)
        throw http::Exception(code, m_curl.m_errbuf, "Performing query at " + url);

    if (s.response_code >= 400)
        s.throwError("querying inbound/scan from " + url);
}

void HTTPInbound::dispatch(const std::string& fname, const std::string& format, metadata_dest_func consumer)
{
    m_curl.reset();

    string url = str::joinpath(m_baseurl, "inbound/dispatch");
    checked("setting url", curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str()));
    checked("selecting POST method", curl_easy_setopt(m_curl, CURLOPT_POST, 1));
    string postdata;
    if (format.empty())
        postdata = "file=" + str::encode_url(fname);
    else
        postdata = "file=" + str::encode_url(fname) + "&format=" + str::encode_url(format);

    //fprintf(stderr, "URL: %s  POSTDATA: %s\n", url.c_str(), postdata.c_str());
    //fprintf(stderr, "POSTDATA \"%s\"", postdata.c_str());
    checked("setting POST data", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDS, postdata.c_str()));
    // Size of postfields argument if it's non text
    checked("setting POST data size", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDSIZE, postdata.size()));

    MDStreamState s(m_curl, consumer, m_baseurl);
    // CURLOPT_PROGRESSFUNCTION / CURLOPT_PROGRESSDATA ?

    CURLcode code = curl_easy_perform(m_curl);
    if (code != CURLE_OK)
        throw http::Exception(code, m_curl.m_errbuf, "Performing query at " + url);

    if (s.response_code >= 400)
        s.throwError("querying inbound/scan from " + url);
}

}
}
