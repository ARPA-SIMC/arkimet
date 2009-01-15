/*
 * dataset/http - Remote HTTP dataset access
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/dataset/http.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/summary.h>

#include <wibble/string.h>

#include <sstream>

using namespace std;
using namespace wibble;

namespace arki {
namespace dataset {

#define checked(context, stmt) do {\
		CURLcode errcode = stmt; \
		if (errcode != CURLE_OK) \
			throw http::Exception(errcode, context); \
	} while (0)


namespace http {

Exception::Exception(CURLcode code, const std::string& context) throw ()
	: Generic(context), m_errcode(code) {}
Exception::Exception(CURLcode code, const std::string& extrainfo, const std::string& context) throw ()
	: Generic(context), m_extrainfo(extrainfo), m_errcode(code) {}

string Exception::desc() const throw ()
{
	if (m_extrainfo.empty())
		return curl_easy_strerror(m_errcode);
	else
		return string(curl_easy_strerror(m_errcode)) + " (" + m_extrainfo + ")";
}

CurlEasy::CurlEasy() : m_curl(0)
{
	m_errbuf = new char[CURL_ERROR_SIZE];

	// FIXME:
	// curl_easy_init automatically calls curl_global_init
	// curl_global_init is not thread safe, so the documentation suggests it is
	// called not automatically here, but somewhere at program startup
	m_curl = curl_easy_init();
	if (!m_curl)
		throw wibble::exception::Consistency("initialising CURL", "curl_easy_init returned NULL");
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

}

HTTP::HTTP(const ConfigFile& cfg)
	: m_mischief(false)
{
	m_name = cfg.value("name");
	m_baseurl = cfg.value("path");
}

HTTP::~HTTP()
{
}

struct ReqState
{
    http::CurlEasy& m_curl;
    long response_code;
    std::stringstream response_error;

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
        throw wibble::exception::Consistency(context, "Server gave status " + str::fmt(response_code) + ": " + response_error.str());
    }
    ReqState(http::CurlEasy& curl) : m_curl(curl), response_code(-1) {}
};

struct SStreamState : public ReqState
{
    std::stringstream buf;

    SStreamState(http::CurlEasy& curl) : ReqState(curl) {}

    static size_t writefunc(void *ptr, size_t size, size_t nmemb, void *stream)
    {
        SStreamState& s = *(SStreamState*)stream;
        if (size_t res = s.check_error(ptr, size, nmemb)) return res;
        s.buf.write((const char*)ptr, size * nmemb);
        return size * nmemb;
    }
};

struct OstreamState : public ReqState
{
    ostream& os;

    OstreamState(http::CurlEasy& curl, ostream& os)
        : ReqState(curl), os(os) {}

    static size_t writefunc(void *ptr, size_t size, size_t nmemb, void *stream)
    {
        OstreamState& s = *(OstreamState*)stream;
        if (size_t res = s.check_error(ptr, size, nmemb)) return res;
        s.os.write((const char*)ptr, size * nmemb);
        return size * nmemb;
    }
};

struct MDStreamState : public ReqState
{
    MetadataStream mdc;

    MDStreamState(http::CurlEasy& curl, MetadataConsumer& consumer, const std::string& baseurl)
        : ReqState(curl), mdc(consumer, "HTTP download from " + baseurl) {}

    static size_t writefunc(void *ptr, size_t size, size_t nmemb, void *stream)
    {
        MDStreamState& s = *(MDStreamState*)stream;
        if (size_t res = s.check_error(ptr, size, nmemb)) return res;
        s.mdc.readData(ptr, size * nmemb);
        return size * nmemb;
    }
};


void HTTP::queryMetadata(const Matcher& matcher, bool withData, MetadataConsumer& consumer)
{
	using namespace wibble::str;

	m_curl.reset();

	string url = joinpath(m_baseurl, "query");
	checked("setting url", curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str()));
	checked("selecting POST method", curl_easy_setopt(m_curl, CURLOPT_POST, 1));
	string postdata = "query=" + urlencode(matcher.toString());
	if (m_mischief)
	{
		postdata += urlencode(";MISCHIEF");
		m_mischief = false;
	}
	if (withData)
		postdata += "&style=inline";
	postdata += '\n';
	//fprintf(stderr, "URL: %s  POSTDATA: %s\n", url.c_str(), postdata.c_str());
	//fprintf(stderr, "POSTDATA \"%s\"", postdata.c_str());
	checked("setting POST data", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDS, postdata.c_str()));
	// Size of postfields argument if it's non text
	checked("setting POST data size", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDSIZE, postdata.size()));

	MDStreamState s(m_curl, consumer, m_baseurl);
	checked("setting write function", curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, MDStreamState::writefunc));
	checked("setting write function data", curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, &s));
	// CURLOPT_PROGRESSFUNCTION / CURLOPT_PROGRESSDATA ?
	
	CURLcode code = curl_easy_perform(m_curl);
	if (code != CURLE_OK)
		throw http::Exception(code, m_curl.m_errbuf, "Performing query at " + url);

    if (s.response_code >= 400)
        s.throwError("querying summary from " + url);
}

void HTTP::querySummary(const Matcher& matcher, Summary& summary)
{
	using namespace wibble::str;

	m_curl.reset();

	string url = joinpath(m_baseurl, "summary");
	checked("setting url", curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str()));
	checked("selecting POST method", curl_easy_setopt(m_curl, CURLOPT_POST, 1));
	string postdata = "query=" + urlencode(matcher.toString());
	if (m_mischief)
	{
		postdata += urlencode(";MISCHIEF");
		m_mischief = false;
	}
	postdata += '\n';
	//fprintf(stderr, "URL: %s  POSTDATA: %s\n", url.c_str(), postdata.c_str());
	//fprintf(stderr, "POSTDATA \"%s\"", postdata.c_str());
	checked("setting POST data", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDS, postdata.c_str()));
	// Size of postfields argument if it's non text
	checked("setting POST data size", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDSIZE, postdata.size()));

    SStreamState s(m_curl);
	checked("setting write function", curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, &SStreamState::writefunc));
	checked("setting write function data", curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, &s));
	// CURLOPT_PROGRESSFUNCTION / CURLOPT_PROGRESSDATA ?
	
	CURLcode code = curl_easy_perform(m_curl);
	if (code != CURLE_OK)
		throw http::Exception(code, m_curl.m_errbuf, "Performing query at " + url);

    if (s.response_code >= 400)
        s.throwError("querying summary from " + url);

	s.buf.seekg(0);
	summary.read(s.buf, url);
}

void HTTP::queryBytes(const Matcher& matcher, std::ostream& out, ByteQuery qtype, const std::string& param)
{
	using namespace wibble::str;

	m_curl.reset();

	string url = joinpath(m_baseurl, "query");
	checked("setting url", curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str()));
	checked("selecting POST method", curl_easy_setopt(m_curl, CURLOPT_POST, 1));
	string postdata = "query=" + urlencode(matcher.toString());
	if (m_mischief)
	{
		postdata += urlencode(";MISCHIEF");
		m_mischief = false;
	}
	switch (qtype)
	{
		case ReadonlyDataset::BQ_DATA:
			postdata += "&style=data";
			break;
		case ReadonlyDataset::BQ_POSTPROCESS:
			postdata += "&style=postprocess&command=" + urlencode(param);
			break;
		case ReadonlyDataset::BQ_REP_METADATA:
			postdata += "&style=rep_metadata&command=" + urlencode(param);
			break;
		case ReadonlyDataset::BQ_REP_SUMMARY:
			postdata += "&style=rep_summary&command=" + urlencode(param);
			break;
		default:
			throw wibble::exception::Consistency("querying dataset", "unsupported query type: " + fmt((int)qtype));
	}
	postdata += '\n';
	//fprintf(stderr, "URL: %s  POSTDATA: %s\n", url.c_str(), postdata.c_str());
	//fprintf(stderr, "POSTDATA \"%s\"", postdata.c_str());
	checked("setting POST data", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDS, postdata.c_str()));
	// Size of postfields argument if it's non text
	checked("setting POST data size", curl_easy_setopt(m_curl, CURLOPT_POSTFIELDSIZE, postdata.size()));
    OstreamState s(m_curl, out);
	checked("setting write function", curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, OstreamState::writefunc));
	checked("setting write function data", curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, &s));
	// CURLOPT_PROGRESSFUNCTION / CURLOPT_PROGRESSDATA ?
	
	CURLcode code = curl_easy_perform(m_curl);
	if (code != CURLE_OK)
		throw http::Exception(code, m_curl.m_errbuf, "Performing query at " + url);

    if (s.response_code >= 400)
        s.throwError("querying configuration from " + url);
}

void HTTP::readConfig(const std::string& path, ConfigFile& cfg)
{
	using namespace wibble::str;
	using namespace http;

	CurlEasy m_curl;
	m_curl.reset();

	string url = joinpath(path, "config");
	SStreamState content(m_curl);
	checked("setting url", curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str()));
	checked("setting write function", curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, &SStreamState::writefunc));
	checked("setting write function data", curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, &content));
	// CURLOPT_PROGRESSFUNCTION / CURLOPT_PROGRESSDATA ?
	
	CURLcode code = curl_easy_perform(m_curl);
	if (code != CURLE_OK)
		throw http::Exception(code, m_curl.m_errbuf, "Performing query at " + url);

    if (content.response_code >= 400)
        content.throwError("querying configuration from " + url);

	content.buf.seekg(0);
	cfg.parse(content.buf, url);
}

void HTTP::produce_one_wrong_query()
{
	m_mischief = true;
}

}
}
// vim:set ts=4 sw=4:
