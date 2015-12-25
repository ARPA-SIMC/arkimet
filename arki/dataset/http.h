#ifndef ARKI_DATASET_HTTP_H
#define ARKI_DATASET_HTTP_H

/// Remote HTTP dataset access

#include <arki/dataset/local.h>
#include <curl/curl.h>
#include <string>

namespace arki {
class ConfigFile;
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

struct CurlEasy
{
	CURL* m_curl;
	char* m_errbuf;

	CurlEasy();
	~CurlEasy();

	void reset();

	operator CURL*() { return m_curl; }
	operator const CURL*() const { return m_curl; }
};

}

/**
 * Remote HTTP access to a Dataset.
 *
 * This dataset implements all the dataset query functions through HTTP
 * requests to an Arkimet web server.
 *
 * The dataset is read only: remote import of new data is not supported.
 */
class HTTP : public Reader
{
protected:
	std::string m_name;
	std::string m_baseurl;
	std::string m_qmacro;
	mutable http::CurlEasy m_curl;
	bool m_mischief;

public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	HTTP(const ConfigFile& cfg);
	virtual ~HTTP();

    void query_data(const dataset::DataQuery& q, metadata_dest_func) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
    void query_bytes(const dataset::ByteQuery& q, int out) override;

	static void readConfig(const std::string& path, ConfigFile& cfg);

	/**
	 * Expand the given matcher expression using the aliases on this server
	 */
	static std::string expandMatcher(const std::string& matcher, const std::string& server);

	/**
	 * Read the alias database from the given remote dataset
	 */
	static void getAliasDatabase(const std::string& server, ConfigFile& cfg);

	/**
	 * Introduce a syntax error in the next query sent to the server.
	 *
	 * This is only used by test suites: since the queries are preparsed
	 * client-side, there is no other obvious way to produce a repeatable error
	 * message in the server.  You have to call it before every query you want
	 * to fail.
	 */
	void produce_one_wrong_query();

	/**
	 * Check if all the datasets in the given config are remote and from
	 * the same server.
	 *
	 * @return the common server URL, or the empty string if some datasets
	 * are local or from different servers
	 */
	static std::string allSameRemoteServer(const ConfigFile& cfg);
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
    void testdispatch(const std::string& fname, const std::string& format, int out);

    /// Run a dispatch on a previously uploaded file
    void dispatch(const std::string& fname, const std::string& format, metadata_dest_func consumer);
};

}
}
#endif
