#ifndef ARKI_DATASET_HTTP_H
#define ARKI_DATASET_HTTP_H

/*
 * dataset/http - Remote HTTP dataset access
 *
 * Copyright (C) 2007--2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/local.h>
#include <wibble/exception.h>
#include <curl/curl.h>
#include <string>

namespace arki {

class ConfigFile;
class Metadata;
class MetadataConsumer;
class Matcher;

namespace dataset {

namespace http {
class Exception : public wibble::exception::Generic
{
protected:
	std::string m_extrainfo;
	CURLcode m_errcode;

public:
	Exception(CURLcode code, const std::string& context) throw ();
	Exception(CURLcode code, const std::string& extrainfo, const std::string& context) throw ();
	~Exception() throw () {}

	virtual const char* type() const throw () { return "HTTP"; }

	/// Get the system error code associated to the exception
	virtual int code() const throw () { return m_errcode; }

	/// Get the description of the error code
	virtual std::string desc() const throw ();
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
class HTTP : public Local
{
protected:
	std::string m_name;
	std::string m_baseurl;
	mutable http::CurlEasy m_curl;
	bool m_mischief;

public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	HTTP(const ConfigFile& cfg);
	virtual ~HTTP();

	/**
	 * Query the dataset using the given matcher, and sending the results to
	 * the metadata consumer.
	 */
	virtual void queryData(const dataset::DataQuery& q, MetadataConsumer& consumer);
	virtual void querySummary(const Matcher& matcher, Summary& summary);
	virtual void queryBytes(const dataset::ByteQuery& q, std::ostream& out);

	static void readConfig(const std::string& path, ConfigFile& cfg);

    /**
     * Expand the given matcher expression using the aliases on this server
     */
    static std::string expandMatcher(const std::string& matcher, const std::string& server);

	/**
	 * Introduce a syntax error in the next query sent to the server.
	 *
	 * This is only used by test suites: since the queries are preparsed
	 * client-side, there is no other obvious way to produce a repeatable error
	 * message in the server.  You have to call it before every query you want
	 * to fail.
	 */
	void produce_one_wrong_query();
};

}
}

// vim:set ts=4 sw=4:
#endif
