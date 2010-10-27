/*
 * arki-server - Arkimet server
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <wibble/exception.h>
#include <wibble/commandline/parser.h>
#include <wibble/regexp.h>
#include <wibble/string.h>
#include <wibble/sys/process.h>
#include <wibble/sys/childprocess.h>
#include <wibble/sys/fs.h>
#include <wibble/log/stream.h>
#include <wibble/log/syslog.h>
#include <wibble/log/file.h>
#include <wibble/log/ostream.h>
#include <wibble/log/filters.h>
#include <arki/configfile.h>
#include <arki/dataset.h>
#include <arki/dataset/http.h>
#include <arki/summary.h>
#include <arki/matcher.h>
#include <arki/runtime.h>
#include <arki/runtime/config.h>
#include <wibble/net/server.h>
#include <arki/utils/http.h>
//#include <arki/utils/lua.h>

#include <string>
#include <vector>
#include <map>
#include <sstream>
#include <fstream>
#include <iostream>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <fcntl.h>

#include "config.h"

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace wibble;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	StringOption* host;
	StringOption* port;
	StringOption* url;
	StringOption* runtest;
	StringOption* accesslog;
	StringOption* errorlog;
	BoolOption* syslog;
	BoolOption* quiet;

	Options() : StandardParserWithManpage("arki-server", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] configfile";
		description =
			"Start the arkimet server, serving the datasets found"
			" in the configuration file";

		host = add<StringOption>("host", 0, "host", "hostname",
				"interface to listen to. Default: all interfaces");
		port = add<StringOption>("port", 'p', "port", "port",
				"port to listen not. Default: 8080");
		url = add<StringOption>("url", 0, "url", "url",
				"url to use to reach the server");
		runtest = add<StringOption>("runtest", 0, "runtest", "cmd",
				"start the server, run the given test command"
				" and return its exit status");
		accesslog = add<StringOption>("accesslog", 0, "accesslog", "file",
				"file where to log normal access information");
		errorlog = add<StringOption>("errorlog", 0, "errorlog", "file",
				"file where to log errors");
		syslog = add<BoolOption>("syslog", 0, "syslog", "",
			"log to system log");
		quiet = add<BoolOption>("quiet", 0, "quiet", "",
			"do not log to standard output");
	}
};

}
}

/// Delete the directory \a dir and all its content
void rmtree(const std::string& dir)
{
	sys::fs::Directory d(dir);
	for (sys::fs::Directory::const_iterator i = d.begin();
			i != d.end(); ++i)
	{
		if (*i == "." || *i == "..") continue;
		string pathname = str::joinpath(dir, *i);
		if (i->d_type == DT_DIR ||
		    (i->d_type == DT_UNKNOWN && sys::fs::isDirectory(pathname)))
		{
			rmtree(pathname);
		} else {
			if (unlink(pathname.c_str()) < 0)
				throw wibble::exception::System("cannot delete " + pathname);
		}
	}
	if (rmdir(dir.c_str()) < 0)
		throw wibble::exception::System("cannot delete directory " + dir);
}

/**
 * RAII-style class changing into a newly created temporary directory during
 * the lifetime of the object.
 *
 * The temporary directory is created at constructor time and deleted at
 * destructor time.
 */
struct MoveToTempDir
{
	string old_dir;
	string tmp_dir;

	MoveToTempDir(const std::string& prefix = "/tmp/tmpdir.XXXXXX")
	{
		old_dir = sys::process::getcwd();
		char buf[prefix.size() + 1];
		memcpy(buf, prefix.c_str(), prefix.size() + 1);
		if (mkdtemp(buf) == NULL)
			throw wibble::exception::System("cannot create temporary directory");
		tmp_dir = buf;
		sys::process::chdir(tmp_dir);
	}
	~MoveToTempDir()
	{
		sys::process::chdir(old_dir);
		rmtree(tmp_dir);
	}
};

struct Request : public http::Request
{
	ConfigFile arki_conf;
	ConfigFile arki_conf_remote;

	const ConfigFile& get_config_remote(const std::string& dsname)
	{
		const ConfigFile* cfg = arki_conf_remote.section(dsname);
		if (cfg == NULL)
			throw http::error404();
		return *cfg;
	}

	const ConfigFile& get_config(const std::string& dsname)
	{
		const ConfigFile* cfg = arki_conf.section(dsname);
		if (cfg == NULL)
			throw http::error404();
		return *cfg;
	}

	auto_ptr<ReadonlyDataset> get_dataset(const std::string& dsname)
	{
		return get_dataset(get_config(dsname));
	}

	auto_ptr<ReadonlyDataset> get_dataset(const ConfigFile& cfg)
	{
		return auto_ptr<ReadonlyDataset>(ReadonlyDataset::create(cfg));
	}

};


/// Base interface for GET or POST parameters
struct Param
{
	virtual ~Param() {}

	// This can be called more than once, if the value is found multiple
	// times, nor never, if the value is never found
	virtual void parse(const std::string& str) = 0;
};

/// Single-valued parameter
struct ParamSingle : public std::string, public Param
{
	virtual void parse(const std::string& str)
	{
		assign(str);
	}
};

/// Multi-valued parameter
struct ParamMulti : public std::vector<std::string>, public Param
{
	virtual void parse(const std::string& str)
	{
		push_back(str);
	}
};

struct FileParam
{
	struct FileInfo
	{
		std::string fname;
		std::string client_fname;

		bool read(net::mime::Reader& mime_reader,
			  map<string, string> headers,
			  const std::string& outdir,
			  const std::string& fname_blacklist,
			  const std::string& client_fname,
			  int sock,
			  const std::string& boundary,
			  size_t inputsize);
	};

	virtual ~FileParam() {}

	virtual bool read(
			net::mime::Reader& mime_reader,
			map<string, string> headers,
			const std::string& outdir,
			const std::string& fname_blacklist,
			const std::string& client_fname,
			int sock,
			const std::string& boundary,
			size_t inputsize) = 0;
};

bool FileParam::FileInfo::read(
		net::mime::Reader& mime_reader,
		map<string, string> headers,
		const std::string& outdir,
		const std::string& fname_blacklist,
		const std::string& client_fname,
		int sock,
		const std::string& boundary,
		size_t inputsize)
{
	int openflags = O_CREAT | O_WRONLY;

	// Store the client provided pathname
	this->client_fname = client_fname;

	// Generate the output file name
	if (fname.empty())
	{
		fname = client_fname;
		openflags |= O_EXCL;
	}
	string preferred_fname = str::basename(fname);

	// Replace blacklisted chars
	if (!fname_blacklist.empty())
		for (string::iterator i = preferred_fname.begin(); i != preferred_fname.end(); ++i)
			if (fname_blacklist.find(*i) != string::npos)
				*i = '_';

	preferred_fname = str::joinpath(outdir, preferred_fname);

	fname = preferred_fname;

	// Create the file
	int outfd;
	for (unsigned i = 1; ; ++i)
	{
		outfd = open(fname.c_str(), openflags, 0600);
		if (outfd >= 0) break;
		if (errno != EEXIST)
			throw wibble::exception::File(fname, "creating file");
		// Alter the file name and try again
		fname = preferred_fname + str::fmtf(".%u", i);
	}

	// Wrap output FD into a stream, which will take care of
	// closing it
	wibble::stream::PosixBuf posixBuf(outfd);
	ostream out(&posixBuf);

	// Read until boundary, sending data to temp file

	bool has_part = mime_reader.read_until_boundary(sock, boundary, out, inputsize);

	return has_part;
}

struct FileParamSingle : public FileParam
{
	FileInfo info;

	/**
	 * If a file name is given, use its base name for storing the file;
	 * else, use the file name given by the client, without path
	 */
	FileParamSingle(const std::string& fname=std::string())
	{
		info.fname = fname;
	}

	virtual bool read(
			net::mime::Reader& mime_reader,
			map<string, string> headers,
			const std::string& outdir,
			const std::string& fname_blacklist,
			const std::string& client_fname,
			int sock,
			const std::string& boundary,
			size_t inputsize)
	{
		return info.read(mime_reader, headers, outdir, fname_blacklist, client_fname, sock, boundary, inputsize);
	}
};

struct FileParamMulti : public FileParam
{
	std::vector<FileInfo> files;

	virtual bool read(
			net::mime::Reader& mime_reader,
			map<string, string> headers,
			const std::string& outdir,
			const std::string& fname_blacklist,
			const std::string& client_fname,
			int sock,
			const std::string& boundary,
			size_t inputsize)
	{
		files.push_back(FileInfo());
		return files.back().read(mime_reader, headers, outdir, fname_blacklist, client_fname, sock, boundary, inputsize);
	}
};

/**
 * Parse and store HTTP query parameters
 *
 * It can be preconfigured witn 
 */
struct Params : public std::map<std::string, Param*> 
{
	/// File parameters
	std::map<std::string, FileParam*> files;

	/// Maximum size of POST input data
	size_t conf_max_input_size;

	/// Maximum size of field data for one non-file field
	size_t conf_max_field_size;

	/**
	 * Whether to accept unknown fields.
	 *
	 * If true, unkown fields are stored as ParamMulti
	 *
	 * If false, unknown fields are ignored.
	 */
	bool conf_accept_unknown_fields;

	/**
	 * Whether to accept unknown file upload fields.
	 *
	 * If true, unkown fields are stored as FileParamMulti
	 *
	 * If false, unknown file upload fields are ignored.
	 */
	bool conf_accept_unknown_file_fields;

	/**
	 * Directory where we write uploaded files
	 *
	 * @warning: if it is not set to anything, it ignores all file uploads
	 */
	std::string conf_outdir;

	/**
	 * String containing blacklist characters that are replaced with "_" in
	 * the file name. If empty, nothing is replaced.
	 *
	 * This only applies to the basename: the pathname is ignored when
	 * building the local file name.
	 */
	std::string conf_fname_blacklist;


	Params()
	{
		conf_max_input_size = 20 * 1024 * 1024;
		conf_max_field_size = 1024 * 1024;
		conf_accept_unknown_fields = false;
		conf_accept_unknown_file_fields = false;
	}
	~Params()
	{
		for (iterator i = begin(); i != end(); ++i)
			delete i->second;
		for (std::map<std::string, FileParam*>::iterator i = files.begin(); i != files.end(); ++i)
			delete i->second;
	}

	template<typename TYPE>
	TYPE* add(const std::string& name)
	{
		TYPE* res = new TYPE;
		add(name, res);
		return res;
	}

	void add(const std::string& name, Param* param)
	{
		iterator i = find(name);
		if (i != end())
		{
			delete i->second;
			i->second = param;
		} else
			insert(make_pair(name, param));
	}

	void add(const std::string& name, FileParam* param)
	{
		std::map<std::string, FileParam*>::iterator i = files.find(name);
		if (i != files.end())
		{
			delete i->second;
			i->second = param;
		} else
			files.insert(make_pair(name, param));
	}

	Param* obtain_field(const std::string& name)
	{
		iterator i = find(name);
		if (i != end())
			return i->second;
		if (!conf_accept_unknown_fields)
			return NULL;
		pair<iterator, bool> res = insert(make_pair(name, new ParamMulti));
		return res.first->second;
	}

	FileParam* obtain_file_field(const std::string& name)
	{
		std::map<std::string, FileParam*>::iterator i = files.find(name);
		if (i != files.end())
			return i->second;
		if (!conf_accept_unknown_file_fields)
			return NULL;
		pair<std::map<std::string, FileParam*>::iterator, bool> res =
			files.insert(make_pair(name, new FileParamMulti));
		return res.first->second;
	}

	Param* field(const std::string& name)
	{
		iterator i = find(name);
		if (i != end())
			return i->second;
		return NULL;
	}

	FileParam* file_field(const std::string& name)
	{
		std::map<std::string, FileParam*>::iterator i = files.find(name);
		if (i != files.end())
			return i->second;
		return NULL;
	}

	void parse_get_or_post(http::Request& req)
	{
		if (req.method == "GET")
		{
			size_t pos = req.url.find('?');
			if (pos != string::npos)
				parse_urlencoded(req.url.substr(pos + 1));
		}
		else if (req.method == "POST")
			parse_post(req);
		else
			throw wibble::exception::Consistency("cannot parse parameters from \"" + req.method + "\" request");
	}

	void parse_urlencoded(const std::string& qstring)
	{
		// Split on &
		str::Split splitter("&", qstring);
		for (str::Split::const_iterator i = splitter.begin();
				i != splitter.end(); ++i)
		{
			if (i->empty()) continue;

			// Split on =
			size_t pos = i->find('=');
			if (pos == string::npos)
			{
				// foo=
				Param* p = obtain_field(str::urldecode(*i));
				if (p != NULL)
					p->parse(string());
			}
			else
			{
				// foo=bar
				Param* p = obtain_field(str::urldecode(i->substr(0, pos)));
				if (p != NULL)
					p->parse(str::urldecode(i->substr(pos+1)));
			}
		}
	}

	void parse_multipart(http::Request& req, size_t inputsize, const std::string& content_type)
	{
        net::mime::Reader mime_reader;

		// Get the mime boundary
		size_t pos = content_type.find("boundary=");
		if (pos == string::npos)
			throw http::error400("no boundary in content-type");
		// TODO: strip boundary of leading and trailing "
		string boundary = "--" + content_type.substr(pos + 9);

		// Read until first boundary, discarding data
		mime_reader.discard_until_boundary(req.sock, boundary);

		boundary = "\r\n" + boundary;

		// Content-Disposition: form-data; name="submit-name"
		//wibble::ERegexp re_disposition(";%s*([^%s=]+)=\"(.-)\"", 2);
		wibble::Splitter cd_splitter("[[:blank:]]*;[[:blank:]]*", REG_EXTENDED);
		wibble::ERegexp cd_parm("([^=]+)=\"([^\"]+)\"", 3);

		bool has_part = true;
		while (has_part)
		{
			// Read mime headers for this part
			map<string, string> headers;
			if (!mime_reader.read_headers(req.sock, headers))
				throw http::error400("request truncated at MIME headers");

			// Get name and (optional) filename from content-disposition
			map<string, string>::const_iterator i = headers.find("content-disposition");
			if (i == headers.end())
				throw http::error400("no Content-disposition in MIME headers");
			wibble::Splitter::const_iterator j = cd_splitter.begin(i->second);
			if (j == cd_splitter.end())
				throw http::error400("incomplete content-disposition header");
			if (*j != "form-data")
				throw http::error400("Content-disposition is not \"form-data\"");
			string name;
			string filename;
			for (++j; j != cd_splitter.end(); ++j)
			{
				if (!cd_parm.match(*j)) continue;
				string key = cd_parm[1];
				if (key == "name")
				{
					name = cd_parm[2];
				} else if (key == "filename") {
					filename = cd_parm[2];
				}
			}

			if (!filename.empty())
			{
				// Get a file param
				FileParam* p = conf_outdir.empty() ? NULL : obtain_file_field(name);
				if (p != NULL)
					has_part = p->read(mime_reader, headers, conf_outdir, conf_fname_blacklist, filename, req.sock, boundary, inputsize);
				else
					has_part = mime_reader.discard_until_boundary(req.sock, boundary);
			} else {
				// Read until boundary, storing data in string
				stringstream value;
				has_part = mime_reader.read_until_boundary(req.sock, boundary, value, conf_max_field_size);

				// Store the field value
				Param* p = obtain_field(name);
				if (p != NULL)
					p->parse(value.str());
			}
		}
	}

	void parse_post(http::Request& req)
	{
		// Get the supposed size of incoming data
		map<string, string>::const_iterator i = req.headers.find("content-length");
		if (i == req.headers.end())
			throw wibble::exception::Consistency("no Content-Length: found in request header");
		// Validate the post size
		size_t inputsize = strtoul(i->second.c_str(), 0, 10);
		if (inputsize > conf_max_input_size)
		{
			// Discard all input
			req.discard_input();
			throw wibble::exception::Consistency(str::fmtf(
				"Total size of incoming data (%zdb) exceeds configured maximum (%zdb)",
				inputsize, conf_max_input_size));
		}

		// Get the content type
		i = req.headers.find("content-type");
		if (i == req.headers.end())
			throw wibble::exception::Consistency("no Content-Type: found in request header");
		if (i->second.find("x-www-form-urlencoded") != string::npos)
		{
			string line;
			req.read_buf(req.sock, line, inputsize);
			parse_urlencoded(line);
		}
		else if (i->second.find("multipart/form-data") != string::npos)
		{
			parse_multipart(req, inputsize, i->second);
		}
		else
			throw wibble::exception::Consistency("unsupported Content-Type: " + i->second);
	}
};

// Interface for local request handlers
struct LocalHandler
{
	virtual ~LocalHandler() {}
	virtual void operator()(Request& req) = 0;
};

// Repository of local request handlers
struct LocalHandlers
{
	std::map<string, LocalHandler*> handlers;

	~LocalHandlers()
	{
		for (std::map<string, LocalHandler*>::iterator i = handlers.begin();
				i != handlers.end(); ++i)
			delete i->second;
	}

	void add(const std::string& name, LocalHandler* handler)
	{
		add(name, auto_ptr<LocalHandler>(handler));
	}

	// Add a local handler for the given script name
	void add(const std::string& name, std::auto_ptr<LocalHandler> handler)
	{
		std::map<string, LocalHandler*>::iterator i = handlers.find(name);
		if (i == handlers.end())
			handlers[name] = handler.release();
		else
		{
			delete i->second;
			i->second = handler.release();
		}
	}

	// Return true if it handled it, else false
	bool try_do(Request& req)
	{
		std::map<string, LocalHandler*>::iterator i = handlers.find(req.script_name);
		if (i == handlers.end())
			return false;
		(*(i->second))(req);
		return true;
	}
};

/*
struct ScriptHandlers
{
	string scriptdir;

	ScriptHandlers()
	{
		// Directory where we find our CGI scripts
		scriptdir = SERVER_DIR;
		const char* dir = getenv("ARKI_SERVER");
		if (dir != NULL)
			scriptdir = dir;
	}

	bool try_do(Request& req)
	{
		string scriptpath = str::joinpath(scriptdir, req.script_name) + ".lua";
		if (!sys::fs::access(scriptpath, R_OK))
			return false;

		// Run the CGI

		// Setup CGI environment
		req.set_cgi_env();

		// Connect stdin and stdout to the socket
		if (dup2(req.sock, 0) < 0)
			throw wibble::exception::System("redirecting input socket to stdin");
		if (dup2(req.sock, 1) < 0)
			throw wibble::exception::System("redirecting input socket to stdout");

		// Create and populate the Lua VM
		Lua L;
		types::Type::lua_loadlib(L);

		// Run the script
		if (luaL_dofile(L, scriptpath.c_str()))
		{
			string error = lua_tostring(L, -1);
			cerr << error << endl;
		}

		close(0);
		close(1);

		return true;
	}
};
*/

LocalHandlers local_handlers;
//ScriptHandlers script_handlers;

/// Show a list of all available datasets
struct IndexHandler : public LocalHandler
{
	virtual void operator()(Request& req)
	{
		stringstream res;
		res << "<html><body>" << endl;
		res << "Available datasets:" << endl;
		res << "<ul>" << endl;
		for (ConfigFile::const_section_iterator i = req.arki_conf.sectionBegin();
				i != req.arki_conf.sectionEnd(); ++i)
		{
			res << "<li><a href='/dataset/" << i->first << "'>";
			res << i->first << "</a></li>" << endl;
		}
		res << "</ul>" << endl;
		res << "<a href='/query'>Perform a query</a>" << endl;
		res << "</body></html>" << endl;
		req.send_result(res.str());
	}
};

/// Return the configuration
struct ConfigHandler : public LocalHandler
{
	virtual void operator()(Request& req)
	{
		// if "json" in args:
		//	return server.configdict
		stringstream out;
		req.arki_conf_remote.output(out, "(memory)");
		req.send_result(out.str(), "text/plain");
	}
};

/// Dump the alias database
struct AliasesHandler : public LocalHandler
{
	virtual void operator()(Request& req)
	{
		ConfigFile cfg;
		MatcherAliasDatabase::serialise(cfg);

		stringstream out;
		cfg.output(out, "(memory)");

		req.send_result(out.str(), "text/plain");
	}
};

// Expand a query
struct QexpandHandler : public LocalHandler
{
	virtual void operator()(Request& req)
	{
		Params params;
		ParamSingle* query = params.add<ParamSingle>("query");
		params.parse_get_or_post(req);
		Matcher m = Matcher::parse(*query);
		string out = m.toStringExpanded();
		req.send_result(out, "text/plain");
	}
};

// Common code for handling queries
struct QueryHelper
{
	Params params;
	string content_type;
	string ext;
	runtime::ProcessorMaker pmaker;

	ParamSingle* query;
	ParamSingle* style;
	ParamSingle* command;
	FileParamMulti* postprocfile;
	ParamSingle* sort;

	QueryHelper()
		: content_type("application/octet-stream"), ext("bin")
	{
		params.conf_fname_blacklist = ":";
		query = params.add<ParamSingle>("query");
		style = params.add<ParamSingle>("style");
		command = params.add<ParamSingle>("command");
		postprocfile = params.add<FileParamMulti>("postprocfile");
		sort = params.add<ParamSingle>("sort");
	}

	void parse_request(Request& req)
	{
		params.parse_get_or_post(req);

		// Configure the ProcessorMaker with the request
		if (style->empty() || *style == "metadata") {
			;
		} else if (*style == "yaml") {
			content_type = "text/x-yaml";
			ext = "yaml";
			pmaker.yaml = true;
		} else if (*style == "inline") {
			pmaker.data_inline = true;
		} else if (*style == "data") {
			pmaker.data_only = true;
		} else if (*style == "postprocess") {
			pmaker.postprocess = *command;

			vector<string> postproc_files;
			for (vector<FileParam::FileInfo>::const_iterator i = postprocfile->files.begin();
					i != postprocfile->files.end(); ++i)
				postproc_files.push_back(i->fname);

			if (!postproc_files.empty())
			{
				// Pass files for the postprocessor in the environment
				string val = str::join(postproc_files.begin(), postproc_files.end(), ":");
				setenv("ARKI_POSTPROC_FILES", val.c_str(), 1);
			} else
				unsetenv("ARKI_POSTPROC_FILES");
		} else if (*style == "rep_metadata") {
			pmaker.report = *command;
			content_type = "text/plain";
			ext = "txt";
		} else if (*style == "rep_summary") {
			pmaker.summary = true;
			pmaker.report = *command;
			content_type = "text/plain";
			ext = "txt";
		}

		if (!sort->empty())
			pmaker.sort = *sort;

		// Validate request
		string errors = pmaker.verify_option_consistency();
		if (!errors.empty())
			throw http::error400(errors);
	}

	void send_headers(Request& req, const std::string& fname)
	{
		req.send_status_line(200, "OK");
		req.send_date_header();
		req.send_server_header();
		req.send("Content-Type: " + content_type + "\r\n");
		req.send("Content-Disposition: attachment; filename=" + fname + "." + ext + "\r\n");
		req.send("\r\n");
	}

	void do_processing(ReadonlyDataset& ds, Request& req, Matcher& matcher, const std::string& fname);
};

struct StreamHeaders : public metadata::Hook
{
	QueryHelper& qhelper;
	Request& req;
	std::string fname;
	bool fired;

	StreamHeaders(QueryHelper& qhelper, Request& req, const std::string& fname)
		: qhelper(qhelper), req(req), fname(fname), fired(false)
	{
	}

	virtual void operator()()
	{
		// Send headers for streaming
		qhelper.send_headers(req, fname);
		fired = true;
	}

	void sendIfNotFired()
	{
		if (!fired) operator()();
	}
};

void QueryHelper::do_processing(ReadonlyDataset& ds, Request& req, Matcher& matcher, const std::string& fname)
{
	StreamHeaders headers_hook(*this, req, fname);

	// If we are postprocessing, we cannot monitor the postprocessor
	// output to hook sending headers: the postprocessor is
	// connected directly to the output socket.
	//
	// Therefore we need to send the headers in advance.

	{
		// Create Output directed to req.sock
		runtime::Output sockoutput(req.sock, "socket");

		if (pmaker.postprocess.empty())
			// Send headers when data starts flowing
			sockoutput.set_hook(headers_hook);
		else
			// Hook sending headers to when the subprocess start sending
            pmaker.data_start_hook = &headers_hook;

		// Create the dataset processor for this query
		auto_ptr<runtime::DatasetProcessor> p = pmaker.make(matcher, sockoutput);

		// Process the virtual qmacro dataset producing the output
		p->process(ds, fname);
		p->end();
	}

	// If we had empty output, headers were not sent: catch up
	headers_hook.sendIfNotFired();
}

struct RootQueryHandler : public LocalHandler
{
	virtual void operator()(Request& req)
	{
		// Work in a temporary directory
		MoveToTempDir tempdir("/tmp/arki-server.XXXXXX");

		QueryHelper qhelper;
		qhelper.params.conf_outdir = tempdir.tmp_dir;
		ParamSingle* qmacro = qhelper.params.add<ParamSingle>("qmacro");
		qhelper.parse_request(req);

		string macroname = str::trim(*qmacro);

		if (macroname.empty())
			throw http::error400("root-level query without qmacro parameter");

		// Create qmacro dataset
		auto_ptr<ReadonlyDataset> ds = runtime::make_qmacro_dataset(
				req.arki_conf, macroname, *qhelper.query);

		// Stream out data
		Matcher emptyMatcher;
		qhelper.do_processing(*ds, req, emptyMatcher, macroname);

		// End of streaming
	}
};

struct RootSummaryHandler : public LocalHandler
{
	virtual void operator()(Request& req)
	{
		Params params;
		ParamSingle* style = params.add<ParamSingle>("style");
		ParamSingle* query = params.add<ParamSingle>("query");
		ParamSingle* qmacro = params.add<ParamSingle>("qmacro");
		params.parse_get_or_post(req);

		string macroname = str::trim(*qmacro);

		if (macroname.empty())
			throw http::error400("root-level query without qmacro parameter");

		// Create qmacro dataset
		auto_ptr<ReadonlyDataset> ds = runtime::make_qmacro_dataset(
				req.arki_conf, macroname, *query);

		// Query the summary
		Summary sum;
		ds->querySummary(Matcher(), sum);

		if (*style == "yaml")
		{
			stringstream res;
			sum.writeYaml(res);
			req.send_result(res.str(), "text/x-yaml", macroname + "-summary.yaml");
		}
		else
		{
			string res = sum.encode(true);
			req.send_result(res, "application/octet-stream", macroname + "-summary.bin");
		}
	}
};

// Dispatch dataset-specific actions
struct DatasetHandler : public LocalHandler
{
	wibble::ERegexp pop_first_path;

	DatasetHandler()
		: pop_first_path("^/*([^/]+)(/+(.+))?", 4)
	{
	}

	// Show the summary of a dataset
	void do_index(Request& req, const std::string& dsname)
	{
		auto_ptr<ReadonlyDataset> ds = req.get_dataset(dsname);

		// Query the summary
		Summary sum;
		ds->querySummary(Matcher(), sum);

		// Create the output page
		stringstream res;
		res << "<html>" << endl;
		res << "<head><title>Dataset " << dsname << "</title></head>" << endl;
		res << "<body>" << endl;
		res << "<ul>" << endl;
		res << "<li><a href='/'>All datasets</a></li>" << endl;
		res << "<li><a href='/dataset/" << dsname << "/queryform'>Query</a></li>" << endl;
		res << "<li><a href='/dataset/" << dsname << "/summary'>Download summary</a></li>" << endl;
		res << "</ul>" << endl;
		res << "<p>Summary of dataset <b>" << dsname << "</b>:</p>" << endl;
		res << "<pre>" << endl;
		sum.writeYaml(res);
		res << "</pre>" << endl;
		res << "</body>" << endl;
		res << "</html>" << endl;

		req.send_result(res.str());
	}

	// Show a form to query the dataset
	void do_queryform(Request& req, const std::string& dsname)
	{
		stringstream res;
		res << "<html>" << endl;
		res << "<head><title>Query dataset " << dsname << "</title></head>" << endl;
		res << "<body>" << endl;
		res << "  Please type or paste your query and press submit:" << endl;
		res << "  <form action='/dataset/" << dsname << "/query' method='push'>" << endl;
		res << "    <textarea name='query' cols='80' rows='15'>" << endl;
		res << "    </textarea>" << endl;
		res << "    <br/>" << endl;
		res << "    <select name='style'>" << endl;
		res << "      <option value='data'>Data</option>" << endl;
		res << "      <option value='yaml'>Human-readable metadata</option>" << endl;
		res << "      <option value='inline'>Binary metadata and data</option>" << endl;
		res << "      <option value='md'>Binary metadata</option>" << endl;
		res << "    </select>" << endl;
		res << "    <br/>" << endl;
		res << "    <input type='submit'>" << endl;
		res << "  </form>" << endl;
		res << "</body>" << endl;
		res << "</html>" << endl;
		req.send_result(res.str());
	}

	// Return the dataset configuration
	void do_config(Request& req, const std::string& dsname)
	{
		// args = Args()
		// if "json" in args:
		//    return c

		stringstream res;
		res << "[" << dsname << "]" << endl;
		req.get_config_remote(dsname).output(res, "(memory)");
		req.send_result(res.str(), "text/plain");
	}

	// Generate a dataset summary
	void do_summary(Request& req, const std::string& dsname)
	{
		Params params;
		ParamSingle* style = params.add<ParamSingle>("style");
		ParamSingle* query = params.add<ParamSingle>("query");
		params.parse_get_or_post(req);

		auto_ptr<ReadonlyDataset> ds = req.get_dataset(dsname);

		// Query the summary
		Summary sum;
		ds->querySummary(Matcher::parse(*query), sum);

		if (*style == "yaml")
		{
			stringstream res;
			sum.writeYaml(res);
			req.send_result(res.str(), "text/x-yaml", dsname + "-summary.yaml");
		}
		else
		{
			string res = sum.encode(true);
			req.send_result(res, "application/octet-stream", dsname + "-summary.bin");
		}
	}

	// Download the results of querying a dataset
	void do_query(Request& req, const std::string& dsname)
	{
		// Work in a temporary directory
		MoveToTempDir tempdir("/tmp/arki-server.XXXXXX");

		QueryHelper qhelper;
		qhelper.params.conf_outdir = tempdir.tmp_dir;
		qhelper.parse_request(req);

		// Validate query
		Matcher matcher;
		try {
			matcher = Matcher::parse(*qhelper.query);
		} catch (std::exception& e) {
			throw http::error400(e.what());
		}

		// Get the dataset here so in case of error we generate a
		// proper error reply before we send the good headers
		auto_ptr<ReadonlyDataset> ds = req.get_dataset(dsname);

		qhelper.do_processing(*ds, req, matcher, dsname);

		// End of streaming
	}


	virtual void operator()(Request& req)
	{
		string dsname;
		if (!pop_first_path.match(req.path_info))
			throw http::error404();

		dsname = pop_first_path[1];
		string rest = pop_first_path[3];

		string action;
		if (!pop_first_path.match(rest))
			action = "index";
		else
			action = pop_first_path[1];

		if (action == "index")
			do_index(req, dsname);
		else if (action == "queryform")
			do_queryform(req, dsname);
		else if (action == "config")
			do_config(req, dsname);
		else if (action == "summary")
			do_summary(req, dsname);
		else if (action == "query")
			do_query(req, dsname);
		else
			throw wibble::exception::Consistency("Unknown dataset action: \"" + action + "\"");
	}
};

struct ChildServer : public sys::ChildProcess
{
	ostream& log;
	Request& req;

	ChildServer(ostream& log, Request& req) : log(log), req(req) {}

	// Executed in child thread
	virtual int main()
	{
		try {
			while (req.read_request(req.sock))
			{
				// Request line and headers have been read
				// sock now points to the optional message body

				// Dump request
				/*
				cerr << "Method: " << req.method << endl;
				cerr << "URL: " << req.url << endl;
				cerr << "Version: " << req.version << endl;
				cerr << "Headers:" << endl;
				for (map<string, string>::const_iterator i = req.headers.begin();
						i != req.headers.end(); ++i)
					cout << " " << i->first <<  ": " << i->second << endl;
				*/

				// Handle request

				// Get the path
				size_t pos = req.url.find('?');
				string path;
				if (pos == string::npos)
					path = req.url;
				else
					path = req.url.substr(0, pos);

				// Strip leading /
				pos = path.find_first_not_of('/');
				if (pos == string::npos)
					req.script_name = "index";
				else
				{
					path = path.substr(pos);
					pos = path.find("/");
					if (pos == string::npos)
						req.script_name = path;
					else
					{
						req.script_name = path.substr(0, pos);
						req.path_info = path.substr(pos);
					}
				}

				// Run the handler for this request
				try {
					if (!local_handlers.try_do(req))
						// if (!script_handlers.try_do(req))
						throw http::error404();
				} catch (http::error& e) {
					e.send(req);
				} catch (std::exception& e) {
					http::error httpe(500, "Server error", e.what());
					httpe.send(req);
				}

				// Here there can be some keep-alive bit
				break;
			}

			close(req.sock);

			return 0;
		} catch (std::exception& e) {
			log << log::ERR << str::replace(e.what(), '\n', ' ') << endl;
			return 1;
		}
	}
};

struct HTTP : public net::TCPServer
{
	ostream& log;
	string server_name;
	map<pid_t, ChildServer*> children;

	string arki_config;

	HTTP(ostream& log) : log(log) {}

	void set_server_name(const std::string& server_name)
	{
		this->server_name = server_name;
	}

	virtual void handle_client(int sock,
			const std::string& peer_hostname,
			const std::string& peer_hostaddr,
			const std::string& peer_port)
	{
		log << log::INFO << "Connection from " << peer_hostname << " " << peer_hostaddr << ":" << peer_port << endl;

		if (children.size() > 256)
		{
			log << log::WARN << "Maximum number of connections reached: connection refused" << endl;
			close(sock);
			return;
		}

		// Set some timeout on this socket to avoid getting stuck if
		// clients are stuck
		struct timeval timeout;
		timeout.tv_sec = 300;
		timeout.tv_usec = 0;
		if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(struct timeval)) < 0)
			throw wibble::exception::System("setting SO_RCVTIMEO on socket");
		if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(struct timeval)) < 0)
			throw wibble::exception::System("setting SO_SNDTIMEO on socket");

		Request req;

		// Parse local config file
		runtime::parseConfigFile(req.arki_conf, arki_config);

		// Create an amended configuration with links to remote dataset
		req.arki_conf_remote.merge(req.arki_conf);
		for (ConfigFile::section_iterator i = req.arki_conf_remote.sectionBegin();
				i != req.arki_conf_remote.sectionEnd(); ++i)
		{
			i->second->setValue("path", server_name + "/dataset/" + i->first);
			i->second->setValue("type", "remote");
			i->second->setValue("server", server_name);
		}

		// Fill in the rest of the request parameters
		req.sock = sock;
		req.peer_hostname = peer_hostname;
		req.peer_hostaddr = peer_hostaddr;
		req.peer_port = peer_port;
		req.server_name = server_name;
		req.server_port = port;

		auto_ptr<ChildServer> handler(new ChildServer(log, req));
		pid_t pid = handler->fork();
		children[pid] = handler.release();

		close(sock);
	}

	void run_server()
	{
		stop_signals.push_back(SIGCHLD);

		while (true)
		{
			int sig_num = accept_loop();
			if (sig_num == SIGCHLD)
			{
				// Wait for children
				while (true)
				{
					int status;
					pid_t pid = waitpid(-1, &status, WNOHANG);
					if (pid == 0)
						break;
					else if (pid < 0)
					{
						if (errno == ECHILD)
							break;
						else
							throw wibble::exception::System("checking for childred that exited");
					}
					log << log::INFO << "Child " << pid << " ended" << endl;

					map<pid_t, ChildServer*>::iterator i = children.find(pid);
					if (i != children.end())
					{
						delete i->second;
						children.erase(i);
					}
					log << log::DEBUG << children.size() << " running child processes left." << endl;
				}
			} else {
				break;
			}
		}
	}
};

struct LogFilter : public log::Sender
{
	log::Level minLevel;
	log::Sender* access;
	log::Sender* error;
	vector<log::Sender*> log_components;

	LogFilter() : minLevel(log::INFO), access(0), error(0) {}
    ~LogFilter()
    {
        for (vector<log::Sender*>::iterator i = log_components.begin();
                i != log_components.end(); ++i)
            delete *i;
    }
	virtual void send(log::Level level, const std::string& msg)
	{
		if (level < minLevel) return;
		if (level >= log::ERR)
		{
			if (error) error->send(level, msg);
		} else {
			if (access) access->send(level, msg);
		}
	}
};

struct ServerProcess : public sys::ChildProcess
{
	commandline::Options& opts;
	ostream log;
	HTTP http;
	LogFilter filter;
	log::Streambuf logstream;

	ServerProcess(commandline::Options& opts)
		: opts(opts), log(cerr.rdbuf()), http(log)
	{
		http.arki_config = sys::fs::abspath(opts.next());

		log::Sender* console = new log::OstreamSender(cerr);
		filter.log_components.push_back(console);
		filter.access = console;
		filter.error = console;

		if (opts.quiet->boolValue())
			filter.minLevel = log::WARN;
		if (opts.syslog->boolValue())
		{
			log::Sender* syslog = new log::SyslogSender("arki-server", LOG_PID, LOG_DAEMON);
			filter.log_components.push_back(syslog);
			filter.access = syslog;
			filter.error = syslog;
		}
		if (opts.accesslog->isSet())
		{
			log::Sender* accesslog = new log::FileSender(opts.accesslog->stringValue());
			filter.log_components.push_back(accesslog);
			log::Sender* ts = new log::Timestamper(accesslog);
			filter.log_components.push_back(ts);
			filter.access = ts;
		}
		if (opts.errorlog->isSet())
		{
			log::Sender* errorlog = new log::FileSender(opts.errorlog->stringValue());
			filter.log_components.push_back(errorlog);
			log::Sender* ts = new log::Timestamper(errorlog);
			filter.log_components.push_back(ts);
			filter.error = ts;
		}
		logstream.setSender(&filter);
		log.rdbuf(&logstream);

		const char* host = NULL;
		if (opts.host->isSet())
			host = opts.host->stringValue().c_str();
		const char* port = "8080";
		if (opts.port->isSet())
			port = opts.port->stringValue().c_str();

		http.bind(port, host);

		if (opts.url->isSet())
			http.set_server_name(opts.url->stringValue());
		else
			http.set_server_name("http://" + http.host + ":" + http.port);

		http.listen();
	}

	~ServerProcess()
	{
	}

	virtual int main()
	{
		// Set FD_CLOEXEC so server workers don't get the master socket
		http.set_sock_cloexec();

		// Server main loop
		http.run_server();

		return 0;
	}
};

int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	try {
		if (opts.parse(argc, argv))
			return 0;

		if (!opts.hasNext())
			throw wibble::exception::BadOption("please specify a configuration file");

		runtime::init();

		runtime::readMatcherAliasDatabase();

		local_handlers.add("index", new IndexHandler);
		local_handlers.add("config", new ConfigHandler);
		local_handlers.add("aliases", new AliasesHandler);
		local_handlers.add("qexpand", new QexpandHandler);
		local_handlers.add("dataset", new DatasetHandler);
		local_handlers.add("query", new RootQueryHandler);
		local_handlers.add("summary", new RootSummaryHandler);

		// Configure the server and start listening
		ServerProcess srv(opts);
		srv.log << log::INFO << "Listening on " << srv.http.host << ":" << srv.http.port << " for " << srv.http.server_name << endl;

		if (opts.runtest->isSet())
		{
			// Fork and start the server
			srv.fork();

			// No need to poll the server until ready, as the
			// socket was already listening since before forking
			int res = system(opts.runtest->stringValue().c_str());

			srv.kill(SIGINT);
			srv.wait();
			return res;
		} else {
			return srv.main();
		}
	} catch (wibble::exception::BadOption& e) {
		cerr << e.desc() << endl;
		opts.outputHelp(cerr);
		return 1;
	} catch (std::exception& e) {
		cerr << e.what() << endl;
		return 1;
	}
}

// vim:set ts=4 sw=4:
