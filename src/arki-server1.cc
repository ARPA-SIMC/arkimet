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
#include <wibble/sys/childprocess.h>
#include <wibble/sys/fs.h>
#include <arki/configfile.h>
#include <arki/dataset.h>
#include <arki/summary.h>
#include <arki/matcher.h>
#if 0
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/formatter.h>
#include <arki/utils/geosdef.h>
#endif
#include <arki/runtime.h>
#include <arki/utils/server.h>
#include <arki/utils/http.h>
#include <arki/utils/lua.h>

#include <string>
#include <vector>
#include <map>
#include <sstream>
#include <fstream>
#include <iostream>
#include <cctype>
#include <cerrno>
#include <cstdlib>

#include <sys/types.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/socket.h>

#include "config.h"

using namespace std;
using namespace arki;
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
		quiet = add<BoolOption>("quiet", 0, "quiet", "",
			"do not log to standard output");
	}
};

}
}

struct Request : public utils::http::Request
{
	ConfigFile arki_conf;
	ConfigFile arki_conf_remote;
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

/**
 * Parse and store HTTP query parameters
 *
 * It can be preconfigured witn 
 */
struct Params : public std::map<std::string, Param*> 
{
	/// Maximum size of POST input data
	size_t conf_max_input_size;

	/**
	 * Whether to accept unknown fields.
	 *
	 * If true, unkown fields are stored as MultiParam or FileParam as
	 * appropriate.
	 *
	 * If false, unknown fields are ignored.
	 */
	bool conf_accept_unknown_fields;

	Params()
	{
		conf_max_input_size = 20 * 1024 * 1024;
		conf_accept_unknown_fields = false;
	}
	~Params()
	{
		for (iterator i = begin(); i != end(); ++i)
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

	void parse_get_or_post(utils::http::Request& req)
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

	void parse_post(utils::http::Request& req)
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
			; // Main (inputsize, defs.args)
		}
		else
			throw wibble::exception::Consistency("unsupported Content-Type: " + i->second);
	}
};

struct error404
{
	std::string msg;

	error404() {}
	error404(const std::string& msg) : msg(msg) {}

	void send(Request& req)
	{
		stringstream body;
		body << "<html>" << endl;
		body << "<head>" << endl;
		body << "  <title>Not found</title>" << endl;
		body << "</head>" << endl;
		body << "<body>" << endl;
		if (msg.empty())
			body << "<p>Resource " << req.script_name << " not found.</p>" << endl;
		else
			body << "<p>" + msg + "</p>" << endl;
		body << "</body>" << endl;
		body << "</html>" << endl;

		req.send_status_line(404, "Not found");
		req.send_date_header();
		req.send_server_header();
		req.send("Content-Type: text/html; charset=utf-8\r\n");
		req.send(str::fmtf("Content-Length: %d\r\n", body.str().size()));
		req.send("\r\n");
		req.send(body.str());
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

LocalHandlers local_handlers;
ScriptHandlers script_handlers;

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

#if 0
// Download the summary of a dataset
@route("/summary")
@route("/summary/")
@post("/summary")
@post("/summary/")
def summary():
    args = Args()
    if not "qmacro" in args:
        raise bottle.HTTPError(400, "Root-level query withouth qmacro argument")
    return ArkiQuerySummary(owndir = True, fields=args).stream()
#endif

// Dispatch dataset-specific actions
struct DatasetHandler : public LocalHandler
{
	wibble::ERegexp pop_first_path;

	DatasetHandler()
		: pop_first_path("^/*([^/]+)(/+(.+))?", 4)
	{
	}

	const ConfigFile& get_config_remote(Request& req, const std::string& dsname)
	{
		const ConfigFile* cfg = req.arki_conf_remote.section(dsname);
		if (cfg == NULL)
			throw error404();
		return *cfg;
	}

	const ConfigFile& get_config(Request& req, const std::string& dsname)
	{
		const ConfigFile* cfg = req.arki_conf.section(dsname);
		if (cfg == NULL)
			throw error404();
		return *cfg;
	}

	auto_ptr<ReadonlyDataset> get_dataset(Request& req, const std::string& dsname)
	{
		return get_dataset(get_config(req, dsname));
	}

	auto_ptr<ReadonlyDataset> get_dataset(const ConfigFile& cfg)
	{
		return auto_ptr<ReadonlyDataset>(ReadonlyDataset::create(cfg));
	}

	// Show the summary of a dataset
	void do_index(Request& req, const std::string& dsname)
	{
		auto_ptr<ReadonlyDataset> ds = get_dataset(req, dsname);

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
		get_config_remote(req, dsname).output(res, "(memory)");
		req.send_result(res.str(), "text/plain");
	}

	virtual void operator()(Request& req)
	{
		string dsname;
		if (!pop_first_path.match(req.path_info))
			throw error404();

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
		else
			throw wibble::exception::Consistency("Unknown dataset action: \"" + action + "\"");
	}
};

/*
""" % name


@route("/dataset/:name/summary")
@route("/dataset/:name/summary/")
@post("/dataset/:name/summary")
@post("/dataset/:name/summary/")
def dataset_summary(name):
    """
    Download the summary of a dataset
    """
    conf = dsconfig.get(name, None)
    if conf is None:
        raise bottle.HTTPError(code=404, output='dataset %s not found' % name)
    return ArkiQuerySummary(dsconf=conf, fields=Args()).stream()

@route("/dataset/:name/query")
@route("/dataset/:name/query/")
@post("/dataset/:name/query")
@post("/dataset/:name/query/")
def dataset_query(name):
    """
    Download the summary of a dataset
    """
    conf = dsconfig.get(name, None)
    if conf is None:
        raise bottle.HTTPError(code=404, output='dataset %s not found' % name)
    return ArkiQuery(owndir = True, dsconf=conf, fields=Args()).stream()
*/

struct ChildServer : public sys::ChildProcess
{
	Request& req;

	ChildServer(Request& req) : req(req) {}

	// Executed in child thread
	virtual int main()
	{
		try {
			while (req.read_request(req.sock))
			{
				// Request line and headers have been read
				// sock now points to the optional message body

				// Dump request
				cerr << "Method: " << req.method << endl;
				cerr << "URL: " << req.url << endl;
				cerr << "Version: " << req.version << endl;
				cerr << "Headers:" << endl;
				for (map<string, string>::const_iterator i = req.headers.begin();
						i != req.headers.end(); ++i)
					cout << " " << i->first <<  ": " << i->second << endl;


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
						if (!script_handlers.try_do(req))
							throw error404();
				} catch (error404& e) {
					e.send(req);
				}

				// Here there can be some keep-alive bit
				break;
			}

			close(req.sock);

			return 0;
		} catch (std::exception& e) {
			cerr << e.what() << endl;
			return 1;
		}
	}
};

struct HTTP : public utils::net::TCPServer
{
	string server_name;
	map<pid_t, ChildServer*> children;

	string arki_config;

	void set_server_name(const std::string& server_name)
	{
		this->server_name = server_name;
	}

	virtual void handle_client(int sock,
			const std::string& peer_hostname,
			const std::string& peer_hostaddr,
			const std::string& peer_port)
	{
		cerr << "Connection from " << peer_hostname << " " << peer_hostaddr << ":" << peer_port << endl;

		if (children.size() > 256)
		{
			cerr << "Maximum number of connections reached" << endl;
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

		auto_ptr<ChildServer> handler(new ChildServer(req));
		pid_t pid = handler->fork();
		children[pid] = handler.release();

		close(sock);
	}

	void run_server()
	{
		listen();
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
					cout << "Child " << pid << " ended" << endl;

					map<pid_t, ChildServer*>::iterator i = children.find(pid);
					if (i != children.end())
					{
						delete i->second;
						children.erase(i);
					}
					cout << children.size() << " running child processes left." << endl;
				}
			} else {
				break;
			}
		}
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

		local_handlers.add("index", new IndexHandler);
		local_handlers.add("config", new ConfigHandler);
		local_handlers.add("aliases", new AliasesHandler);
		local_handlers.add("qexpand", new QexpandHandler);
		local_handlers.add("dataset", new DatasetHandler);

		/*
		runtest = add<StringOption>("runtest", 0, "runtest", "cmd",
				"start the server, run the given test command"
				" and return its exit status");
		accesslog = add<StringOption>("accesslog", 0, "accesslog", "file",
				"file where to log normal access information");
		errorlog = add<StringOption>("errorlog", 0, "errorlog", "file",
				"file where to log errors");
		quiet = add<BoolOption>("quiet", 0, "quiet", "",
			"do not log to standard output");
		*/

		HTTP http;
		http.arki_config = opts.next();

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

		cout << "Listening on " << http.host << ":" << http.port << " for " << http.server_name << endl;

		http.run_server();

		cout << "Done." << endl;

		return 0;
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
