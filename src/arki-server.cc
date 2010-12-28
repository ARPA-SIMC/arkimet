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
#include <arki/dataset/http/server.h>
#include <arki/dataset/http/inbound.h>
#include <arki/dataset/merged.h>
#include <arki/dataset/file.h>
#include <arki/summary.h>
#include <arki/matcher.h>
#include <arki/formatter.h>
#include <arki/dispatcher.h>
#include <arki/report.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/fd.h>
#include <arki/nag.h>
#include <arki/runtime.h>
#include <arki/runtime/config.h>
#include <wibble/net/server.h>
#include <wibble/net/http.h>
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
#include <unistd.h>

#include "config.h"

#define SERVER_SOFTWARE PACKAGE_NAME "/" PACKAGE_VERSION

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace wibble;
using namespace wibble::net;

extern char** environ;

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
    StringOption* inbound;
    BoolOption* syslog;
    BoolOption* quiet;
    BoolOption* verbose;
    BoolOption* debug;

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
        inbound = add<StringOption>("inbound", 0, "inbound", "dir",
                "directory where files to import are found."
                " If specified, it enables a way of triggering uploads"
                " remotely: files found in the inbound directory can be"
                " remotely scanned or imported");
        accesslog = add<StringOption>("accesslog", 0, "accesslog", "file",
                "file where to log normal access information");
        errorlog = add<StringOption>("errorlog", 0, "errorlog", "file",
                "file where to log errors");
        syslog = add<BoolOption>("syslog", 0, "syslog", "",
            "log to system log");
        quiet = add<BoolOption>("quiet", 0, "quiet", "",
            "do not log to standard output");
        verbose = add<BoolOption>("verbose", 0, "verbose", "", "verbose output");
        debug = add<BoolOption>("debug", 0, "debug", "", "debug output");
    }
};

}
}

struct Request : public net::http::Request
{
    ostream& log;
    ConfigFile arki_conf;
    ConfigFile arki_conf_remote;

    Request(ostream& log) : log(log) {}

    const ConfigFile& get_config_remote(const std::string& dsname)
    {
        const ConfigFile* cfg = arki_conf_remote.section(dsname);
        if (cfg == NULL)
            throw net::http::error404();
        return *cfg;
    }

    const ConfigFile& get_config(const std::string& dsname)
    {
        const ConfigFile* cfg = arki_conf.section(dsname);
        if (cfg == NULL)
            throw net::http::error404();
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

    void log_action(const std::string& action)
    {
        sys::process::setproctitle("arki-server worker process: " + action);
        log << log::INFO << action << endl;
    }
};


// Interface for local request handlers
struct LocalHandler
{
    virtual ~LocalHandler() {}
    virtual void operator()(Request& req) = 0;
};

// Map a path component to handlers
struct HandlerMap : public LocalHandler
{
    std::map<string, LocalHandler*> handlers;

    virtual ~HandlerMap()
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
        string head = req.path_info_head();
        std::map<string, LocalHandler*>::iterator i = handlers.find(head);
        if (i == handlers.end())
            return false;
        req.pop_path_info();
        (*(i->second))(req);
        return true;
    }

    // Dispatch the request or throw error404 if not found
    void operator()(Request& req)
    {
        string head = req.pop_path_info();
        std::map<string, LocalHandler*>::iterator i = handlers.find(head);
        if (i == handlers.end())
            throw net::http::error404();
        (*(i->second))(req);
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

HandlerMap local_handlers;
//ScriptHandlers script_handlers;

/// Show a list of all available datasets
struct IndexHandler : public LocalHandler
{
    virtual void operator()(Request& req)
    {
        req.log_action("main index page");

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
        req.log_action("global configuration");

        // if "json" in args:
        //  return server.configdict
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
        req.log_action("alias file contents");

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
        req.log_action("expand aliases in query");

        using namespace wibble::net::http;
        Params params;
        ParamSingle* query = params.add<ParamSingle>("query");
        params.parse_get_or_post(req);
        Matcher m = Matcher::parse(*query);
        string out = m.toStringExpanded();
        req.send_result(out, "text/plain");
    }
};

struct RootQueryHandler : public LocalHandler
{
    virtual void operator()(Request& req)
    {
        req.log_action("root-level query");

        using namespace wibble::net::http;

        // Work in a temporary directory
        utils::MoveToTempDir tempdir("/tmp/arki-server.XXXXXX");

        // Query parameters
        dataset::http::LegacyQueryParams params(tempdir.tmp_dir);

        // Plus a qmacro parameter
        ParamSingle* qmacro;
        qmacro = params.add<ParamSingle>("qmacro");
        // Parse the parameters
        params.parse_get_or_post(req);

        // Build the qmacro dataset
        string macroname = str::trim(*qmacro);
        if (macroname.empty())
            throw error400("root-level query without qmacro parameter");
        auto_ptr<ReadonlyDataset> ds = runtime::make_qmacro_dataset(
                req.arki_conf, macroname, *params.query);

        // params.query contains the qmacro query body; we need to clear the
        // query so do_query gets an empty match expression
        params.query->clear();

        // Serve the result
        dataset::http::ReadonlyDatasetServer srv(*ds, macroname);
        srv.do_query(params, req);
    }
};

struct RootSummaryHandler : public LocalHandler
{
    virtual void operator()(Request& req)
    {
        req.log_action("root-level summary");

        using namespace wibble::net::http;
        Params params;
        ParamSingle* style = params.add<ParamSingle>("style");
        ParamSingle* query = params.add<ParamSingle>("query");
        ParamSingle* qmacro = params.add<ParamSingle>("qmacro");
        params.parse_get_or_post(req);

        string macroname = str::trim(*qmacro);

        auto_ptr<ReadonlyDataset> ds;
        if (macroname.empty())
            // Create a merge dataset with all we have
            ds.reset(new dataset::AutoMerged(req.arki_conf));
        else
            // Create qmacro dataset
            ds = runtime::make_qmacro_dataset(
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
    DatasetHandler()
    {
    }

    // Show the summary of a dataset
    void do_index(ReadonlyDataset& ds, const std::string& dsname, Request& req)
    {
        req.log_action("index of dataset " + dsname);

        // Query the summary
        Summary sum;
        ds.querySummary(Matcher(), sum);

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
        try {
            Report rep("dsinfo");
            rep.captureOutput(res);
            rep(sum);
            rep.report();
        } catch (std::exception& e) {
            sum.writeYaml(res);
        }
        res << "</pre>" << endl;
        res << "</body>" << endl;
        res << "</html>" << endl;

        req.send_result(res.str());
    }

    // Show a form to query the dataset
    void do_queryform(const std::string& dsname, Request& req)
    {
        req.log_action("query form for dataset " + dsname);

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

    virtual void operator()(Request& req)
    {
        string dsname = req.pop_path_info();
        if (dsname.empty())
            throw net::http::error404();

        string action = req.pop_path_info();
        if (action.empty())
            action = "index";

        auto_ptr<ReadonlyDataset> ds = req.get_dataset(dsname);

        if (action == "index")
            do_index(*ds, dsname, req);
        else if (action == "queryform")
            do_queryform(dsname, req);
        else if (action == "config")
        {
            req.log_action("configuration for dataset " + dsname);
            dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
            srv.do_config(req.get_config_remote(dsname), req);
        }
        else if (action == "summary")
        {
            req.log_action("summary for dataset " + dsname);
            dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
            dataset::http::LegacySummaryParams params;
            params.parse_get_or_post(req);
            srv.do_summary(params, req);
        }
        else if (action == "query")
        {
            req.log_action("query dataset " + dsname);
            dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
            utils::MoveToTempDir tempdir("/tmp/arki-server.XXXXXX");
            dataset::http::LegacyQueryParams params(tempdir.tmp_dir);
            params.parse_get_or_post(req);
            srv.do_query(params, req);
        }
        else if (action == "querydata")
        {
            req.log_action("querydata in dataset " + dsname);
            dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
            dataset::http::QueryDataParams params;
            params.parse_get_or_post(req);
            srv.do_queryData(params, req);
        }
        else if (action == "querysummary")
        {
            req.log_action("querysummary in dataset " + dsname);
            dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
            dataset::http::QuerySummaryParams params;
            params.parse_get_or_post(req);
            srv.do_querySummary(params, req);
        }
        else if (action == "querybytes")
        {
            req.log_action("querybytes in dataset " + dsname);
            dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
            utils::MoveToTempDir tempdir("/tmp/arki-server.XXXXXX");
            dataset::http::QueryBytesParams params(tempdir.tmp_dir);
            params.parse_get_or_post(req);
            srv.do_queryBytes(params, req);
        }
        else
            throw wibble::exception::Consistency("Unknown dataset action: \"" + action + "\"");
    }
};

void list_files(const std::string& root, const std::string& path, vector<string>& files)
{
    string absdir = str::joinpath(root, path);
    sys::fs::Directory dir(absdir);
    for (sys::fs::Directory::const_iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if ((*i)[0] == '.') continue;
        if (dir.isdir(i))
            list_files(root, str::joinpath(path, *i), files);
        else
            files.push_back(str::joinpath(path, *i));
    }
}

/// Inbound directory management
struct InboundHandler : public LocalHandler
{
    string dir;

    InboundHandler(const std::string& dir)
        : dir(dir)
    {
    }

    virtual void operator()(Request& req)
    {
        string action = req.pop_path_info();
        if (action.empty())
        {
            req.log_action("inbound index");
            vector<string> files;
            list_files(dir, "", files);
            stringstream res;
            res << "<html><body>" << endl;
            ConfigFile importcfg;
            dataset::http::InboundServer::make_import_config(req, req.arki_conf, importcfg);
            bool can_import = importcfg.sectionSize() > 0;
            res << "<p>Remote import is ";
            if (!can_import)
            {
                res << "<b>not allowed</b></p>";
            } else {
                res << "allowed in:";
                for (ConfigFile::const_section_iterator i = importcfg.sectionBegin();
                        i != importcfg.sectionEnd(); ++i)
                {
                    if (i != importcfg.sectionBegin()) res << ", ";
                    res << "<b>" << i->first << "</b>";
                }
                res << ".</p>";
            }
            res << "Files in inbound directory:" << endl;
            res << "<table>" << endl;
            res << "<tr><th>Name</th><th>Format</th><th>Actions</th></tr>" << endl;
            for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
            {
                // Detect file type
                ConfigFile cfg;
                dataset::File::readConfig(str::joinpath(dir, *i), cfg);
                const ConfigFile *info = cfg.sectionBegin()->second;
                string format = info->value("format");
                // Show file info and actions
                res << "<tr>";
                res << "<td>" << *i << "</td>";
                if (format == "arkimet")
                {
                    res << "<td colspan='2'>(undetected)</td>";
                } else {
                    res << "<td>" << format << "</td>";
                    res << "<td>";
                    string escaped = str::urlencode(*i);
                    res << "<a href='/inbound/show?file=" << escaped << "'>[show]</a>";
                    if (can_import)
                        res << " <a href='/inbound/simulate?file=" << escaped << "'>[simulate import]</a>";
                    res << "</td>";
                }
                res << "</tr>";
            }
            res << "</table>" << endl;
            res << "</body></html>" << endl;
            req.send_result(res.str());
        }
        else if (action == "show")
        {
            req.log_action("show contents of file from inbound");
            using namespace wibble::net::http;

            // Get the file argument
            Params params;
            ParamSingle* file = params.add<ParamSingle>("file");
            params.parse_get_or_post(req);

            // Open it as a dataset
            ConfigFile cfg;
            dataset::File::readConfig(str::joinpath(dir, *file), cfg);
            const ConfigFile *info = cfg.sectionBegin()->second;
            auto_ptr<ReadonlyDataset> ds(dataset::File::create(*info));

            stringstream res;
            res << "<html><body>" << endl;
            res << "Contents of " << *file << ":" << endl;
            res << "<pre>" << endl;

            struct Printer : public metadata::Consumer
            {
                ostream& str;
                Formatter* f;

                Printer(ostream& str) : str(str), f(Formatter::create()) { }
                ~Printer() { delete f; }

                virtual bool operator()(Metadata& md)
                {
                    md.writeYaml(str, f);
                    str << endl;
                    return true;
                }
            } printer(res);
            ds->queryData(dataset::DataQuery(Matcher::parse("")), printer);

            res << "</pre>" << endl;
            res << "</body></html>" << endl;
            req.send_result(res.str());
        }
        else if (action == "simulate")
        {
            req.log_action("simulate import from inbound");
            // Filter configuration to only keep those that have remote import = yes
            // and whose "restrict import" matches
            using namespace wibble::net::http;

            ConfigFile importcfg;
            dataset::http::InboundServer::make_import_config(req, req.arki_conf, importcfg);
            bool can_import = importcfg.sectionSize() > 0;
            if (!can_import)
                throw error400("import is not allowed");

            // Get the file argument
            Params params;
            ParamSingle* file = params.add<ParamSingle>("file");
            params.parse_get_or_post(req);

            // Open it as a dataset
            ConfigFile cfg;
            dataset::File::readConfig(str::joinpath(dir, *file), cfg);
            const ConfigFile *info = cfg.sectionBegin()->second;
            auto_ptr<ReadonlyDataset> ds(dataset::File::create(*info));

            stringstream res;

            struct Simulator : public metadata::Consumer
            {
                TestDispatcher td;
                ostream& str;
                Formatter* f;

                Simulator(const ConfigFile& cfg, ostream& str)
                    : td(cfg, str), str(str), f(Formatter::create()) { }
                ~Simulator() { delete f; }

                virtual bool operator()(Metadata& md)
                {
                    str << "<dt><pre>" << endl;
                    md.writeYaml(str, f);
                    str << "</pre></dt><dd><pre>" << endl;
                    metadata::Collection mdc;
                    Dispatcher::Outcome res = td.dispatch(md, mdc);
                    str << "</pre>" << endl;
                    switch (res)
                    {
                        case Dispatcher::DISP_OK: str << "<b>Imported ok</b>"; break;
                        case Dispatcher::DISP_DUPLICATE_ERROR: str << "<b>Imported as duplicate</b>"; break;
                        case Dispatcher::DISP_ERROR: str << "<b>Imported as error</b>"; break;
                        case Dispatcher::DISP_NOTWRITTEN: str << "<b>Not imported anywhere: do not delete the original</b>"; break;
                        default: str << "<b>Unknown outcome</b>"; break;
                    }
                    str << "</dd>" << endl;
                    return true;
                }
            } simulator(importcfg, res);

            res << "<html><body>" << endl;
            res << "Simulation of import of " << *file << ":" << endl;
            res << "<dl>" << endl;

            ds->queryData(dataset::DataQuery(Matcher::parse("")), simulator);

            res << "</dl>" << endl;
            res << "</body></html>" << endl;
            req.send_result(res.str());
        }
        else if (action == "list")
        {
            req.log_action("list files in inbound");
            vector<string> files;
            list_files(dir, "", files);

            stringstream out;
            for (vector<string>::const_iterator i = files.begin();
                    i != files.end(); ++i)
                out << *i << endl;

            req.send_result(out.str(), "text/plain", "list.txt");
        }
        else if (action == "scan")
        {
            req.log_action("scan file in inbound");
            // Scan file and output binary metadata
            dataset::http::InboundParams params;
            params.parse_get_or_post(req);

            // Empty import config file, as it is not used in do_scan
            ConfigFile import_config;
            dataset::http::InboundServer srv(import_config, dir);
            srv.do_scan(params, req);
        }
        else if (action == "testdispatch")
        {
            req.log_action("test dispatch from inbound");
            // Scan file and output binary metadata
            dataset::http::InboundParams params;
            params.parse_get_or_post(req);

            // Empty import config file, as it is not used in do_scan
            ConfigFile import_config;
            dataset::http::InboundServer::make_import_config(req, req.arki_conf, import_config);
            dataset::http::InboundServer srv(import_config, dir);
            srv.do_testdispatch(params, req);
        }
        else if (action == "dispatch")
        {
            req.log_action("dispatch from inbound");
            // Scan file and output binary metadata
            dataset::http::InboundParams params;
            params.parse_get_or_post(req);

            // Empty import config file, as it is not used in do_scan
            ConfigFile import_config;
            dataset::http::InboundServer::make_import_config(req, req.arki_conf, import_config);
            dataset::http::InboundServer srv(import_config, dir);
            srv.do_dispatch(params, req);
        }
        else
            throw net::http::error404();
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
        sys::process::setproctitle("arki-server worker process: parsing request");
        try {
            while (req.read_request())
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

                // Run the handler for this request
                try {
                    if (!local_handlers.try_do(req))
                        // if (!script_handlers.try_do(req))
                        throw net::http::error404();
                } catch (net::http::error& e) {
                    log << log::WARN << e.what() << endl;
                    if (!req.response_started)
                        e.send(req);
                } catch (std::exception& e) {
                    log << log::WARN << e.what() << endl;
                    if (!req.response_started)
                    {
                        req.extra_response_headers["Arkimet-Exception"] = e.what();
                        net::http::error httpe(500, "Server error", e.what());
                        httpe.send(req);
                    }
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
        utils::fd::HandleWatch hw("Client socket", sock);
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

        Request req(log);
        req.server_software = SERVER_SOFTWARE;

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
    // Track components purely for memory management purpose
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
        if (level >= log::WARN)
        {
            if (error) error->send(level, msg);
        } else {
            if (access) access->send(level, msg);
        }
    }
};

struct ServerProcess : public sys::ChildProcess
{
    vector<string> restart_argv;
    vector<string> restart_environ;
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
        try {
            // Set FD_CLOEXEC so server workers or restarted servers don't get
            // the master socket
            http.set_sock_cloexec();

            // Server main loop
            http.run_server();
        } catch (std::exception& e) {
            log << log::ERR << str::replace(e.what(), '\n', ' ') << endl;
            restart();
        }

        return 0;
    }

    void restart()
    {
        string argv0 = utils::files::find_executable(restart_argv[0]);
        log << log::ERR << "Restarting server " << argv0 << endl;

        // Build argument and environment lists
        char* argv[restart_argv.size() + 1];
        for (size_t i = 0; i < restart_argv.size(); ++i)
            argv[i] = strdup(restart_argv[i].c_str());
        argv[restart_argv.size()] = 0;

        char* envp[restart_environ.size() + 1];
        for (size_t i = 0; i < restart_environ.size(); ++i)
            envp[i] = strdup(restart_environ[i].c_str());
        envp[restart_environ.size()] = 0;

        // Exec self.
        // The master socket will be closed, the child workers will keep
        // working and we will get SIGCHLD from them
        if (execve(argv0.c_str(), argv, envp) < 0)
            throw wibble::exception::System("reloading server");
    }
};

int main(int argc, const char* argv[])
{
    // Save argv and environ so we can use them to reload the server process
    vector<string> restart_argv;
    vector<string> restart_environ;
    for (int i = 0; i < argc; ++i)
        restart_argv.push_back(argv[i]);
    for (char** e = environ; *e; ++e)
        restart_environ.push_back(*e);

    // Initialise setproctitle hacks
    sys::process::initproctitle(argc, (char**)argv);

    wibble::commandline::Options opts;
    try {
        if (opts.parse(argc, argv))
            return 0;

        nag::init(opts.verbose->isSet(), opts.debug->isSet());

        if (!opts.hasNext())
            throw wibble::exception::BadOption("please specify a configuration file");

        // Build the full path of argv[0]
        restart_argv[0];


        runtime::init();

        runtime::readMatcherAliasDatabase();

        local_handlers.add("", new IndexHandler);
        local_handlers.add("config", new ConfigHandler);
        local_handlers.add("aliases", new AliasesHandler);
        local_handlers.add("qexpand", new QexpandHandler);
        local_handlers.add("dataset", new DatasetHandler);
        local_handlers.add("query", new RootQueryHandler);
        local_handlers.add("summary", new RootSummaryHandler);
        if (opts.inbound->isSet())
            local_handlers.add("inbound", new InboundHandler(opts.inbound->stringValue()));

        // Configure the server and start listening
        ServerProcess srv(opts);
        srv.restart_argv = restart_argv;
        srv.restart_environ = restart_environ;
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
