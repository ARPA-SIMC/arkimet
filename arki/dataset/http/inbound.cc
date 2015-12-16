#include <arki/dataset/http/inbound.h>
#include <arki/dataset/http/server.h>
#include <arki/dataset/file.h>
#include <arki/dispatcher.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/metadata/collection.h>
#include <arki/configfile.h>
#include <arki/utils/fd.h>
#include <arki/utils/string.h>
#include <arki/utils/sys.h>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace http {

InboundParams::InboundParams()
{
    using namespace net::http;

    file = add<ParamSingle>("file");
    format = add<ParamSingle>("format");
}

InboundServer::InboundServer(const ConfigFile& import_config, const std::string& root)
    : import_config(import_config), root(root)
{
}

void InboundServer::do_scan(const InboundParams& parms, net::http::Request& req)
{
    // Build the full file name
    string fname = str::joinpath(root, *parms.file);

    // Build a dataset configuration for it
    ConfigFile cfg;
    dataset::File::readConfig(fname, cfg);
    ConfigFile *info = cfg.sectionBegin()->second;

    // Override format if requested
    if (!parms.format->empty())
        info->setValue("format", *parms.format);

    // Build a dataset to scan the file
    unique_ptr<Reader> ds(dataset::File::create(*info));

    // Response header generator
    StreamHeaders headers(req, str::basename(*parms.file));
    headers.ext = "arkimet";

    ds->query_data(Matcher(), [&](unique_ptr<Metadata> md) {
        headers.sendIfNotFired();
        md->write(headers.req.sock, "socket");
        return true;
    });

    // If we had empty output, headers were not sent: catch up
    headers.sendIfNotFired();
}

void InboundServer::do_testdispatch(const InboundParams& parms, net::http::Request& req)
{
    using namespace net::http;

    bool can_import = import_config.sectionSize() > 0;
    if (!can_import)
        throw error400("import is not allowed");

    // Build the full file name
    string fname = str::joinpath(root, *parms.file);

    // Build a dataset configuration for it
    ConfigFile cfg;
    dataset::File::readConfig(fname, cfg);
    ConfigFile *info = cfg.sectionBegin()->second;

    // Override format if requested
    if (!parms.format->empty())
        info->setValue("format", *parms.format);

    // Build a dataset to scan the file
    unique_ptr<Reader> ds(dataset::File::create(*info));

    stringstream str;
    TestDispatcher td(import_config, str);

    ds->query_data(Matcher(), [&](unique_ptr<Metadata> md) {
        /*Dispatcher::Outcome res =*/ td.dispatch(move(md), [](unique_ptr<Metadata>) { return true; });
        /*
        switch (res)
        {
            case Dispatcher::DISP_OK: str << "<b>Imported ok</b>"; break;
            case Dispatcher::DISP_DUPLICATE_ERROR: str << "<b>Imported as duplicate</b>"; break;
            case Dispatcher::DISP_ERROR: str << "<b>Imported as error</b>"; break;
            case Dispatcher::DISP_NOTWRITTEN: str << "<b>Not imported anywhere: do not delete the original</b>"; break;
            default: str << "<b>Unknown outcome</b>"; break;
        }
        */
        return true;
    });

    req.send_result(str.str(), "text/plain", str::basename(*parms.file) + ".log");
}

void InboundServer::do_dispatch(const InboundParams& parms, net::http::Request& req)
{
    using namespace net::http;

    bool can_import = import_config.sectionSize() > 0;
    if (!can_import)
        throw error400("import is not allowed");

    // Build the full file name
    string fname = str::joinpath(root, *parms.file);

    // Build a dataset configuration for it
    ConfigFile cfg;
    dataset::File::readConfig(fname, cfg);
    ConfigFile *info = cfg.sectionBegin()->second;

    // Override format if requested
    if (!parms.format->empty())
        info->setValue("format", *parms.format);

    // Build a dataset to scan the file
    unique_ptr<Reader> ds(dataset::File::create(*info));

    // Response header generator
    StreamHeaders headers(req, str::basename(*parms.file));
    headers.ext = "arkimet";

    RealDispatcher d(import_config);
    bool all_ok = true;
    ds->query_data(Matcher(), [&](unique_ptr<Metadata> md) {
        Dispatcher::Outcome res = d.dispatch(move(md), [&](unique_ptr<Metadata> md) {
            headers.sendIfNotFired();
            md->write(headers.req.sock, "socket");
            return true;
        });
        switch (res)
        {
            case Dispatcher::DISP_OK:
            case Dispatcher::DISP_DUPLICATE_ERROR:
                break;
            case Dispatcher::DISP_ERROR:
            case Dispatcher::DISP_NOTWRITTEN:
            default:
                all_ok = false;
                break;
        }
        return true;
    });

    // Delete fname if all was ok
    if (all_ok)
        sys::unlink(fname);

    // If we had empty output, headers were not sent: catch up
    headers.sendIfNotFired();
}

static bool can_import(const net::http::Request& req, const ConfigFile& cfg)
{
    // Need "remote import = yes"
    if (!ConfigFile::boolValue(cfg.value("remote import")))
        return false;

    // TODO: check that "restrict import" matches
    return true;
}

void InboundServer::make_import_config(const net::http::Request& req, const ConfigFile& src, ConfigFile& dst)
{
    // Check that the 'error' dataset is importable
    bool has_error = false;
    for (ConfigFile::const_section_iterator i = src.sectionBegin();
            i != src.sectionEnd(); ++i)
    {
        if (i->first == "error")
        {
            has_error = can_import(req, *i->second);
            break;
        }
    }

    // If no 'error' is importable, give up now without touching dst
    if (!has_error) return;

    // Copy all importable datasets to dst
    for (ConfigFile::const_section_iterator i = src.sectionBegin();
            i != src.sectionEnd(); ++i)
        if (can_import(req, *i->second))
            dst.mergeInto(i->first, *i->second);
}

}
}
}
// vim:set ts=4 sw=4:
