/*
 * dataset/http/inbound - Server-side remote inbound HTTP server
 *
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/http/inbound.h>
#include <arki/dataset/http/server.h>
#include <arki/dataset/file.h>
#include <arki/dispatcher.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/metadata/collection.h>
#include <arki/configfile.h>
#include <arki/utils/fd.h>
#include <arki/utils/files.h>

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace http {

InboundParams::InboundParams()
{
    using namespace wibble::net::http;

    file = add<ParamSingle>("file");
    format = add<ParamSingle>("format");
}

InboundServer::InboundServer(const ConfigFile& import_config, const std::string& root)
    : import_config(import_config), root(root)
{
}

void InboundServer::do_scan(const InboundParams& parms, wibble::net::http::Request& req)
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
    auto_ptr<ReadonlyDataset> ds(dataset::File::create(*info));

    // Response header generator
    StreamHeaders headers(req, str::basename(*parms.file));
    headers.ext = "arkimet";

    MetadataStreamer cons(headers);
    ds->queryData(dataset::DataQuery(Matcher::parse("")), cons);

    // If we had empty output, headers were not sent: catch up
    headers.sendIfNotFired();
}

void InboundServer::do_testdispatch(const InboundParams& parms, wibble::net::http::Request& req)
{
    using namespace wibble::net::http;

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
    auto_ptr<ReadonlyDataset> ds(dataset::File::create(*info));

    struct Simulator : public metadata::Consumer
    {
        TestDispatcher td;
        stringstream str;

        Simulator(const ConfigFile& cfg)
            : td(cfg, str) {}

        virtual bool operator()(Metadata& md)
        {
            metadata::Collection mdc;
            /*Dispatcher::Outcome res =*/ td.dispatch(md, mdc);
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
        }
    } simulator(import_config);

    ds->queryData(dataset::DataQuery(Matcher::parse("")), simulator);

    req.send_result(simulator.str.str(), "text/plain", str::basename(*parms.file) + ".log");
}

void InboundServer::do_dispatch(const InboundParams& parms, wibble::net::http::Request& req)
{
    using namespace wibble::net::http;

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
    auto_ptr<ReadonlyDataset> ds(dataset::File::create(*info));

    // Response header generator
    StreamHeaders headers(req, str::basename(*parms.file));
    headers.ext = "arkimet";

    MetadataStreamer cons(headers);

    struct Worker : public metadata::Consumer
    {
        RealDispatcher d;
        metadata::Consumer& cons;
        bool all_ok;

        Worker(const ConfigFile& cfg, metadata::Consumer& cons)
            : d(cfg), cons(cons), all_ok(true) { }

        virtual bool operator()(Metadata& md)
        {
            Dispatcher::Outcome res = d.dispatch(md, cons);
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
        }
    } worker(import_config, cons);

    ds->queryData(dataset::DataQuery(Matcher::parse("")), worker);

    // Delete fname if all was ok
    if (worker.all_ok)
        utils::files::unlink(fname);

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

void InboundServer::make_import_config(const wibble::net::http::Request& req, const ConfigFile& src, ConfigFile& dst)
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
