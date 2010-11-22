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
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/configfile.h>
#include <arki/utils/fd.h>

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
}

void InboundServer::do_dispatch(const InboundParams& parms, wibble::net::http::Request& req)
{
}

}
}
}
// vim:set ts=4 sw=4:
