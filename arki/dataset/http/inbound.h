#ifndef ARKI_DATASET_HTTP_INBOUND_H
#define ARKI_DATASET_HTTP_INBOUND_H

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

#include <arki/dataset.h>
#include <arki/wibble/net/http.h>
#include <string>

namespace wibble {
namespace net {
namespace http {
struct Request;
}
}
}


namespace arki {
struct ConfigFile;

namespace runtime {
struct ProcessorMaker;
}

namespace dataset {
namespace http {

/// Parameters used by the legacy /summary/ interface
struct InboundParams : public wibble::net::http::Params
{
    InboundParams();

    // Legacy params
    wibble::net::http::ParamSingle* file;
    wibble::net::http::ParamSingle* format;
};

/**
 * Server-side HTTP endpoint for scan/dispatch of preuploaded files
 */
struct InboundServer
{
    const ConfigFile& import_config;
    std::string root;

    InboundServer(const ConfigFile& import_config, const std::string& root);

    /// Server side implementation of scan of preuploaded file
    void do_scan(const InboundParams& parms, wibble::net::http::Request& req);

    /// Server side implementation of testdispatch of preuploaded file
    void do_testdispatch(const InboundParams& parms, wibble::net::http::Request& req);

    /// Server side implementation of dispatch of preuploaded file
    void do_dispatch(const InboundParams& parms, wibble::net::http::Request& req);

    /// Fill \a dst with those datasets from \a src that permit remote importing
    static void make_import_config(const wibble::net::http::Request& req, const ConfigFile& src, ConfigFile& dst);
};

}
}
}

// vim:set ts=4 sw=4:
#endif
