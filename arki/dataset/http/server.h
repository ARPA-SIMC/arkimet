#ifndef ARKI_DATASET_HTTP_SERVER_H
#define ARKI_DATASET_HTTP_SERVER_H

/*
 * dataset/http/server - Server-side remote HTTP dataset access
 *
 * Copyright (C) 2010--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/metadata/consumer.h>
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

struct StreamHeaders : public metadata::Hook
{
    std::string content_type;
    std::string ext;
    std::string fname;
    wibble::net::http::Request& req;
    bool fired;

    StreamHeaders(wibble::net::http::Request& req, const std::string& fname);

    virtual void operator()();

    void send_result(const std::string& res);
    void sendIfNotFired();
};

struct MetadataStreamer : public metadata::Eater
{
    StreamHeaders& sh;

    MetadataStreamer(StreamHeaders& sh);

    bool eat(std::unique_ptr<Metadata>&& md) override;
};


/// Parameters used by the legacy /summary/ interface
struct LegacySummaryParams : public wibble::net::http::Params
{
    LegacySummaryParams();

    // Legacy params
    wibble::net::http::ParamSingle* query;
    wibble::net::http::ParamSingle* style;
};

/// Parameters used by the legacy /query/ interface
struct LegacyQueryParams : public LegacySummaryParams
{
    LegacyQueryParams(const std::string& tmpdir);

    // Legacy params
    wibble::net::http::ParamSingle* command;
    wibble::net::http::FileParamMulti* postprocfile;
    wibble::net::http::ParamSingle* sort;

    /// Initialise a ProcessorMaker with the parsed query params
    void set_into(runtime::ProcessorMaker& pmaker) const;
};

/// Parameters used by the /querysummary/ interface
struct QuerySummaryParams : public wibble::net::http::Params
{
    // DataQuery params
    wibble::net::http::ParamSingle* matcher;

    QuerySummaryParams();
};

/// Parameters used by the /querydata/ interface
struct QueryDataParams : public QuerySummaryParams
{
    // DataQuery params
    wibble::net::http::ParamSingle* withdata;
    wibble::net::http::ParamSingle* sorter;

    QueryDataParams();

    /// Initialise a DataQuery with the parsed query params
    void set_into(DataQuery& dq) const;
};

struct QueryBytesParams : public QueryDataParams
{
    wibble::net::http::ParamSingle* type;
    wibble::net::http::ParamSingle* param;
    wibble::net::http::FileParamMulti* postprocfile;

    QueryBytesParams(const std::string& tmpdir);

    /// Initialise a ByteQuery with the parsed query params
    void set_into(ByteQuery& dq) const;
};

/**
 * Server-side endpoint of the ReadonlyDataset interface exported via HTTP
 */
struct ReadonlyDatasetServer
{
    ReadonlyDataset& ds;
    std::string dsname;

    ReadonlyDatasetServer(
            arki::ReadonlyDataset& ds,
            const std::string& dsname)
        : ds(ds), dsname(dsname)
    {
    }

    // Return the dataset configuration
    void do_config(const ConfigFile& remote_config, wibble::net::http::Request& req);

    // Generate a dataset summary
    void do_summary(const LegacySummaryParams& parms, wibble::net::http::Request& req);

    // Download the results of querying a dataset
    void do_query(const LegacyQueryParams& parms, wibble::net::http::Request& req);

    /// Server side implementation of queryData
    void do_queryData(const QueryDataParams& parms, wibble::net::http::Request& req);

    /// Server side implementation of querySummary
    void do_querySummary(const QuerySummaryParams& parms, wibble::net::http::Request& req);

    /// Server side implementation of queryBytes
    void do_queryBytes(const QueryBytesParams& parms, wibble::net::http::Request& req);
};

}
}
}

// vim:set ts=4 sw=4:
#endif
