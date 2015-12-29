#ifndef ARKI_DATASET_HTTP_SERVER_H
#define ARKI_DATASET_HTTP_SERVER_H

/// Server-side remote HTTP dataset access

#include <arki/dataset.h>
#include <arki/metadata/consumer.h>
#include <arki/utils/net/http.h>
#include <string>

namespace arki {
namespace utils {
namespace net {
namespace http {
struct Request;
}
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

struct StreamHeaders
{
    std::string content_type;
    std::string ext;
    std::string fname;
    arki::utils::net::http::Request& req;
    bool fired;

    StreamHeaders(arki::utils::net::http::Request& req, const std::string& fname);

    void send_headers();
    void send_result(const std::vector<uint8_t>& res);
    void sendIfNotFired();
};

/// Parameters used by the legacy /summary/ interface
struct LegacySummaryParams : public arki::utils::net::http::Params
{
    LegacySummaryParams();

    // Legacy params
    arki::utils::net::http::ParamSingle* query;
    arki::utils::net::http::ParamSingle* style;
};

/// Parameters used by the legacy /query/ interface
struct LegacyQueryParams : public LegacySummaryParams
{
    LegacyQueryParams(const std::string& tmpdir);

    // Legacy params
    arki::utils::net::http::ParamSingle* command;
    arki::utils::net::http::FileParamMulti* postprocfile;
    arki::utils::net::http::ParamSingle* sort;

    /// Initialise a ProcessorMaker with the parsed query params
    void set_into(runtime::ProcessorMaker& pmaker) const;
};

/// Parameters used by the /querysummary/ interface
struct QuerySummaryParams : public arki::utils::net::http::Params
{
    // DataQuery params
    arki::utils::net::http::ParamSingle* matcher;

    QuerySummaryParams();
};

/// Parameters used by the /querydata/ interface
struct QueryDataParams : public QuerySummaryParams
{
    // DataQuery params
    arki::utils::net::http::ParamSingle* withdata;
    arki::utils::net::http::ParamSingle* sorter;

    QueryDataParams();

    /// Initialise a DataQuery with the parsed query params
    void set_into(DataQuery& dq) const;
};

struct QueryBytesParams : public QueryDataParams
{
    arki::utils::net::http::ParamSingle* type;
    arki::utils::net::http::ParamSingle* param;
    arki::utils::net::http::FileParamMulti* postprocfile;

    QueryBytesParams(const std::string& tmpdir);

    /// Initialise a ByteQuery with the parsed query params
    void set_into(ByteQuery& dq) const;
};

/**
 * Server-side endpoint of the Reader interface exported via HTTP
 */
struct ReaderServer
{
    Reader& ds;
    std::string dsname;

    ReaderServer(
            arki::dataset::Reader& ds,
            const std::string& dsname)
        : ds(ds), dsname(dsname)
    {
    }

    // Return the dataset configuration
    void do_config(const ConfigFile& remote_config, arki::utils::net::http::Request& req);

    // Generate a dataset summary
    void do_summary(const LegacySummaryParams& parms, arki::utils::net::http::Request& req);

    // Download the results of querying a dataset
    void do_query(const LegacyQueryParams& parms, arki::utils::net::http::Request& req);

    /// Server side implementation of queryData
    void do_queryData(const QueryDataParams& parms, arki::utils::net::http::Request& req);

    /// Server side implementation of querySummary
    void do_querySummary(const QuerySummaryParams& parms, arki::utils::net::http::Request& req);

    /// Server side implementation of queryBytes
    void do_queryBytes(const QueryBytesParams& parms, arki::utils::net::http::Request& req);
};

}
}
}
#endif
