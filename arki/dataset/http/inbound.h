#ifndef ARKI_DATASET_HTTP_INBOUND_H
#define ARKI_DATASET_HTTP_INBOUND_H

/// Server-side remote inbound HTTP server

#include <arki/dataset.h>
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

/// Parameters used by the legacy /summary/ interface
struct InboundParams : public arki::utils::net::http::Params
{
    InboundParams();

    // Legacy params
    arki::utils::net::http::ParamSingle* file;
    arki::utils::net::http::ParamSingle* format;
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
    void do_scan(const InboundParams& parms, arki::utils::net::http::Request& req);

    /// Server side implementation of testdispatch of preuploaded file
    void do_testdispatch(const InboundParams& parms, arki::utils::net::http::Request& req);

    /// Server side implementation of dispatch of preuploaded file
    void do_dispatch(const InboundParams& parms, arki::utils::net::http::Request& req);

    /// Fill \a dst with those datasets from \a src that permit remote importing
    static void make_import_config(const arki::utils::net::http::Request& req, const ConfigFile& src, ConfigFile& dst);
};

}
}
}
#endif
