#include <arki/dataset/http/server.h>
#include <arki/configfile.h>
#include <arki/summary.h>
#include <arki/sort.h>
#include <arki/utils.h>
#include <arki/utils/string.h>
#include <arki/runtime.h>
#include <arki/emitter/json.h>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace http {

StreamHeaders::StreamHeaders(net::http::Request& req, const std::string& fname)
    : content_type("application/octet-stream"), ext("bin"), fname(fname),
      req(req), fired(false)
{
}

void StreamHeaders::send_headers()
{
    req.send_status_line(200, "OK");
    req.send_date_header();
    req.send_server_header();
    req.send("Content-Type: " + content_type + "\r\n");
    req.send("Content-Disposition: attachment; filename=" + fname + "." + ext + "\r\n");
    req.send("\r\n");
    fired = true;
}

void StreamHeaders::send_result(const std::vector<uint8_t>& res)
{
    req.send_result(res, content_type, fname + "." + ext);
}

void StreamHeaders::sendIfNotFired()
{
    if (!fired) send_headers();
}


LegacySummaryParams::LegacySummaryParams()
{
    using namespace net::http;

    query = add<ParamSingle>("query");
    style = add<ParamSingle>("style");
    annotate = add<ParamSingle>("annotate");
}

void LegacySummaryParams::set_into(runtime::ProcessorMaker& pmaker) const
{
    using namespace net::http;

    pmaker.server_side = true;

    // Configure the ProcessorMaker with the request
    if (style->empty()) {
        ;
    } else if (*style == "yaml") {
        pmaker.yaml = true;
    } else if (*style == "json") {
        pmaker.json = true;
    }
    if (*annotate == "true") {
        pmaker.annotate = true;
    }

    // Validate request
    string errors = pmaker.verify_option_consistency();
    if (!errors.empty())
        throw net::http::error400(errors);
}


LegacyQueryParams::LegacyQueryParams(const std::string& tmpdir)
{
    using namespace net::http;

    conf_fname_blacklist = ":";
    conf_outdir = tmpdir;

    command = add<ParamSingle>("command");
    postprocfile = add<FileParamMulti>("postprocfile");
    sort = add<ParamSingle>("sort");
}

static void postprocfiles_to_env(net::http::FileParamMulti& postprocfile)
{
    using namespace net::http;

    vector<string> postproc_files;
    for (vector<FileParam::FileInfo>::const_iterator i = postprocfile.files.begin();
            i != postprocfile.files.end(); ++i)
        postproc_files.push_back(i->fname);

    if (!postproc_files.empty())
    {
        // Pass files for the postprocessor in the environment
        string val = str::join(":", postproc_files.begin(), postproc_files.end());
        setenv("ARKI_POSTPROC_FILES", val.c_str(), 1);
    } else
        unsetenv("ARKI_POSTPROC_FILES");
}

void LegacyQueryParams::set_into(runtime::ProcessorMaker& pmaker) const
{
    using namespace net::http;

    pmaker.server_side = true;

    // Configure the ProcessorMaker with the request
    if (style->empty() || *style == "metadata") {
        ;
    } else if (*style == "yaml") {
        pmaker.yaml = true;
    } else if (*style == "json") {
        pmaker.json = true;
    } else if (*style == "inline") {
        pmaker.data_inline = true;
    } else if (*style == "data") {
        pmaker.data_only = true;
    } else if (*style == "postprocess") {
        pmaker.postprocess = *command;
        postprocfiles_to_env(*postprocfile);
    } else if (*style == "rep_metadata") {
        pmaker.report = *command;
    } else if (*style == "rep_summary") {
        pmaker.summary = true;
        pmaker.report = *command;
    }

    if (!sort->empty())
        pmaker.sort = *sort;

    // Validate request
    string errors = pmaker.verify_option_consistency();
    if (!errors.empty())
        throw net::http::error400(errors);
}

QuerySummaryParams::QuerySummaryParams()
{
    using namespace net::http;

    matcher = add<ParamSingle>("matcher");
}

QueryDataParams::QueryDataParams()
{
    using namespace net::http;

    withdata = add<ParamSingle>("withdata");
    sorter = add<ParamSingle>("sorter");
}

void QueryDataParams::set_into(DataQuery& dq) const
{
    dq.matcher = Matcher::parse(*matcher);
    dq.with_data = *withdata == "true";
    if (!sorter->empty())
        dq.sorter = sort::Compare::parse(*sorter);
}

QueryBytesParams::QueryBytesParams(const std::string& tmpdir)
{
    using namespace net::http;

    conf_fname_blacklist = ":";
    conf_outdir = tmpdir;

    type = add<ParamSingle>("type");
    param = add<ParamSingle>("param");
    postprocfile = add<FileParamMulti>("postprocfile");
}

void QueryBytesParams::set_into(ByteQuery& dq) const
{
    QueryDataParams::set_into(dq);
    dq.param = *param;
    if (*type == "data")
        dq.type = ByteQuery::BQ_DATA;
    else if (*type == "postprocess")
        dq.type = ByteQuery::BQ_POSTPROCESS;
    else if (*type == "rep_metadata")
        dq.type = ByteQuery::BQ_REP_METADATA;
    else if (*type == "rep_summary")
        dq.type = ByteQuery::BQ_REP_SUMMARY;
    else
        dq.type = ByteQuery::BQ_DATA;
    postprocfiles_to_env(*postprocfile);
}


void ReaderServer::do_config(const ConfigFile& remote_config, net::http::Request& req)
{
    // args = Args()
    // if "json" in args:
    //    return c

    stringstream res;
    res << "[" << dsname << "]" << endl;
    remote_config.output(res, "(memory)");
    req.send_result(res.str(), "text/plain");
}

void ReaderServer::do_summary(const LegacySummaryParams& parms, net::http::Request& req)
{
    Matcher matcher = Matcher::parse(*parms.query);
    runtime::ProcessorMaker pmaker;
    pmaker.summary = true;
    parms.set_into(pmaker);

    // Response header generator
    StreamHeaders headers(req, dsname + "-summary");

    // Set content type and file name accordingly
    if (pmaker.yaml)
    {
        headers.content_type = "text/x-yaml";
        headers.ext = "yaml";
    }
    else if (pmaker.json)
    {
        headers.content_type = "application/json";
        headers.ext = "json";
    } else if (!pmaker.report.empty()) {
        headers.content_type = "text/plain";
        headers.ext = "txt";
    }

    {
        // Create Output directed to req.sock
        sys::NamedFileDescriptor sockoutput(req.sock, "socket");

        // Hook sending headers to when the subprocess start sending
        pmaker.data_start_hook = [&]{ headers.send_headers(); };

        // Create the dataset processor for this query
        unique_ptr<runtime::DatasetProcessor> p = pmaker.make(matcher, sockoutput);

        // Process the dataset producing the output
        p->process(ds, dsname);
        p->end();
    }

    // End of streaming

}

void ReaderServer::do_summary_short(const LegacySummaryParams& parms, arki::utils::net::http::Request& req)
{
    Matcher matcher = Matcher::parse(*parms.query);
    runtime::ProcessorMaker pmaker;
    pmaker.summary_short = true;
    parms.set_into(pmaker);

    // Response header generator
    StreamHeaders headers(req, dsname + "-summaryshort");

    // Set content type and file name accordingly
    if (pmaker.yaml)
    {
        headers.content_type = "text/x-yaml";
        headers.ext = "yaml";
    }
    else if (pmaker.json)
    {
        headers.content_type = "application/json";
        headers.ext = "json";
    } else if (!pmaker.report.empty()) {
        headers.content_type = "text/plain";
        headers.ext = "txt";
    }

    {
        // Create Output directed to req.sock
        sys::NamedFileDescriptor sockoutput(req.sock, "socket");

        // Hook sending headers to when the subprocess start sending
        pmaker.data_start_hook = [&]{ headers.send_headers(); };

        // Create the dataset processor for this query
        unique_ptr<runtime::DatasetProcessor> p = pmaker.make(matcher, sockoutput);

        // Process the dataset producing the output
        p->process(ds, dsname);
        p->end();
    }

    // End of streaming
}

void ReaderServer::do_query(const LegacyQueryParams& parms, net::http::Request& req)
{
    // Validate query
    Matcher matcher;
    try {
        matcher = Matcher::parse(*parms.query);
    } catch (std::exception& e) {
        req.extra_response_headers["Arkimet-Exception"] = e.what();
        throw net::http::error400(e.what());
    }

    // Configure a ProcessorMaker with the request
    runtime::ProcessorMaker pmaker;
    parms.set_into(pmaker);

    // Response header generator
    StreamHeaders headers(req, dsname);

    // Set content type and file name accordingly
    if (pmaker.yaml)
    {
        headers.content_type = "text/x-yaml";
        headers.ext = "yaml";
    }
    else if (pmaker.json)
    {
        headers.content_type = "application/json";
        headers.ext = "json";
    } else if (!pmaker.report.empty()) {
        headers.content_type = "text/plain";
        headers.ext = "txt";
    }

    // If we are postprocessing, we cannot monitor the postprocessor
    // output to hook sending headers: the postprocessor is
    // connected directly to the output socket.
    //
    // Therefore we need to send the headers in advance.

    {
        // Create Output directed to req.sock
        sys::NamedFileDescriptor sockoutput(req.sock, "socket");

        // Hook sending headers to when the subprocess start sending
        pmaker.data_start_hook = [&]{ headers.send_headers(); };

        // Create the dataset processor for this query
        unique_ptr<runtime::DatasetProcessor> p = pmaker.make(matcher, sockoutput);

        // Process the dataset producing the output
        p->process(ds, dsname);
        p->end();
    }

    // If we had empty output, headers were not sent: catch up
    headers.sendIfNotFired();

    // End of streaming
}

void ReaderServer::do_queryData(const QueryDataParams& parms, net::http::Request& req)
{
    // Response header generator
    StreamHeaders headers(req, dsname);
    headers.ext = "arkimet";
    DataQuery dq;
    parms.set_into(dq);
// TODO: hook here something that makes absolute BLOB sources or Inline sources depending on sq.with_data
    NamedFileDescriptor out(headers.req.sock, "socket");
    ds.query_data(dq, [&](unique_ptr<Metadata> md) {
        headers.sendIfNotFired();
        md->write(out);
        return true;
    });

    // If we had empty output, headers were not sent: catch up
    headers.sendIfNotFired();
}

void ReaderServer::do_querySummary(const QuerySummaryParams& parms, net::http::Request& req)
{
    StreamHeaders headers(req, dsname);
    headers.ext = "summary";
    Summary s;
    ds.query_summary(Matcher::parse(*parms.matcher), s);
    headers.send_result(s.encode());
}

void ReaderServer::do_queryBytes(const QueryBytesParams& parms, net::http::Request& req)
{
    // Response header generator
    StreamHeaders headers(req, dsname);

    ByteQuery bq;
    parms.set_into(bq);
    // Send headers when data starts flowing
    bq.data_start_hook = [&]{ headers.send_headers(); };

    // Pick a nice extension
    switch (bq.type)
    {
        case ByteQuery::BQ_DATA: headers.ext = "bin";
        case ByteQuery::BQ_POSTPROCESS: headers.ext = "bin";
        case ByteQuery::BQ_REP_METADATA: headers.ext = "txt";
        case ByteQuery::BQ_REP_SUMMARY: headers.ext = "txt";
    }

    // Produce the results
    NamedFileDescriptor out(req.sock, "socket");
    ds.query_bytes(bq, out);

    // If we had empty output, headers were not sent: catch up
    headers.sendIfNotFired();
}

}
}
}
