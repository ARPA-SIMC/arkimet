#include "config.h"
#include "arki/dataset/http.h"
#include "arki/dataset/session.h"
#include "arki/dataset/progress.h"
#include "arki/dataset/query.h"
#include "arki/dataset/querymacro.h"
#include "arki/core/file.h"
#include "arki/metadata.h"
#include "arki/metadata/stream.h"
#include "arki/metadata/sort.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/core/binary.h"
#include "arki/nag.h"
#include <cstdlib>
#include <sstream>

using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace dataset {

namespace http {


Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : dataset::Dataset(session, cfg),
      baseurl(cfg.value("path")),
      qmacro(cfg.value("qmacro"))
{
}

std::shared_ptr<dataset::Reader> Dataset::create_reader()
{
    return std::make_shared<Reader>(std::static_pointer_cast<Dataset>(shared_from_this()));
}

std::string Reader::type() const { return "http"; }

struct OstreamState : public core::curl::Request
{
    NamedFileDescriptor& out;
    std::function<void(NamedFileDescriptor&)> data_start_hook;
    std::shared_ptr<dataset::QueryProgress> progress;

    OstreamState(core::curl::CurlEasy& curl, NamedFileDescriptor& out, std::function<void(NamedFileDescriptor&)> data_start_hook=0)
        : Request(curl), out(out), data_start_hook(data_start_hook)
    {
    }

    void perform() override
    {
        if (progress) progress->start();
        Request::perform();
    }

    size_t process_body_chunk(void *ptr, size_t size, size_t nmemb, void *stream) override
    {
        if (data_start_hook && size > 0)
        {
            data_start_hook(out);
            data_start_hook = nullptr;
        }
        size_t res = out.write((const char*)ptr, size * nmemb);
        if (progress) progress->update(0, size * nmemb);
        return res;
    }
};

struct AbstractOutputState : public core::curl::Request
{
    AbstractOutputFile& out;
    std::shared_ptr<dataset::QueryProgress> progress;

    AbstractOutputState(core::curl::CurlEasy& curl, AbstractOutputFile& out)
        : Request(curl), out(out)
    {
    }

    void perform() override
    {
        if (progress) progress->start();
        Request::perform();
    }

    size_t process_body_chunk(void *ptr, size_t size, size_t nmemb, void *stream) override
    {
        out.write((const char*)ptr, size * nmemb);
        if (progress) progress->update(0, size * nmemb);
        return size * nmemb;
    }
};

struct MDStreamState : public core::curl::Request
{
    metadata::Stream mdc;

    MDStreamState(core::curl::CurlEasy& curl, metadata_dest_func dest, const std::string& baseurl)
        : Request(curl), mdc(dest, baseurl)
    {
    }

    size_t process_body_chunk(void *ptr, size_t size, size_t nmemb, void *stream) override
    {
        mdc.readData(ptr, size * nmemb);
        return size * nmemb;
    }
};

void Reader::set_post_query(core::curl::Request& request, const std::string& query)
{
    if (dataset().qmacro.empty())
        request.post_data.add_string("query", query);
    else
    {
        request.post_data.add_string("query", dataset().qmacro);
        request.post_data.add_string("qmacro", name());
    }
}

void Reader::set_post_query(core::curl::Request& request, const dataset::DataQuery& q)
{
    set_post_query(request, q.matcher.toStringExpanded());
    if (q.sorter)
        request.post_data.add_string("sort", q.sorter->toString());
}

bool Reader::impl_query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    dataset::TrackProgress track(q.progress);
    dest = track.wrap(dest);

    m_curl.reset();

    MDStreamState request(m_curl, dest, dataset().baseurl);
    request.set_url(str::joinpath(dataset().baseurl, "query"));
    request.set_method("POST");
    set_post_query(request, q);
    if (q.with_data)
        request.post_data.add_string("style", "inline");
    request.perform();

    return track.done(!request.mdc.consumer_canceled());
}

void Reader::impl_query_summary(const Matcher& matcher, Summary& summary)
{
    m_curl.reset();

    core::curl::BufState<std::vector<uint8_t>> request(m_curl);
    request.set_url(str::joinpath(dataset().baseurl, "summary"));
    request.set_method("POST");
    set_post_query(request, matcher.toStringExpanded());
    request.perform();

    BinaryDecoder dec(request.buf);
    summary.read(dec, request.url);
}

void Reader::impl_fd_query_bytes(const dataset::ByteQuery& q, NamedFileDescriptor& out)
{
    m_curl.reset();

    OstreamState request(m_curl, out, q.data_start_hook);
    request.set_url(str::joinpath(dataset().baseurl, "query"));
    request.set_method("POST");
    request.progress = q.progress;
    set_post_query(request, q);

    const char* toupload = getenv("ARKI_POSTPROC_FILES");
    if (toupload != NULL)
    {
        unsigned count = 0;
        // Split by ':'
        str::Split splitter(toupload, ":");
        for (str::Split::const_iterator i = splitter.begin(); i != splitter.end(); ++i)
            request.post_data.add_file("postprocfile" + std::to_string(++count), *i);
    }
    switch (q.type)
    {
        case dataset::ByteQuery::BQ_DATA:
            request.post_data.add_string("style", "data");
            break;
        case dataset::ByteQuery::BQ_POSTPROCESS:
            request.post_data.add_string("style", "postprocess");
            request.post_data.add_string("command", q.param);
            break;
        default: {
            std::stringstream ss;
            ss << "cannot query dataset: unsupported query type: " << (int)q.type;
            throw std::runtime_error(ss.str());
        }
    }
    request.perform();
    if (q.progress) q.progress->done();
}

void Reader::impl_abstract_query_bytes(const dataset::ByteQuery& q, AbstractOutputFile& out)
{
    if (q.data_start_hook)
        throw std::runtime_error("Cannot use data_start_hook on abstract output files");

    m_curl.reset();

    AbstractOutputState request(m_curl, out);
    request.set_url(str::joinpath(dataset().baseurl, "query"));
    request.set_method("POST");
    request.progress = q.progress;
    set_post_query(request, q);

    const char* toupload = getenv("ARKI_POSTPROC_FILES");
    if (toupload != NULL)
    {
        unsigned count = 0;
        // Split by ':'
        str::Split splitter(toupload, ":");
        for (str::Split::const_iterator i = splitter.begin(); i != splitter.end(); ++i)
            request.post_data.add_file("postprocfile" + std::to_string(++count), *i);
    }
    switch (q.type)
    {
        case dataset::ByteQuery::BQ_DATA:
            request.post_data.add_string("style", "data");
            break;
        case dataset::ByteQuery::BQ_POSTPROCESS:
            request.post_data.add_string("style", "postprocess");
            request.post_data.add_string("command", q.param);
            break;
        default: {
            std::stringstream ss;
            ss << "cannot query dataset: unsupported query type: " << (int)q.type;
            throw std::runtime_error(ss.str());
        }
    }
    request.perform();
    if (q.progress) q.progress->done();
}

core::Interval Reader::get_stored_time_interval()
{
    throw std::runtime_error("http::Reader::get_stored_time_interval not yet implemented");
}

std::shared_ptr<core::cfg::Sections> Reader::load_cfg_sections(const std::string& path)
{
    using namespace http;

    core::curl::CurlEasy m_curl;
    m_curl.reset();

    core::curl::BufState<std::string> request(m_curl);
    request.set_url(str::joinpath(path, "config"));
    request.perform();

    auto res = core::cfg::Sections::parse(request.buf, request.url);
    // Make sure name=* is present in each section
    for (auto& si: *res)
        si.second->set("name", si.first);
    return res;
}

std::shared_ptr<core::cfg::Section> Reader::load_cfg_section(const std::string& path)
{
    using namespace http;

    core::curl::CurlEasy m_curl;
    m_curl.reset();

    core::curl::BufState<std::string> request(m_curl);
    request.set_url(str::joinpath(path, "config"));
    request.perform();

    auto sections = core::cfg::Sections::parse(request.buf, request.url);
    if (sections->size() != 1)
        throw std::runtime_error(request.url + ": only 1 section expected in resulting configuration, found " + std::to_string(sections->size()));

    auto res = sections->begin()->second;
    res->set("name", sections->begin()->first);
    return res;
}

std::string Reader::expandMatcher(const std::string& matcher, const std::string& server)
{
    using namespace http;

    core::curl::CurlEasy m_curl;
    m_curl.reset();

    core::curl::BufState<std::string> request(m_curl);
    request.set_url(str::joinpath(server, "qexpand"));
    request.set_method("POST");
    request.post_data.add_string("query", matcher);
    request.perform();

    return str::strip(request.buf);
}


HTTPInbound::HTTPInbound(const std::string& baseurl)
    : m_baseurl(baseurl)
{
}

HTTPInbound::~HTTPInbound()
{
}

void HTTPInbound::list(std::vector<std::string>& files)
{
    m_curl.reset();

    core::curl::BufState<std::string> request(m_curl);
    request.set_url(str::joinpath(m_baseurl, "inbound/list"));
    request.perform();

    // Parse the results
    str::Split splitter(request.buf, "\n");
    for (str::Split::const_iterator i = splitter.begin(); i != splitter.end(); ++i)
        files.push_back(*i);
}

void HTTPInbound::scan(const std::string& fname, const std::string& format, metadata_dest_func dest)
{
    m_curl.reset();

    MDStreamState request(m_curl, dest, m_baseurl);
    request.set_url(str::joinpath(m_baseurl, "inbound/scan"));
    request.set_method("POST");
    request.post_data.add_string("file", fname);
    if (!format.empty())
        request.post_data.add_string("format", format);
    request.perform();
}

void HTTPInbound::testdispatch(const std::string& fname, const std::string& format, NamedFileDescriptor& out)
{
    m_curl.reset();

    OstreamState request(m_curl, out);
    request.set_url(str::joinpath(m_baseurl, "inbound/testdispatch"));
    request.set_method("POST");
    request.post_data.add_string("file", fname);
    if (!format.empty())
        request.post_data.add_string("format", format);
    request.perform();
}

void HTTPInbound::dispatch(const std::string& fname, const std::string& format, metadata_dest_func consumer)
{
    m_curl.reset();

    MDStreamState request(m_curl, consumer, m_baseurl);
    request.set_url(str::joinpath(m_baseurl, "inbound/dispatch"));
    request.set_method("POST");
    request.post_data.add_string("file", fname);
    if (!format.empty())
        request.post_data.add_string("format", format);
    request.perform();
}

}
}
}
