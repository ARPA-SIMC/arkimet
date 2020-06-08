#ifndef ARKI_DATASET_HTTP_H
#define ARKI_DATASET_HTTP_H

/// Remote HTTP dataset access

#include <arki/dataset.h>
#include <arki/dataset/impl.h>
#include <arki/core/curl.h>
#include <string>
#include <sstream>

namespace arki {
class Metadata;
class Matcher;

namespace dataset {

namespace http {

struct Dataset : public dataset::Dataset
{
    std::string baseurl;
    std::string qmacro;

    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::shared_ptr<dataset::Reader> create_reader() override;
};


/**
 * Remote HTTP access to a Dataset.
 *
 * This dataset implements all the dataset query functions through HTTP
 * requests to an Arkimet web server.
 *
 * The dataset is read only: remote import of new data is not supported.
 */
class Reader : public DatasetAccess<Dataset, dataset::Reader>
{
protected:
    core::curl::CurlEasy m_curl;
    void set_post_query(core::curl::Request& request, const std::string& query);
    void set_post_query(core::curl::Request& request, const dataset::DataQuery& q);

    bool impl_query_data(const dataset::DataQuery& q, metadata_dest_func) override;
    void impl_query_summary(const Matcher& matcher, Summary& summary) override;
    void impl_fd_query_bytes(const dataset::ByteQuery& q, core::NamedFileDescriptor& out) override;
    void impl_abstract_query_bytes(const dataset::ByteQuery& q, core::AbstractOutputFile& out) override;

public:
    using DatasetAccess::DatasetAccess;

    std::string type() const override;

    core::Interval get_stored_time_interval() override;

    static std::shared_ptr<core::cfg::Sections> load_cfg_sections(const std::string& path);
    static std::shared_ptr<core::cfg::Section> load_cfg_section(const std::string& path);

    /**
     * Expand the given matcher expression using the aliases on this server
     */
    static std::string expandMatcher(const std::string& matcher, const std::string& server);
};

class HTTPInbound
{
    std::string m_baseurl;
    mutable core::curl::CurlEasy m_curl;

public:
    HTTPInbound(const std::string& baseurl);
    ~HTTPInbound();

    /// Scan a previously uploaded file
    void list(std::vector<std::string>& files);

    /// Scan a previously uploaded file
    void scan(const std::string& fname, const std::string& format, metadata_dest_func dest);

    /// Run a testdispatch on a previously uploaded file
    void testdispatch(const std::string& fname, const std::string& format, core::NamedFileDescriptor& out);

    /// Run a dispatch on a previously uploaded file
    void dispatch(const std::string& fname, const std::string& format, metadata_dest_func consumer);
};

}
}
}
#endif
