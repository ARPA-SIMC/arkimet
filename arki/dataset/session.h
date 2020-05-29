#ifndef ARKI_DATASET_SESSION_H
#define ARKI_DATASET_SESSION_H

#include <arki/segment/fwd.h>
#include <arki/core/fwd.h>
#include <arki/dataset/fwd.h>
#include <arki/matcher/parser.h>
#include <unordered_map>
#include <string>

namespace arki {
namespace dataset {

class Session: public std::enable_shared_from_this<Session>
{
protected:
    /// Map segment absolute paths to possibly reusable reader instances
    std::unordered_map<std::string, std::weak_ptr<segment::Reader>> reader_pool;
    matcher::Parser matcher_parser;

public:
    Session(bool load_aliases=true);
    Session(const Session&) = delete;
    Session(Session&&) = delete;
    virtual ~Session();
    Session& operator=(const Session&) = delete;
    Session& operator=(Session&&) = delete;

    virtual std::shared_ptr<segment::Reader> segment_reader(const std::string& format, const std::string& root, const std::string& relpath, std::shared_ptr<core::Lock> lock);
    virtual std::shared_ptr<segment::Writer> segment_writer(const std::string& format, const std::string& root, const std::string& relpath);
    virtual std::shared_ptr<segment::Checker> segment_checker(const std::string& format, const std::string& root, const std::string& relpath);

    /// Insantiate a dataset give its configuration
    std::shared_ptr<Dataset> dataset(const core::cfg::Section& cfg);

    /// Compile a matcher expression
    Matcher matcher(const std::string& expr);

    /**
     * Expand the given query on the local session and all remote servers.
     *
     * If the results are consistent, return the expanded query. Else, raises
     * std::runtime_error.
     */
    std::string expand_remote_query(const core::cfg::Sections& remotes, const std::string& query);

    /**
     * Return the current alias database
     */
    core::cfg::Sections get_alias_database() const;

    /**
     * Read the configuration of the dataset at the given path or URL
     */
    static core::cfg::Section read_config(const std::string& path);

    /**
     * Read a multi-dataset configuration at the given path or URL
     */
    static core::cfg::Sections read_configs(const std::string& path);
};

}
}
#endif
