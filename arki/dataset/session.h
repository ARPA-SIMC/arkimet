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
    /// Pool of known, reusable datasets
    std::unordered_map<std::string, std::shared_ptr<dataset::Dataset>> dataset_pool;
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

    /**
     * Instantiate a dataset and add it to the pool.
     *
     * If a dataset with the same name already exists in the pool, it throws
     * std::runtime_error.
     *
     * If the dataset is remote, the aliases used by the remote server will be
     * added to the session alias database. If different servers define some
     * aliases differently, it throws std::runtime_error
     */
    void add_dataset(const core::cfg::Section& cfg);

    /// Check if the dataset pool has a dataset with the given name
    bool has_dataset(const std::string& name) const;

    /**
     * Instantiate a dataset give its configuration.
     *
     * If the dataset has been previously added to the pool, it will be reused.
     *
     * If the dataset is not present in the pool, it will be created and
     * returned, but *not added* to the pool. To add a dataset to the pool, use
     * ``add_dataset``.
     */
    std::shared_ptr<Dataset> dataset(const core::cfg::Section& cfg);

    /**
     * Return a dataset from the session pool.
     *
     * If the named dataset does not exist in the pool, it throws
     * std::runtime_error
     */
    std::shared_ptr<Dataset> dataset(const std::string& name);

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
     * Add the given set of aliases to the alias database in this session
     */
    void load_aliases(const core::cfg::Sections& aliases);

    /**
     * Look for the dataset in the pool that contains the given metadata item,
     * and make the metadata source relative to it.
     *
     * If no dataset contains the metadata, it returns an empty shared_ptr and
     * leaves \a md untouched.
     */
    std::shared_ptr<dataset::Dataset> locate_metadata(Metadata& md);

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
