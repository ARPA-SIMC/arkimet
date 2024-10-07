#ifndef ARKI_DATASET_SESSION_H
#define ARKI_DATASET_SESSION_H

#include <arki/segment/fwd.h>
#include <arki/core/fwd.h>
#include <arki/dataset/fwd.h>
#include <arki/matcher/parser.h>
#include <unordered_map>
#include <filesystem>
#include <string>

namespace arki {
namespace dataset {

class Session: public std::enable_shared_from_this<Session>
{
protected:
    /// Map segment absolute paths to possibly reusable reader instances
    // TODO: use std::filesystem::path on newer GCC
    std::unordered_map<std::string, std::weak_ptr<segment::Reader>> reader_pool;

    matcher::Parser matcher_parser;

public:
    explicit Session(bool load_aliases=true);
    Session(const Session&) = delete;
    Session(Session&&) = delete;
    virtual ~Session();
    Session& operator=(const Session&) = delete;
    Session& operator=(Session&&) = delete;

    virtual std::shared_ptr<segment::Reader> segment_reader(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, std::shared_ptr<core::Lock> lock);
    virtual std::shared_ptr<segment::Writer> segment_writer(const segment::WriterConfig& config, const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath);
    virtual std::shared_ptr<segment::Checker> segment_checker(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath);

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

    /// Compile a matcher expression
    Matcher matcher(const std::string& expr);

    /**
     * Expand the given query on the local session and all remote servers.
     *
     * If the results are consistent, return the expanded query. Else, raises
     * std::runtime_error.
     */
    std::string expand_remote_query(std::shared_ptr<const core::cfg::Sections> remotes, const std::string& query);

    /**
     * Return the current alias database
     */
    std::shared_ptr<core::cfg::Sections> get_alias_database() const;

    /**
     * Add the given set of aliases to the alias database in this session
     */
    void load_aliases(const core::cfg::Sections& aliases);

    /// Load aliases from the given remote server
    void load_remote_aliases(const std::string& url);

    /**
     * Read the configuration of the dataset at the given path or URL
     */
    static std::shared_ptr<core::cfg::Section> read_config(const std::filesystem::path& path);

    /**
     * Read a multi-dataset configuration at the given path or URL
     */
    static std::shared_ptr<core::cfg::Sections> read_configs(const std::filesystem::path& path);
};


class DirSegmentsSession : public Session
{
public:
    using Session::Session;

    std::shared_ptr<segment::Reader> segment_reader(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, std::shared_ptr<core::Lock> lock) override;
    std::shared_ptr<segment::Writer> segment_writer(const segment::WriterConfig& config, const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath) override;
    std::shared_ptr<segment::Checker> segment_checker(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath) override;

};

}
}
#endif
