#ifndef ARKI_DATASET_SESSION_H
#define ARKI_DATASET_SESSION_H

#include <arki/segment/fwd.h>
#include <arki/core/fwd.h>
#include <arki/dataset/fwd.h>
#include <arki/matcher/parser.h>
#include <arki/segment/session.h>
#include <unordered_map>
#include <filesystem>
#include <string>

namespace arki::dataset {

class Session: public virtual segment::Session
{
protected:
    matcher::Parser matcher_parser;

public:
    using segment::Session::Session;
    explicit Session(bool load_aliases=true);
    virtual ~Session();

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


struct DirSegmentsSession: public virtual Session, public virtual segment::DirSegmentsMixin
{
    using dataset::Session::Session;
};

}
#endif
