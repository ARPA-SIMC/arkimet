#ifndef ARKI_QUERYMACRO_H
#define ARKI_QUERYMACRO_H

/// Macros implementing special query strategies

#include <arki/dataset.h>
#include <arki/core/cfg.h>
#include <string>
#include <map>

namespace arki {
class Metadata;
class Lua;

namespace dataset {
class Reader;
}

class Querymacro : public dataset::Reader
{
protected:
    std::shared_ptr<const dataset::Config> m_config;
    std::map<std::string, Reader*> ds_cache;

public:
    core::cfg::Sections dispatch_cfg;
    Lua *L = nullptr;
    int funcid_querydata;
    int funcid_querysummary;

    /**
     * Create a query macro read from the query macro file with the given
     * name.
     *
     * @param cfg
     *   Configuration used to instantiate datasets
     */
    Querymacro(const core::cfg::Section& ds_cfg, const core::cfg::Sections& dispatch_cfg, const std::string& name, const std::string& query);
    virtual ~Querymacro();

    const dataset::Config& config() const override { return *m_config; }
    std::string type() const override;

	/**
	 * Get a dataset from the querymacro dataset cache, instantiating it in
	 * the cache if it is not already there
	 */
	Reader* dataset(const std::string& name);

    bool query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
};

}
#endif
