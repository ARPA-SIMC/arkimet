#ifndef ARKI_QUERYMACRO_H
#define ARKI_QUERYMACRO_H

/// Macros implementing special query strategies

#include <arki/utils/lua.h>
#include <arki/dataset.h>
#include <string>
#include <map>

namespace arki {
class ConfigFile;
class Metadata;

namespace dataset {
class Reader;
}

class Querymacro : public dataset::Reader
{
protected:
	std::map<std::string, Reader*> ds_cache;
	// std::map<std::string, int> ref_cache;

public:
	const ConfigFile& cfg;
	Lua *L;
	int funcid_querydata;
	int funcid_querysummary;

	/**
	 * Create a query macro read from the query macro file with the given
	 * name.
	 *
	 * @param cfg
	 *   Configuration used to instantiate datasets
	 */
	Querymacro(const ConfigFile& cfg, const std::string& name, const std::string& query);
	virtual ~Querymacro();

	/**
	 * Get a dataset from the querymacro dataset cache, instantiating it in
	 * the cache if it is not already there
	 */
	Reader* dataset(const std::string& name);

    void query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
};

}
#endif
