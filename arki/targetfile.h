#ifndef ARKI_TARGETFILE_H
#define ARKI_TARGETFILE_H

#include <arki/utils/lua.h>
#include <arki/dataset.h>
#include <string>
#include <map>

namespace arki {
class Metadata;

namespace runtime {
class Output;
}

class Targetfile
{
protected:
	Lua *L;
	std::map<std::string, int> ref_cache;

public:
	struct Func
	{
		Lua* L;
		int idx;

		Func(const Func& f) : L(f.L), idx(f.idx) {}
		Func(Lua* L, int idx) : L(L), idx(idx) {}

		/**
		 * Compute a target file name for a metadata
		 */
		std::string operator()(Metadata& md);
	};
	
	Targetfile(const std::string& code = std::string());
	virtual ~Targetfile();

	/**
	 * Load definition from the rc files.
	 *
	 * This is called automatically by the costructor if the constructor
	 * code parameter is empty.
	 */
	void loadRCFiles();

	/**
	 * Get a target file generator
	 *
	 * @param def
	 *   Target file definition in the form "type:parms".
	 */
	Func get(const std::string& def);

	/**
	 * Return a global, yet thread-local instance
	 */
	static Targetfile& instance();
};

/**
 * Dynamically open an Output according to what is coming out of a dataset.
 *
 * Wrap a dataset. As query results arrive, use arki::Targetfile to generate a
 * file name for their output, and open/reopen an Output accordingly.
 */
class TargetfileSpy : public ReadonlyDataset
{
	Targetfile::Func func;
	ReadonlyDataset& ds;
	runtime::Output& output;

public:
	TargetfileSpy(ReadonlyDataset& ds, runtime::Output& output, const std::string& def);

    void redirect(Metadata& md);

    virtual void query_data(const dataset::DataQuery& q, metadata_dest_func dest);
    virtual void querySummary(const Matcher& matcher, Summary& summary);
};

}
#endif
