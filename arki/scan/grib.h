#ifndef ARKI_SCAN_GRIB_H
#define ARKI_SCAN_GRIB_H

/// scan/grib - Scan a GRIB (version 1 or 2) file for metadata

#include <arki/scan/base.h>
#include <string>
#include <vector>

struct grib_context;
struct grib_handle;
struct lua_State;

namespace arki {
class Metadata;

namespace scan {
struct Validator;

namespace grib {
const Validator& validator();
}

class Grib;
struct GribLua;

class Grib : public Scanner
{
protected:
    FILE* in;
    grib_context* context;
    grib_handle* gh;
    GribLua* L;
    std::vector<int> grib1_funcs;
    std::vector<int> grib2_funcs;

	/**
	 * Set the source information in the metadata
	 */
	virtual void setSource(Metadata& md);

	/**
	 * Run Lua scanning functions on \a md
	 */
	bool scanLua(std::vector<int> ids, Metadata& md);

	/**
	 * Read GRIB1 data from the currently open handle into md
	 */
	void scanGrib1(Metadata& md);

	/**
	 * Read GRIB2 data from the currently open handle into md
	 */
	void scanGrib2(Metadata& md);

	static int arkilua_lookup_grib(lua_State* L);
	static int arkilua_lookup_gribl(lua_State* L);
	static int arkilua_lookup_gribs(lua_State* L);
	static int arkilua_lookup_gribd(lua_State* L);

public:
	Grib(const std::string& grib1code = std::string(), const std::string& grib2code = std::string());
	virtual ~Grib();

    void open(const std::string& filename, const std::string& basedir, const std::string& relpath, std::shared_ptr<core::Lock> lock) override;
    void close() override;
    bool next(Metadata& md) override;

    friend class GribLua;
};

}
}
#endif
