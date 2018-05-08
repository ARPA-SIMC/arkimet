#ifndef ARKI_SCAN_GRIB_H
#define ARKI_SCAN_GRIB_H

/// scan/grib - Scan a GRIB (version 1 or 2) file for metadata

#include <arki/scan.h>
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
    std::string filename;
    std::shared_ptr<segment::Reader> reader;

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

    void scan_handle(Metadata& md);
    bool next(Metadata& md);
    void open(const std::string& filename, std::shared_ptr<segment::Reader> reader);
    void close();

public:
    Grib(const std::string& grib1code=std::string(), const std::string& grib2code=std::string());
    virtual ~Grib();

    std::unique_ptr<Metadata> scan_data(const std::vector<uint8_t>& data) override;
    bool scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest) override;
    bool scan_file(const std::string& abspath, std::shared_ptr<segment::Reader> reader, metadata_dest_func dest) override;
    bool scan_file_inline(const std::string& abspath, metadata_dest_func dest) override;

    friend class GribLua;
};

}
}
#endif
