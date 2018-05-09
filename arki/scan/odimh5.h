#ifndef ARKI_SCAN_ODIMH5_H
#define ARKI_SCAN_ODIMH5_H

#include <arki/scan.h>
#include <arki/utils/h5.h>
#include <string>
#include <vector>

struct lua_State;

namespace arki {
class Metadata;

namespace scan {
struct Validator;

namespace odimh5 {
const Validator& validator();
}

struct OdimH5Lua;

class OdimH5 : public Scanner
{
public:
	OdimH5();
	virtual ~OdimH5();

    void scan_file(const std::string& filename, Metadata& md);

    std::unique_ptr<Metadata> scan_data(const std::vector<uint8_t>& data) override;
    bool scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest) override;
    bool scan_file(const std::string& abspath, std::shared_ptr<segment::Reader> reader, metadata_dest_func dest) override;
    bool scan_file_inline(const std::string& abspath, metadata_dest_func dest) override;

protected:
    hid_t h5file;
    std::vector<int> odimh5_funcs;
    OdimH5Lua* L;

    void set_inline_source(Metadata& md, const std::string& abspath);
    void set_blob_source(Metadata& md, std::shared_ptr<segment::Reader> reader);

    /**
     * Run Lua scanning functions on \a md
     */
    bool scanLua(Metadata& md);

    static int arkilua_find_attr(lua_State* L);
    static int arkilua_get_groups(lua_State* L);

    friend class OdimH5Lua;
};

}
}

#endif
