#ifndef ARKI_SCAN_ODIMH5_H
#define ARKI_SCAN_ODIMH5_H

#include <arki/scan/base.h>
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

    /**
     * Access a file with ODIMH5 data  - alternate version with explicit
     * basedir/relname separation
     */
    void open(const std::string& filename, const std::string& basedir, const std::string& relname, const core::lock::Policy* lock_policy) override;

    /**
     * Close the input file.
     *
     * This is optional: the file will be closed by the destructor if needed.
     */
    void close() override;

	/**
	 * Scan the next ODIMH5 in the file.
	 *
	 * @returns
	 *   true if it found a ODIMH5 message,
	 *   false if there are no more ODIMH5 messages in the file
	 */
	bool next(Metadata& md);

protected:
    hid_t h5file;
    bool read;
    std::vector<int> odimh5_funcs;
    OdimH5Lua* L;

	/**
	 * Set the source information in the metadata
	 */
	virtual void setSource(Metadata& md);

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
