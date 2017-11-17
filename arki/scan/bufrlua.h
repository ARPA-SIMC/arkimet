#ifndef ARKI_SCAN_BUFRLUA_H
#define ARKI_SCAN_BUFRLUA_H

#include <arki/utils/lua.h>
#include <dballe/message.h>
#include <dballe/msg/msg.h>
#include <map>
#include <string>

namespace arki {
class Metadata;

namespace scan {
namespace bufr {

class BufrLua : protected Lua
{
	std::map<dballe::MsgType, int> scan_funcs;

	/**
	 * Get (loading it if needed) the scan function ID for the given
	 * message type
	 */
	int get_scan_func(dballe::MsgType type);

public:
	BufrLua();
	~BufrLua();

	/**
	 * Best effort scanning of message contents
	 *
	 * Scanning is best effort in the sense that if anything goes wrong
	 * (for example there is no scanning function, or something odd is
	 * found in the message), scanning stops.
	 */
	void scan(dballe::Message& msg, Metadata& md);
};

}
}
}
#endif
