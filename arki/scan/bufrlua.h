#ifndef ARKI_SCAN_BUFRLUA_H
#define ARKI_SCAN_BUFRLUA_H

/*
 * scan/bufrlua - Use Lua macros for scanning contents of BUFR messages
 *
 * Copyright (C) 2010  ARPAE-SIMC <simc-urp@arpae.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

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

// vim:set ts=4 sw=4:
#endif
