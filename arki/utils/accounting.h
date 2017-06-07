#ifndef ARKI_UTILS_ACCOUNTING_H
#define ARKI_UTILS_ACCOUNTING_H

/*
 * utils/accounting - Manage counters to analyse arkimet behaviour
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

#include <stddef.h>

namespace arki {
namespace utils {

/**
 * Simple global counters to keep track of various arkimet functions
 */
namespace acct {

class Counter
{
protected:
	// Use a static const char* (forcing to use of const strings) to keep
	// initialisation simple
	const char* m_name;
	size_t m_val;

public:
	Counter(const char* name) : m_name(name), m_val(0) {}

	/// Increment counter by some amount
	void incr(size_t amount = 1) { m_val += amount; }

	/// Reset counter
	void reset() { m_val = 0; }
		
	/// Get value
	size_t val() const { return m_val; }

	/// Get name
	const char* name() const { return m_name; }
};

extern Counter plain_data_read_count;
extern Counter gzip_data_read_count;
extern Counter gzip_forward_seek_bytes;
extern Counter gzip_idx_reposition_count;

}
}
}

// vim:set ts=4 sw=4:
#endif
