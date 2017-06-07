/*
 * utils/accounting - Manage counters to analyse arkimet behaviour
 *
 * Copyright (C) 2010--2011  ARPAE-SIMC <simc-urp@arpae.it>
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

#include "config.h"

#include <arki/utils/accounting.h>

using namespace std;

namespace arki {
namespace utils {
namespace acct {

Counter plain_data_read_count("Plain data read count");
Counter gzip_data_read_count("Gzip data read count");
Counter gzip_forward_seek_bytes("Gzip forward seek bytes");
Counter gzip_idx_reposition_count("Gzip index reposition count");

}
}
}
// vim:set ts=4 sw=4:
