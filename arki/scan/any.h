#ifndef ARKI_SCAN_ANY_H
#define ARKI_SCAN_ANY_H

/*
 * scan/any - Scan files autodetecting the format
 *
 * Copyright (C) 2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <string>

namespace arki {
class MetadataConsumer;

namespace scan {

/**
 * Scan the given file, sending its metadata to a consumer.
 *
 * If `filename`.metadata exists and its timestamp is the same of the file or
 * newer, it will be used instead of the file.
 *
 * @return true if the file has been scanned, false if the file is in a format
 * that is not supported or recognised.
 */
bool scan(const std::string& file, MetadataConsumer& c);

}
}

// vim:set ts=4 sw=4:
#endif
