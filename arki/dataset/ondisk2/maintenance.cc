/*
 * dataset/ondisk2/maintenance - Ondisk2 dataset maintenance
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/dataset/ondisk2/maintenance.h>
#include <arki/dataset/ondisk2/writer.h>
#include <arki/dataset/ondisk2/index.h>
#include <arki/dataset/archive.h>
#include <arki/summary.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/metadata.h>
#include <arki/scan/any.h>
#include <arki/nag.h>

#include <wibble/sys/fs.h>

#include <ostream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <ctime>
#include <cstdio>

// Be compatible with systems without O_NOATIME
#ifndef O_NOATIME
#  define O_NOATIME 0
#endif

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace ondisk2 {
namespace writer {


}
}
}
}
// vim:set ts=4 sw=4:
