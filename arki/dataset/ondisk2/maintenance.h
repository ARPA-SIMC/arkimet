#ifndef ARKI_DATASET_ONDISK2_MAINTENANCE_H
#define ARKI_DATASET_ONDISK2_MAINTENANCE_H

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

#include <arki/dataset/maintenance.h>
#include <wibble/sys/buffer.h>
#include <iosfwd>

namespace arki {
class MetadataConsumer;

namespace scan {
struct Validator;
}

namespace dataset {
class WritableLocal;

namespace maintenance {
class MaintFileVisitor;
}

namespace ondisk2 {
class WIndex;
class Index;

namespace writer {

}

}
}
}

// vim:set ts=4 sw=4:
#endif
