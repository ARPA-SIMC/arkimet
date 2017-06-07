/*
 * types-init - initialise arkimet base type library
 *
 * Copyright (C) 2011  ARPAE-SIMC <simc-urp@arpae.it>
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

#include <arki/types-init.h>
#include <arki/types/origin.h>
#include <arki/matcher/origin.h>
#include <arki/types/product.h>
#include <arki/matcher/product.h>
#include <arki/types/proddef.h>
#include <arki/matcher/proddef.h>
#include <arki/types/level.h>
#include <arki/matcher/level.h>
#include <arki/types/timerange.h>
#include <arki/matcher/timerange.h>
#include <arki/types/area.h>
#include <arki/matcher/area.h>
#include <arki/types/reftime.h>
#include <arki/matcher/reftime.h>
#include <arki/types/run.h>
#include <arki/matcher/run.h>
#include <arki/types/bbox.h>
#include <arki/types/quantity.h>
#include <arki/matcher/quantity.h>
#include <arki/types/task.h>
#include <arki/matcher/task.h>
#include <arki/types/value.h>
#include <arki/matcher/source.h>
#include <arki/types/note.h>
#include <arki/types/source.h>
#include <arki/types/assigneddataset.h>

namespace arki {
namespace types {

void init_default_types()
{
    Origin::init();
    matcher::MatchOrigin::init();
    Product::init();
    matcher::MatchProduct::init();
    Proddef::init();
    matcher::MatchProddef::init();
    Level::init();
    matcher::MatchLevel::init();
    Timerange::init();
    matcher::MatchTimerange::init();
    Area::init();
    matcher::MatchArea::init();
    Reftime::init();
    matcher::MatchReftime::init();
    Run::init();
    matcher::MatchRun::init();
    BBox::init();
    Quantity::init();
    matcher::MatchQuantity::init();
    Task::init();
    matcher::MatchTask::init();
    Value::init();
    matcher::MatchSource::init();
    Note::init();
    Source::init();
    AssignedDataset::init();
}

}
}
