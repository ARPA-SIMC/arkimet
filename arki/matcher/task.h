
#ifndef ARKI_MATCHER_TASK
#define ARKI_MATCHER_TASK

/*
 * matcher/task - Task matcher
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * Author: Guido Billi <guidobilli@gmail.com>
 */

#include <arki/matcher.h>
#include <arki/types/task.h>

namespace arki {
namespace matcher {

/*============================================================================*/

/**
 * Match tasks
 */
struct MatchTask : public Implementation
{
    std::string name() const override;

    std::string task;

    MatchTask(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;

    static MatchTask* parse(const std::string& pattern);
    static void init();
};

/*============================================================================*/

}
}

#endif
