
/*
 * matcher/task - Task matcher
 *
 * Copyright (C) 2007,2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/matcher/task.h>
#include <arki/matcher/utils.h>
#include <arki/metadata.h>

#include <wibble/string.h>

using namespace std;
using namespace wibble;

namespace arki { namespace matcher {

/*============================================================================*/

std::string MatchTask::name() const { return "task"; }

MatchTask::MatchTask(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	task = wibble::str::toupper(args.getString(0, ""));
}

bool MatchTask::matchItem(const Item<>& o) const
{
	const types::Task* v = dynamic_cast<const types::Task*>(o.ptr());
	if (!v) return false;
	if (task.size())
	{
		std::string utask = wibble::str::toupper(v->task);
		if (utask.find(task) == std::string::npos)
			return false;
	}
	return true;
}

std::string MatchTask::toString() const
{
	CommaJoiner res;
	res.add("TASK");
	if (task.size()) res.add(task); else res.addUndef();
	return res.join();
}

/*============================================================================*/

MatchTask* MatchTask::parse(const std::string& pattern)
{
	return new MatchTask(pattern);
}

MatcherType task("task", types::TYPE_TASK, (MatcherType::subexpr_parser)MatchTask::parse);

/*============================================================================*/

} }

// vim:set ts=4 sw=4:




