/*
 * itemset - Container for metadata items
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/itemset.h>
#include <arki/utils.h>

using namespace std;

namespace arki {

UItem<> ItemSet::get(types::Code code) const
{
	const_iterator i = m_vals.find(code);
	if (i == m_vals.end())
		return UItem<>();
	return i->second;
}

void ItemSet::set(const Item<>& i)
{
	types::Code code = i->serialisationCode();
	std::map< types::Code, Item<> >::iterator it = m_vals.find(code);
	if (it == m_vals.end())
		m_vals.insert(make_pair(i->serialisationCode(), i));
	else
		it->second = i;
}

void ItemSet::unset(types::Code code)
{
	m_vals.erase(code);
}

void ItemSet::clear()
{
	m_vals.clear();
}

bool ItemSet::operator==(const ItemSet& m) const
{
	return m_vals == m.m_vals;
}

int ItemSet::compare(const ItemSet& m) const
{
	return utils::compareMaps(m_vals, m.m_vals);
}

}
