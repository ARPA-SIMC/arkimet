#ifndef ARKI_ITEMSET_H
#define ARKI_ITEMSET_H

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


#include <arki/types.h>
#include <map>

namespace arki {

class ItemSet
{
protected:
	std::map< types::Code, Item<> > m_vals;

public:
	typedef std::map< types::Code, Item<> >::const_iterator const_iterator;

	const_iterator begin() const { return m_vals.begin(); }
	const_iterator end() const { return m_vals.end(); }

        size_t empty() const { return m_vals.empty(); }

        size_t size() const { return m_vals.size(); }

	bool has(types::Code code) const { return m_vals.find(code) != m_vals.end(); }

	UItem<> get(types::Code code) const;

	template<typename T>
	UItem<T> get() const
	{
		UItem<> i = get(types::traits<T>::type_code);
		return i.upcast<T>();
	};

	/// Set an item
	void set(const Item<>& i);

    /// Set an item, from strings. Useful for quickly setting up data in tests.
    void set(const std::string& type, const std::string& val);

	/// Unset an item
	void unset(types::Code code);

	/// Set an item
	template<typename T>
	void set(const Item<T>& i) { set(Item<>(i)); }

	/// Remove all items
	void clear();

	/**
	 * Check that two ItemSets contain the same information
	 */
	bool operator==(const ItemSet& m) const;

	/**
	 * Check that two ItemSets contain different information
	 */
	bool operator!=(const ItemSet& m) const { return !operator==(m); }

	int compare(const ItemSet& m) const;
	bool operator<(const ItemSet& o) const { return compare(o) < 0; }
	bool operator<=(const ItemSet& o) const { return compare(o) <= 0; }
	bool operator>(const ItemSet& o) const { return compare(o) > 0; }
	bool operator>=(const ItemSet& o) const { return compare(o) >= 0; }
};

}

// vim:set ts=4 sw=4:
#endif
