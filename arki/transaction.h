#ifndef ARKI_TRANSACTION_H
#define ARKI_TRANSACTION_H

/*
 * transaction - RAII transaction management
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

namespace arki {

/**
 * RAII-style transaction.
 *
 * To work it needs to be embedded inside a Pending container.
 */
class Transaction
{
public:
    virtual ~Transaction();
    virtual void commit() = 0;
    virtual void rollback() = 0;
};

/**
 * Represent a pending operation.
 *
 * The operation will be performed when the commit method is called.
 * Otherwise, if either rollback is called or the object is destroyed, the
 * operation will be undone.
 *
 * Copy and assignment have unique_ptr-like semantics
 */
struct Pending
{
    Transaction* trans = nullptr;

    Pending() {}
    Pending(const Pending& p) = delete;
    Pending(Pending&& p);
    Pending(Transaction* trans);
    ~Pending();

    Pending& operator=(const Pending& p) = delete;
    Pending& operator=(Pending&& p);

	/// true if there is an operation to commit, else false
	operator bool() const { return trans != 0; }

	/// Commit the pending operation
	void commit();

	/// Rollback the pending operation
	void rollback();
};

}

// vim:set ts=4 sw=4:
#endif
