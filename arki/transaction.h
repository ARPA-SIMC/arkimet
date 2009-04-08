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
 * Reference counting is provided to allow to use Transactions inside a Pending
 * object that can be passed around.  A transaction can be created directly on
 * the stack, and in that case the reference counting can just be ignored.
 */
class Transaction
{
protected:
	int _ref;

public:
	Transaction() : _ref(0) {}
	virtual ~Transaction()
	{
		// Here, in the implementations, you want to call rollback() unless the
		// transaction has already fired.
	}

	/// Increment the reference count
	void ref() { ++_ref; }

	/// Decrement the reference count and return true if it goes to 0
	bool unref() { return --_ref == 0; }

	virtual void commit() = 0;
	virtual void rollback() = 0;
};

/**
 * Represent a pending operation.
 *
 * The operation will be performed when the commit method is called.
 * Otherwise, if either rollback is called or the object is destroyed, the
 * operation will be undone.
 */
struct Pending
{
	Transaction* trans;

	Pending() : trans(0) {}
	Pending(const Pending& p);
	Pending(Transaction* trans);
	~Pending();

	Pending& operator=(const Pending& p);

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
