#ifndef ARKI_REFCOUNTED_H
#define ARKI_REFCOUNTED_H

/*
 * refcounted - Reference counting memory management
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/wibble/exception.h>

#include <string>
#include <iosfwd>
#include <vector>
#include <map>

namespace arki {
namespace refcounted {

/**
 * Base class for reference counted elements
 */
class Base
{
	mutable int _ref;

public:
	Base() : _ref(0) {}
	virtual ~Base() {}

	/// Increment the reference count to this Data object
	void ref() const { ++_ref; }

	/**
	 * Decrement the reference count to this Data object, and return true
	 * if the reference count went down to 0
	 */
	bool unref() const { return (--_ref) == 0; }

	/// Checked upcast
	template<typename TYPE>
	const TYPE* upcast() const
	{
		const TYPE* val = dynamic_cast<const TYPE*>(this);
		if (!val)
			throw wibble::exception::BadCastExt<Base, TYPE>("accessing item");
		return val;
	}

	/// Checked upcast
	template<typename TYPE>
	TYPE* upcast()
	{
		TYPE* val = dynamic_cast<TYPE*>(this);
		if (!val)
			throw wibble::exception::BadCastExt<Base, TYPE>("accessing item");
		return val;
	}

private:
	// Disallow copying
	Base(const Base&);
	Base& operator=(const Base&);
};

/**
 * Reference counting smart pointer
 */
template<typename TYPE>
struct Pointer
{
	TYPE* m_ptr;
	typedef TYPE value_type;

	Pointer() : m_ptr(0) {}
	Pointer(TYPE* ptr) : m_ptr(ptr)
	{
		if (m_ptr) m_ptr->ref();
	}
	Pointer(const Pointer<TYPE>& val)
	{
		m_ptr = val.m_ptr;
		if (m_ptr) m_ptr->ref();
	}
	template<typename TYPE1>
	Pointer(const Pointer<TYPE1>& val)
	{
		// This will fail to compile if TYPE1 is not a subclass of TYPE
		m_ptr = val.m_ptr;
		if (m_ptr) m_ptr->ref();
	}
	~Pointer()
	{
		if (m_ptr && m_ptr->unref())
			delete m_ptr;
	}

	/// Assignment
	Pointer& operator=(const Pointer<TYPE>& val)
	{
		if (val.m_ptr) val.m_ptr->ref();
		if (m_ptr && m_ptr->unref()) delete m_ptr;
		m_ptr = val.m_ptr;
		return *this;
	}
	template<typename TYPE1>
	Pointer& operator=(const Pointer<TYPE1>& val)
	{
		if (val.m_ptr) val.m_ptr->ref();
		if (m_ptr && m_ptr->unref()) delete m_ptr;
		m_ptr = val.m_ptr;
		return *this;
	}
	Pointer& operator=(TYPE* val)
	{
		if (val) val->ref();
		if (m_ptr && m_ptr->unref()) delete m_ptr;
		m_ptr = val;
		return *this;
	}
	template<typename TYPE1>
	Pointer& operator=(TYPE1* val)
	{
		if (val) val->ref();
		if (m_ptr && m_ptr->unref()) delete m_ptr;
		m_ptr = val;
		return *this;
	}

	void ensureValid() const
	{
		if (!m_ptr)
			throw wibble::exception::Consistency(std::string("accessing ") + typeid(TYPE).name() + " item", "item has no value");
	}

	/// Use of the underlying pointer
	const TYPE* operator->() const
	{
		ensureValid();
		return m_ptr;
	}
	TYPE* operator->()
	{
		ensureValid();
		return m_ptr;
	}
	const TYPE& operator*() const
	{
		ensureValid();
		return *m_ptr;
	}
	TYPE& operator*()
	{
		ensureValid();
		return *m_ptr;
	}

	/// Access the underlying pointer, implicit and explicit
	operator TYPE*() { return m_ptr; }
	operator const TYPE*() const { return m_ptr; }
	const TYPE* ptr() const
	{
		return m_ptr;
	}
	TYPE* ptr()
	{
		return m_ptr;
	}

	/// Checked conversion to a more specific Item
	template<typename TYPE1>
	Pointer<TYPE1> upcast() const
	{
		if (!this->m_ptr) return Pointer<TYPE1>();
		return Pointer<TYPE1>(m_ptr->upcast<TYPE1>());
	}
};

}
}

// vim:set ts=4 sw=4:
#endif
