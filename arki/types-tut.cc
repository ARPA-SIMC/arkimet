/*
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/tests/tests.h>
#include <arki/types.h>
#include <arki/types/utils.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;

struct arki_types_shar {
};
TESTGRP(arki_types);

struct TestImpl : public types::Type
{
	int val;
	TestImpl() : val(0) {}
	TestImpl(int val) : val(val) {}
	virtual ~TestImpl() {}

	virtual bool operator==(const Type& o) const { return false; }
	virtual std::string tag() const { return string(); }
	virtual types::Code serialisationCode() const { return types::TYPE_INVALID; }
	virtual size_t serialisationSizeLength() const { return 1; }
	virtual void encodeWithoutEnvelope(utils::codec::Encoder&) const {}
	virtual std::ostream& writeToOstream(std::ostream& o) const { return o; }
    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const {}
	virtual const char* lua_type_name() const { return "arki.types.testimpl"; }
	virtual void lua_push(lua_State* L) const {}
};

struct ImplA : public TestImpl
{
};

struct ImplB : public TestImpl
{
};

struct ImplASub : public ImplA
{
};

// Check generic item assignments
template<> template<>
void to::test<1>()
{
	Item<> a(new ImplA);
	Item<> b(new ImplB);
	Item<> asub(new ImplASub);

	Item<> tmp = a;
	tmp = b;
	tmp = asub;
}

// Check specific item assignments
template<> template<>
void to::test<2>()
{
	Item<ImplA> a(new ImplA);
	Item<ImplB> b(new ImplB);
	Item<ImplASub> asub(new ImplASub);

	Item<ImplA> tmp = a;
	tmp = asub;

	// This is not supposed to compile
	//tmp = b;
}

// Check copying around
template<> template<>
void to::test<3>()
{
	Item<ImplA> a(new ImplA);
	ensure_equals(a->val, 0);

	{
		Item<ImplA> b(a);
		ensure_equals(a->val, 0);
		ensure_equals(b->val, 0);

		{
			Item<ImplA> c = b;
			c = a;
			ensure_equals(a->val, 0);
			ensure_equals(b->val, 0);
			ensure_equals(c->val, 0);
		}

		ensure_equals(a->val, 0);
		ensure_equals(b->val, 0);

		a = b;

		ensure_equals(a->val, 0);
		ensure_equals(b->val, 0);
	}

	ensure_equals(a->val, 0);

	a = a;

	ensure_equals(a->val, 0);
}

// Check UItem
template<> template<>
void to::test<4>()
{
	UItem<ImplA> a;
	UItem<ImplA> b;

	ensure(!a.defined());
	ensure(!b.defined());

	ensure_equals(a, b);
	a = b;
	ensure_equals(a, b);
}

// Check that assignment works
template<> template<>
void to::test<5>()
{
	Item<TestImpl> a(new TestImpl);
	Item<TestImpl> b(new TestImpl(1));
	ensure_equals(a->val, 0);
	ensure_equals(b->val, 1);

	a = new TestImpl(2);
	ensure_equals(a->val, 2);
	a = b;
	ensure_equals(a->val, 1);
	b = new TestImpl(3);
	a = b;
	ensure_equals(a->val, 3);
}

}

// vim:set ts=4 sw=4:
