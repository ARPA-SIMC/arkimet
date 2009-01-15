/*
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

#include <arki/tests/test-utils.h>
#include <arki/refcounted.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::refcounted;

struct arki_refcounted_shar {
};
TESTGRP(arki_refcounted);

struct TestImpl : public refcounted::Base
{
	int val;
	TestImpl() : val(0) {}
	virtual ~TestImpl() {}
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
	Pointer<TestImpl> a(new ImplA);
	Pointer<TestImpl> b(new ImplB);
	Pointer<TestImpl> asub(new ImplASub);

	Pointer<TestImpl> tmp = a;
	tmp = b;
	tmp = asub;
}

// Check specific item assignments
template<> template<>
void to::test<2>()
{
	Pointer<ImplA> a(new ImplA);
	Pointer<ImplB> b(new ImplB);
	Pointer<ImplASub> asub(new ImplASub);

	Pointer<ImplA> tmp = a;
	tmp = asub;

	// This is not supposed to compile
	//tmp = b;
}

// Check copying around
template<> template<>
void to::test<3>()
{
	Pointer<ImplA> a(new ImplA);
	ensure_equals(a->val, 0);

	{
		Pointer<ImplA> b(a);
		ensure_equals(a->val, 0);
		ensure_equals(b->val, 0);

		{
			Pointer<ImplA> c = b;
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

}
// vim:set ts=4 sw=4:
