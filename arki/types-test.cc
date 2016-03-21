#include "config.h"

#include <arki/tests/tests.h>
#include <arki/types.h>
#include <arki/types/utils.h>

#include <sstream>
#include <iostream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

def_tests(arki_types);

struct TestImpl : public types::Type
{
	int val;
	TestImpl() : val(0) {}
	TestImpl(int val) : val(val) {}
	virtual ~TestImpl() {}

    TestImpl* clone() const override = 0;
    bool equals(const Type& o) const override { return false; }
    std::string tag() const override { return string(); }
    types::Code type_code() const override { return TYPE_INVALID; }
    size_t serialisationSizeLength() const override { return 1; }
    void encodeWithoutEnvelope(BinaryEncoder&) const override {}
    std::ostream& writeToOstream(std::ostream& o) const override { return o; }
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override {}
    const char* lua_type_name() const override { return "arki.types.testimpl"; }
};

struct ImplA : public TestImpl
{
    ImplA* clone() const override { return new ImplA; }
};

struct ImplB : public TestImpl
{
    ImplB* clone() const override { return new ImplB; }
};

struct ImplASub : public ImplA
{
    ImplASub* clone() const override { return new ImplASub; }
};

// Check downcast and ucpast
void Tests::register_tests() {

add_method("cast", [] {
    unique_ptr<ImplA> top(new ImplA);
    unique_ptr<ImplASub> sub(new ImplASub);

    unique_ptr<ImplA> from_sub(upcast<ImplA>(move(sub)));
    wassert(actual(from_sub.get()).istrue());

    unique_ptr<ImplASub> back_to_top(downcast<ImplASub>(move(from_sub)));
    wassert(actual(back_to_top.get()).istrue());
});

// Check specific item assignments
add_method("assign", [] {
#if 0
	Item<ImplA> a(new ImplA);
	Item<ImplB> b(new ImplB);
	Item<ImplASub> asub(new ImplASub);

	Item<ImplA> tmp = a;
	tmp = asub;

	// This is not supposed to compile
	//tmp = b;
#endif
});

// Check copying around
add_method("copy", [] {
#if 0
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
#endif
});

// Check UItem
add_method("uitem", [] {
#if 0
	UItem<ImplA> a;
	UItem<ImplA> b;

	ensure(!a.defined());
	ensure(!b.defined());

	ensure_equals(a, b);
	a = b;
	ensure_equals(a, b);
#endif
});

// Check that assignment works
add_method("assign1", [] {
#if 0
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
#endif
});

}

}
