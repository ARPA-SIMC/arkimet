#include "config.h"

#include <arki/tests/tests.h>
#include <arki/utils/intern.h>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::utils;

struct arki_utils_intern_shar {
};
TESTGRP(arki_utils_intern);

template<> template<>
void to::test<1>()
{
    StringInternTable table;

    // Check initial state
    ensure_equals(table.table_size, 1024u);
    for (size_t i = 0; i < table.table_size; ++i)
        ensure(table.table[i] == 0);
    ensure(table.string_table.empty());

    // Check that interning returns stable IDs
    unsigned val = table.intern("foo");
    ensure_equals(table.intern("foo"), val);
    ensure_equals(table.intern(string("foo")), val);

    // Ensure that different strings have different IDs
    unsigned val1 = table.intern("bar");
    ensure(val != val1);

    // Check what happens if we fill the hashtable and need the overflow
    // buckets
    unsigned ids[2000];
    for (unsigned i = 0; i < 2000; ++i)
        ids[i] = table.intern("val" + to_string(i));
    for (unsigned i = 0; i < 2000; ++i)
        ensure_equals(ids[i], table.intern("val" + to_string(i)));
}

}
