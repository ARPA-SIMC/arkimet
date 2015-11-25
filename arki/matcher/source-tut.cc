#include "config.h"

#include <arki/matcher/tests.h>
#include <arki/matcher.h>
#include <arki/metadata.h>
#include <arki/types/source.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_matcher_source_shar
{
    Metadata md;

    arki_matcher_source_shar()
    {
        arki::tests::fill(md);
    }
};
TESTGRP(arki_matcher_source);

// Try matching Minute run
template<> template<>
void to::test<1>()
{
#if 0
	Matcher m;

	m = Matcher::parse("run:MINUTE");
	ensure(m(md));
	m = Matcher::parse("run:MINUTE,12");
	ensure(m(md));
	m = Matcher::parse("run:MINUTE,12:00");
	ensure(m(md));
	m = Matcher::parse("run:MINUTE,13");
	ensure(not m(md));
	m = Matcher::parse("run:MINUTE,12:01");
	ensure(not m(md));

	// Set a different minute
	md.set(run::Minute::create(9, 30));
	m = Matcher::parse("run:MINUTE");
	ensure(m(md));
	m = Matcher::parse("run:MINUTE,09:30");
	ensure(m(md));
	m = Matcher::parse("run:MINUTE,09");
	ensure(not m(md));
	m = Matcher::parse("run:MINUTE,09:31");
	ensure(not m(md));
#endif
}

}

// vim:set ts=4 sw=4:
