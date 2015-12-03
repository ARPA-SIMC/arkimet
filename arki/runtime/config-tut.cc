#include "config.h"

#include <arki/tests/tests.h>
#include <arki/runtime/config.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::runtime;

struct arki_runtime_config_shar {
};
TESTGRP(arki_runtime_config);

// Test restrict functions
template<> template<>
void to::test<1>()
{
	ConfigFile empty_cfg;
	ConfigFile allowed_cfg;
	ConfigFile unallowed_cfg;
	allowed_cfg.setValue("restrict", "c, d, e, f");
	unallowed_cfg.setValue("restrict", "d, e, f");

	// With no restrictions, everything should be allowed
	Restrict r1("");
	ensure(r1.is_allowed(""));
	ensure(r1.is_allowed("a,b,c"));
	ensure(r1.is_allowed(empty_cfg));
	ensure(r1.is_allowed(allowed_cfg));
	ensure(r1.is_allowed(unallowed_cfg));

	// Normal restrictions
	Restrict r2("a, b,c");
	ensure(not r2.is_allowed(""));
	ensure(r2.is_allowed("a"));
	ensure(r2.is_allowed("a, b"));
	ensure(r2.is_allowed("c, d, e, f"));
	ensure(not r2.is_allowed("d, e, f"));
	ensure(not r2.is_allowed(empty_cfg));
	ensure(r2.is_allowed(allowed_cfg));
	ensure(not r2.is_allowed(unallowed_cfg));
}

}
