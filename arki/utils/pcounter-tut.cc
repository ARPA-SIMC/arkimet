#include "config.h"
#include <arki/tests/tests.h>
#include <arki/utils/pcounter.h>
#include <sstream>
#include <iostream>

using namespace arki;
using namespace arki::tests;
using namespace std;

namespace tut
{

/*============================================================================*/

struct arki_utils_pcounter_shar
{
};

TESTGRP(arki_utils_pcounter);

// Test running one shot insert queries
template<> template<> void to::test<1>()
{
	{
	arki::utils::PersistentCounter<int> c("pcounter_int.txt");
	ensure(c.get() == 0);
	ensure(c.inc() == 1);
	ensure(c.inc() == 2);
	ensure(c.inc() == 3);
	}

	{
	arki::utils::PersistentCounter<int> c;
	c.bind("pcounter_int.txt");
	ensure(c.get() == 3);
	c.unbind();
	}
}

// Test running one shot insert queries
template<> template<> void to::test<2>()
{
	{
	arki::utils::PersistentCounter<double> c("pcounter_double.txt");
	ensure(c.get() == 0);
	ensure(c.inc() == 1);
	ensure(c.inc() == 2);
	ensure(c.inc() == 3);
	}

	{
	arki::utils::PersistentCounter<double> c;
	c.bind("pcounter_double.txt");
	ensure(c.get() == 3);
	c.unbind();
	}
}

/*============================================================================*/

}


























