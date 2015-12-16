#include "config.h"
#include <arki/tests/tests.h>
#include <arki/datasetpool.h>
#include <arki/configfile.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::tests;

struct arki_datasetpool_shar {
	ConfigFile config;

	arki_datasetpool_shar()
	{
		// In-memory dataset configuration
		string conf =
			"[test200]\n"
			"type = test\n"
			"step = daily\n"
			"filter = origin: GRIB1,200\n"
			"index = origin, reftime\n"
			"name = test200\n"
			"path = test200\n"
			"\n"
			"[test80]\n"
			"type = test\n"
			"step = daily\n"
			"filter = origin: GRIB1,80\n"
			"index = origin, reftime\n"
			"name = test80\n"
			"path = test80\n"
			"\n"
			"[error]\n"
			"type = error\n"
			"step = daily\n"
			"name = error\n"
			"path = error\n";
		stringstream incfg(conf);
		config.parse(incfg, "(memory)");
	}
};
TESTGRP(arki_datasetpool);

// Test instantiating readonly datasets
template<> template<>
void to::test<1>()
{
	ReadonlyDatasetPool pool(config);
	ensure(pool.get("error") != 0);
	ensure(pool.get("test200") != 0);
	ensure(pool.get("test80") != 0);
	ensure(pool.get("duplicates") == 0);
}

// Test instantiating writable datasets
template<> template<>
void to::test<2>()
{
    WriterPool pool(config);
    ensure(pool.get("error") != 0);
    ensure(pool.get("test200") != 0);
    ensure(pool.get("test80") != 0);
    ensure(pool.get("duplicates") == 0);
}

}
