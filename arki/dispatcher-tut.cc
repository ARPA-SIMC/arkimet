#include <arki/tests/tests.h>
#include <arki/dispatcher.h>
#include <arki/dataset.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/types/source/blob.h>
#include <arki/scan/grib.h>
#include <arki/scan/any.h>
#include <arki/utils/accounting.h>
#include <arki/utils/string.h>
#include <arki/validator.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::tests;
using namespace arki::utils;

struct arki_dispatcher_shar {
	ConfigFile config;

	arki_dispatcher_shar()
	{
		// Cleanup the test datasets
		system("rm -rf test200/*");
		system("rm -rf test80/*");
		system("rm -rf error/*");

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
        config.parse(conf);
    }
};
TESTGRP(arki_dispatcher);

namespace {
inline std::string dsname(const Metadata& md)
{
    if (!md.has_source_blob()) return "(md source is not a blob source)";
    return str::basename(md.sourceBlob().basedir);
}

inline unique_ptr<Metadata> wrap(const Metadata& md)
{
    return unique_ptr<Metadata>(new Metadata(md));
}

}

// Test simple dispatching
def_test(1)
{
    using namespace arki::utils::acct;

    plain_data_read_count.reset();

    Metadata md;
    metadata::Collection mdc;
    scan::Grib scanner;
    RealDispatcher dispatcher(config);
    scanner.open("inbound/test.grib1");
    ensure(scanner.next(md));
    ensure_equals(dispatcher.dispatch(unique_ptr<Metadata>(new Metadata(md)), mdc.inserter_func()), Dispatcher::DISP_OK);
    wassert(actual(dsname(mdc.back())) == "test200");
    ensure(scanner.next(md));
    ensure_equals(dispatcher.dispatch(unique_ptr<Metadata>(new Metadata(md)), mdc.inserter_func()), Dispatcher::DISP_OK);
    wassert(actual(dsname(mdc.back())) == "test80");
    ensure(scanner.next(md));
    ensure_equals(dispatcher.dispatch(unique_ptr<Metadata>(new Metadata(md)), mdc.inserter_func()), Dispatcher::DISP_ERROR);
    wassert(actual(dsname(mdc.back())) == "error");
    ensure(!scanner.next(md));

    dispatcher.flush();

    ensure_equals(plain_data_read_count.val(), 0u);
}

// Test a case where dispatch is known to fail
def_test(2)
{
#ifdef HAVE_DBALLE
	// In-memory dataset configuration
	string conf =
		"[lami_temp]\n"
		"filter = origin:BUFR,200; product:BUFR:t=temp\n"
		"name = lami_temp\n"
		"type = discard\n"
		"\n"
		"[error]\n"
		"type = discard\n"
		"name = error\n";
    config.parse(conf);

    metadata::Collection source("inbound/tempforecast.bufr");
    ensure_equals(source.size(), 1u);

    Matcher matcher = Matcher::parse("origin:BUFR,200; product:BUFR:t=temp");
    ensure(matcher(source[0]));

    metadata::Collection mdc;
    RealDispatcher dispatcher(config);
    ensure_equals(dispatcher.dispatch(wrap(source[0]), mdc.inserter_func()), Dispatcher::DISP_OK);
    wassert(actual(mdc.back().sourceBlob()) == source[0].sourceBlob());

    dispatcher.flush();
#endif
}

// Test dispatch to error datasets after validation errors
def_test(3)
{
    Metadata md;
    metadata::Collection mdc;
    scan::Grib scanner;
    RealDispatcher dispatcher(config);
    validators::FailAlways fail_always;
    dispatcher.add_validator(fail_always);
    scanner.open("inbound/test.grib1");
    ensure(scanner.next(md));
    ensure_equals(dispatcher.dispatch(wrap(md), mdc.inserter_func()), Dispatcher::DISP_ERROR);
    ensure_equals(dsname(mdc.back()), "error");
    ensure(scanner.next(md));
    ensure_equals(dispatcher.dispatch(wrap(md), mdc.inserter_func()), Dispatcher::DISP_ERROR);
    ensure_equals(dsname(mdc.back()), "error");
    ensure(scanner.next(md));
    ensure_equals(dispatcher.dispatch(wrap(md), mdc.inserter_func()), Dispatcher::DISP_ERROR);
    ensure_equals(dsname(mdc.back()), "error");
    ensure(!scanner.next(md));
    dispatcher.flush();
}

// Test dispatching files with no reftime, they should end up in the error dataset
def_test(4)
{
    metadata::Collection source("inbound/wrongdate.bufr");
    wassert(actual(source.size()) == 6u);

    RealDispatcher dispatcher(config);
    metadata::Collection mdc;

    wassert(actual(dispatcher.dispatch(wrap(source[0]), mdc.inserter_func())) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(mdc.back())) == "error");
    wassert(actual(dispatcher.dispatch(wrap(source[1]), mdc.inserter_func())) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(mdc.back())) == "error");
    wassert(actual(dispatcher.dispatch(wrap(source[2]), mdc.inserter_func())) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(mdc.back())) == "error");
    wassert(actual(dispatcher.dispatch(wrap(source[3]), mdc.inserter_func())) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(mdc.back())) == "error");
    wassert(actual(dispatcher.dispatch(wrap(source[4]), mdc.inserter_func())) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(mdc.back())) == "error");
    wassert(actual(dispatcher.dispatch(wrap(source[5]), mdc.inserter_func())) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(mdc.back())) == "error");

    dispatcher.flush();
}

}
