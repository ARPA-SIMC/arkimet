#include <arki/tests/tests.h>
#include <arki/dispatcher.h>
#include <arki/dataset.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/types/assigneddataset.h>
#include <arki/scan/grib.h>
#include <arki/scan/any.h>
#include <arki/utils/accounting.h>
#include <arki/validator.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace wibble::tests;

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
		stringstream incfg(conf);
		config.parse(incfg, "(memory)");
	}
};
TESTGRP(arki_dispatcher);

namespace {
inline std::string dsname(const Metadata& md)
{
    return md.get<AssignedDataset>()->name;
}

inline unique_ptr<Metadata> wrap(const Metadata& md)
{
    return unique_ptr<Metadata>(new Metadata(md));
}

}

// Test simple dispatching
template<> template<>
void to::test<1>()
{
	using namespace arki::utils::acct;

	plain_data_read_count.reset();

	Metadata md;
	metadata::Collection mdc;
	scan::Grib scanner;
	RealDispatcher dispatcher(config);
	scanner.open("inbound/test.grib1");
	ensure(scanner.next(md));
	ensure_equals(dispatcher.dispatch(unique_ptr<Metadata>(new Metadata(md)), mdc), Dispatcher::DISP_OK);
	ensure_equals(dsname(mdc.back()), "test200");
	ensure(scanner.next(md));
	ensure_equals(dispatcher.dispatch(unique_ptr<Metadata>(new Metadata(md)), mdc), Dispatcher::DISP_OK);
	ensure_equals(dsname(mdc.back()), "test80");
	ensure(scanner.next(md));
	ensure_equals(dispatcher.dispatch(unique_ptr<Metadata>(new Metadata(md)), mdc), Dispatcher::DISP_ERROR);
	ensure_equals(dsname(mdc.back()), "error");
	ensure(!scanner.next(md));

	dispatcher.flush();

	ensure_equals(plain_data_read_count.val(), 0u);
}

// Test a case where dispatch is known to fail
template<> template<>
void to::test<2>()
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
	stringstream incfg(conf);
	config.parse(incfg, "(memory)");

	metadata::Collection source;
	scan::scan("inbound/tempforecast.bufr", source);
	ensure_equals(source.size(), 1u);

	Matcher matcher = Matcher::parse("origin:BUFR,200; product:BUFR:t=temp");
	ensure(matcher(source[0]));

	metadata::Collection mdc;
	RealDispatcher dispatcher(config);
    ensure_equals(dispatcher.dispatch(wrap(source[0]), mdc), Dispatcher::DISP_OK);
	ensure_equals(dsname(mdc.back()), "lami_temp");

	dispatcher.flush();
#endif
}

// Test dispatch to error datasets after validation errors
template<> template<>
void to::test<3>()
{
    Metadata md;
    metadata::Collection mdc;
    scan::Grib scanner;
    RealDispatcher dispatcher(config);
    validators::FailAlways fail_always;
    dispatcher.add_validator(fail_always);
    scanner.open("inbound/test.grib1");
    ensure(scanner.next(md));
    ensure_equals(dispatcher.dispatch(wrap(md), mdc), Dispatcher::DISP_ERROR);
    ensure_equals(dsname(mdc.back()), "error");
    ensure(scanner.next(md));
    ensure_equals(dispatcher.dispatch(wrap(md), mdc), Dispatcher::DISP_ERROR);
    ensure_equals(dsname(mdc.back()), "error");
    ensure(scanner.next(md));
    ensure_equals(dispatcher.dispatch(wrap(md), mdc), Dispatcher::DISP_ERROR);
    ensure_equals(dsname(mdc.back()), "error");
    ensure(!scanner.next(md));
    dispatcher.flush();
}

// Test dispatching files with no reftime, they should end up in the error dataset
template<> template<>
void to::test<4>()
{
    metadata::Collection source;
    scan::scan("inbound/wrongdate.bufr", source);
    wassert(actual(source.size()) == 6);

    RealDispatcher dispatcher(config);
    metadata::Collection mdc;

    wassert(actual(dispatcher.dispatch(wrap(source[0]), mdc)) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(mdc.back())) == "error");
    wassert(actual(dispatcher.dispatch(wrap(source[1]), mdc)) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(mdc.back())) == "error");
    wassert(actual(dispatcher.dispatch(wrap(source[2]), mdc)) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(mdc.back())) == "error");
    wassert(actual(dispatcher.dispatch(wrap(source[3]), mdc)) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(mdc.back())) == "error");
    wassert(actual(dispatcher.dispatch(wrap(source[4]), mdc)) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(mdc.back())) == "error");
    wassert(actual(dispatcher.dispatch(wrap(source[5]), mdc)) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(mdc.back())) == "error");

    dispatcher.flush();
}

}

// vim:set ts=4 sw=4:
