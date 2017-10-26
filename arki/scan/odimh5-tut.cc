#include <arki/metadata/tests.h>
#include <arki/scan/odimh5.h>
#include <arki/metadata.h>
#include <arki/types.h>
#include <arki/utils/sys.h>
#include <iostream>

namespace tut {

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

struct arki_scan_odimh5_shar
{
};

TESTGRP(arki_scan_odimh5);

// Scan an ODIMH5 polar volume
def_test(1)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/PVOL_v20.h5");

	ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/PVOL_v20.h5", 0, 320696));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 320696u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(wmo,rad,plc)"));
    wassert(actual(md).contains("product", "ODIMH5(PVOL,SCAN)"));
    wassert(actual(md).contains("level", "ODIMH5(0,27)"));
    wassert(actual(md).contains("reftime", "2000-01-02T03:04:05Z"));
    wassert(actual(md).contains("task", "task"));
    wassert(actual(md).contains("quantity", "ACRR,BRDR,CLASS,DBZH,DBZV,HGHT,KDP,LDR,PHIDP,QIND,RATE,RHOHV,SNR,SQI,TH,TV,UWND,VIL,VRAD,VWND,WRAD,ZDR,ad,ad_dev,chi2,dbz,dbz_dev,dd,dd_dev,def,def_dev,div,div_dev,ff,ff_dev,n,rhohv,rhohv_dev,w,w_dev,z,z_dev"));
    wassert(actual(md).contains("area", "ODIMH5(lat=44456700,lon=11623600,radius=1000)"));

	ensure(not scanner.next(md));
}

def_test(2)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/COMP_CAPPI_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/COMP_CAPPI_v20.h5", 0, 49113));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49113u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(COMP,CAPPI)"));
    wassert(actual(md).contains("level", "GRIB1(105,10)"));
    wassert(actual(md).contains("reftime", "2013-03-18T14:30:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "DBZH"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(3)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/COMP_ETOP_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/COMP_ETOP_v20.h5", 0, 49113));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49113u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(COMP,ETOP)"));
    ensure(!md.get(TYPE_LEVEL));
    wassert(actual(md).contains("reftime", "2013-03-18T14:30:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "HGHT"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(4)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/COMP_LBM_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/COMP_LBM_v20.h5", 0, 49057));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49057u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(COMP,NEW:LBM_ARPA)"));
    ensure(!md.get(TYPE_LEVEL));
    wassert(actual(md).contains("reftime", "2013-03-18T14:30:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "DBZH"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(5)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/COMP_MAX_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/COMP_MAX_v20.h5", 0, 49049));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49049u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(COMP,MAX)"));
    ensure(!md.get(TYPE_LEVEL));
    wassert(actual(md).contains("reftime", "2013-03-18T14:30:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "DBZH"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(6)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/COMP_PCAPPI_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/COMP_PCAPPI_v20.h5", 0, 49113));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49113u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(COMP,PCAPPI)"));
    wassert(actual(md).contains("level", "GRIB1(105,10)"));
    wassert(actual(md).contains("reftime", "2013-03-18T14:30:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "DBZH"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(7)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/COMP_PPI_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/COMP_PPI_v20.h5", 0, 49113));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49113u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(COMP,PPI)"));
    wassert(actual(md).contains("level", "ODIMH5(10, 10)"));
    wassert(actual(md).contains("reftime", "2013-03-18T14:30:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "DBZH"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(8)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/COMP_RR_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/COMP_RR_v20.h5", 0, 49049));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49049u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(COMP,RR)"));
    ensure(!md.get(TYPE_LEVEL));
    wassert(actual(md).contains("timerange", "Timedef(0s,1,1h)"));
    wassert(actual(md).contains("reftime", "2013-03-18T14:30:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "ACRR"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(9)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/COMP_VIL_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/COMP_VIL_v20.h5", 0, 49097));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49097u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(COMP,VIL)"));
    wassert(actual(md).contains("level", "GRIB1(106,10,0)"));
    ensure(!md.get(TYPE_TIMERANGE));
    wassert(actual(md).contains("reftime", "2013-03-18T14:30:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "VIL"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(10)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/IMAGE_CAPPI_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/IMAGE_CAPPI_v20.h5", 0, 49113));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49113u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(IMAGE,CAPPI)"));
    wassert(actual(md).contains("level", "GRIB1(105,500)"));
    ensure(!md.get(TYPE_TIMERANGE));
    wassert(actual(md).contains("reftime", "2013-03-18T14:30:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "DBZH"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(11)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/IMAGE_ETOP_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/IMAGE_ETOP_v20.h5", 0, 49113));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49113u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(IMAGE,ETOP)"));
    ensure(!md.get(TYPE_LEVEL));
    wassert(actual(md).contains("reftime", "2013-03-18T14:30:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "HGHT"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(12)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/IMAGE_HVMI_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/IMAGE_HVMI_v20.h5", 0, 68777));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 68777u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(IMAGE,HVMI)"));
    ensure(!md.get(TYPE_LEVEL));
    wassert(actual(md).contains("reftime", "2013-03-18T14:30:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "DBZH"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(13)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/IMAGE_MAX_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/IMAGE_MAX_v20.h5", 0, 49049));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49049u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(IMAGE,MAX)"));
    ensure(!md.get(TYPE_LEVEL));
    wassert(actual(md).contains("reftime", "2013-03-18T14:30:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "DBZH"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(14)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/IMAGE_PCAPPI_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/IMAGE_PCAPPI_v20.h5", 0, 49113));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49113u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(IMAGE,PCAPPI)"));
    wassert(actual(md).contains("level", "GRIB1(105,500)"));
    wassert(actual(md).contains("reftime", "2013-03-18T14:30:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "DBZH"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(15)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/IMAGE_PPI_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/IMAGE_PPI_v20.h5", 0, 49113));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49113u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(IMAGE,PPI)"));
    wassert(actual(md).contains("level", "ODIMH5(0.5, 0.5)"));
    wassert(actual(md).contains("reftime", "2013-03-18T14:30:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "DBZH"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(16)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/IMAGE_RR_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/IMAGE_RR_v20.h5", 0, 49049));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49049u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(IMAGE,RR)"));
    ensure(!md.get(TYPE_LEVEL));
    wassert(actual(md).contains("timerange", "Timedef(0s,1,1h)"));
    wassert(actual(md).contains("reftime", "2013-03-18T14:30:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "ACRR"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(17)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/IMAGE_VIL_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/IMAGE_VIL_v20.h5", 0, 49097));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 49097u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(IMAGE,VIL)"));
    wassert(actual(md).contains("level", "GRIB1(106,10,0)"));
    wassert(actual(md).contains("reftime", "2013-03-18T14:30:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "VIL"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(18)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/IMAGE_ZLR-BB_v20.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/IMAGE_ZLR-BB_v20.h5", 0, 62161));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 62161u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(IMAGE,NEW:LBM_ARPA)"));
    ensure(!md.get(TYPE_LEVEL));
    wassert(actual(md).contains("reftime", "2013-03-18T10:00:00Z"));
    wassert(actual(md).contains("task", "ZLR-BB"));
    wassert(actual(md).contains("quantity", "DBZH"));
    wassert(actual(md).contains("area", "GRIB(latfirst=42314117,lonfirst=8273203,latlast=46912151,lonlast=14987079,type=0)"));

	ensure(not scanner.next(md));
}
def_test(19)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/XSEC_v21.h5");
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("odimh5", sys::abspath("."), "inbound/odimh5/XSEC_v21.h5", 0, 19717));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 19717u);
	ensure_equals(string((const char*)buf.data(), 1, 3), "HDF");

	// Check notes
	if (md.notes().size() != 1)
	{
		for (size_t i = 0; i < md.notes().size(); ++i)
			cerr << md.notes()[i] << endl;
		ensure_equals(md.notes().size(), 1u);
	}

    // Check contents
    wassert(actual(md).contains("origin", "ODIMH5(16144,IY46,itspc)"));
    wassert(actual(md).contains("product", "ODIMH5(XSEC,XSEC)"));
    ensure(!md.get(TYPE_LEVEL));
    wassert(actual(md).contains("reftime", "2013-11-04T14:10:00Z"));
    wassert(actual(md).contains("task", "XZS"));
    wassert(actual(md).contains("quantity", "DBZH"));
    wassert(actual(md).contains("area", "GRIB(latfirst=44320636,lonfirst=11122189,latlast=44821945,lonlast=12546566,type=0)"));

	ensure(not scanner.next(md));
}

// Check that the scanner silently discard an empty file
def_test(20)
{
    Metadata md;
    scan::OdimH5 scanner;
    vector<uint8_t> buf;

    scanner.test_open("inbound/odimh5/empty.h5");
    ensure(not scanner.next(md));
}

}

// vim:set ts=4 sw=4:
