/*
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/types/test-utils.h>
#include <arki/scan/odimh5.h>
#include <arki/metadata.h>
#include <arki/types.h>
#include <arki/types/origin.h>
#include <arki/types/reftime.h>
#include <arki/types/task.h>
#include <arki/types/quantity.h>
#include <arki/types/level.h>
#include <arki/types/product.h>
#include <arki/types/area.h>

#include <wibble/sys/fs.h>

#include <radarlib/radar.hpp>

namespace tut {

using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

struct arki_scan_odimh5_shar
{
};

TESTGRP(arki_scan_odimh5);

// Scan an ODIMH5 polar volume
template<> template<>
void to::test<1>()
{
    Metadata md;
    scan::OdimH5 scanner;
    wibble::sys::Buffer buf;
    ValueBag vb;

    scanner.open("inbound/pvol001.h5");

    ensure(scanner.next(md));

    // Check the source info
    atest(sourceblob_is, "odimh5", sys::fs::abspath("."), "inbound/pvol001.h5", 0, 320696, md.source);

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

    // Check origin
    ensure(md.get(types::TYPE_ORIGIN).defined());
    ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::ODIMH5::create("wmo","rad","plc")));

    // Check product
    ensure(md.get(types::TYPE_PRODUCT).defined());
    ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::ODIMH5::create("PVOL","SCAN")));

    // Check level
    ensure(md.get(types::TYPE_LEVEL).defined());
    ensure_equals(md.get(types::TYPE_LEVEL), Item<>(types::level::ODIMH5::create(0,27)));

    // Check reftime
    ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
    ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2000,1,2,3,4,5))));

    // Check task
    ensure(md.get(types::TYPE_TASK).defined());
    ensure_equals(md.get(types::TYPE_TASK), Item<>(types::Task::create("task")));

    // Check quantities
    ensure(md.get(types::TYPE_QUANTITY).defined());
    ensure_equals(md.get(types::TYPE_QUANTITY), Item<>(types::Quantity::create("ACRR,BRDR,CLASS,DBZH,DBZV,HGHT,KDP,LDR,PHIDP,QIND,RATE,RHOHV,SNR,SQI,TH,TV,UWND,VIL,VRAD,VWND,WRAD,ZDR,ad,ad_dev,chi2,dbz,dbz_dev,dd,dd_dev,def,def_dev,div,div_dev,ff,ff_dev,n,rhohv,rhohv_dev,w,w_dev,z,z_dev")));

    // Check area
    vb.clear();
    vb.set("lat", Value::createInteger(44456700));
    vb.set("lon", Value::createInteger(11623600));
    vb.set("radius", Value::createInteger(1000));
    ensure(md.get(types::TYPE_AREA).defined());
    ensure_equals(md.get(types::TYPE_AREA), Item<>(area::ODIMH5::create(vb)));

    ensure(not scanner.next(md));
}

}

// vim:set ts=4 sw=4:
