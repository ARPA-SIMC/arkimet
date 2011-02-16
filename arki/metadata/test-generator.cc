/*
 * metadata/test-generator - Metadata generator to user for tests
 *
 * Copyright (C) 2010--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/metadata/test-generator.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/types/reftime.h>
#include <arki/types/run.h>
#include <cstring>

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {
namespace metadata {
namespace test {

Generator::Generator(const std::string& format)
    : format(format)
{
}

bool Generator::has(types::Code code)
{
    return samples.find(code) != samples.end();
}

void Generator::add(Item<> item)
{
    samples[item->serialisationCode()].push_back(item);
}

void Generator::add(types::Code code, const std::string& val)
{
    add(types::decodeString(code, val));
}

void Generator::add_if_missing(types::Code code, const std::string& val)
{
    if (!has(code))
        add(types::decodeString(code, val));
}

void Generator::defaults_grib1()
{
    format = "grib1";
    add_if_missing(types::TYPE_ORIGIN, "GRIB1(200,0,1)");
    add_if_missing(types::TYPE_PRODUCT, "GRIB1(200,0,2)");
    add_if_missing(types::TYPE_LEVEL, "GRIB1(102)");
    add_if_missing(types::TYPE_TIMERANGE, "GRIB1(1)");
    add_if_missing(types::TYPE_REFTIME, "2010-06-01T00:00:00Z");
    add_if_missing(types::TYPE_AREA, "GRIB(Ni=205, Nj=85, latfirst=30000000, latlast=72000000, lonfirst=-60000000, lonlast=42000000, type=0)");
}

void Generator::defaults_grib2()
{
    format = "grib2";
    add_if_missing(types::TYPE_ORIGIN, "GRIB2(250, 200, 0, 1, 2)");
    add_if_missing(types::TYPE_PRODUCT, "GRIB2(250, 0, 1, 2)");
    add_if_missing(types::TYPE_LEVEL, "GRIB2S(103, 0, 10)");
    add_if_missing(types::TYPE_TIMERANGE, "Timedef(3h, 0, 3h)");
    add_if_missing(types::TYPE_REFTIME, "2010-06-01T00:00:00Z");
    add_if_missing(types::TYPE_AREA, "GRIB(Ni=511, Nj=415, latfirst=-16125000, latlast=9750000, latp=-40000000, lonfirst=344250000, lonlast=16125000, lonp=10000000, rot=0, tn=1)");
    add_if_missing(types::TYPE_PRODDEF, "GRIB(mc=ti, mt=0, pf=1, tf=16, ty=3)");
}

void Generator::defaults_bufr()
{
    format = "bufr";
    add_if_missing(types::TYPE_ORIGIN, "BUFR(200, 0)");
    add_if_missing(types::TYPE_PRODUCT, "BUFR(0, 255, 1, t=synop)");
    add_if_missing(types::TYPE_REFTIME, "2010-06-01T00:00:00Z");
    add_if_missing(types::TYPE_AREA, "GRIB(blo=60, lat=3388000, lon=-553000, sta=150)");
}

void Generator::defaults_odimh5()
{
    format = "odimh5";
    add_if_missing(types::TYPE_ORIGIN, "ODIMH5(wmo, rad, plc)");
    add_if_missing(types::TYPE_PRODUCT, "ODIMH5(PVOL, SCAN)");
    add_if_missing(types::TYPE_LEVEL, "ODIMH5(0, 27)");
    add_if_missing(types::TYPE_REFTIME, "2010-06-01T00:00:00Z");
    add_if_missing(types::TYPE_AREA, "ODIMH5(lat=44456700, lon=11623600, radius=1000)");
    add_if_missing(types::TYPE_TASK, "task");
    add_if_missing(types::TYPE_QUANTITY, "ACRR, BRDR, CLASS, DBZH, DBZV, HGHT, KDP, LDR, PHIDP, QIND, RATE, RHOHV, SNR, SQI, TH, TV, UWND, VIL, VRAD, VWND, WRAD, ZDR, ad, ad_dev, chi2, dbz, dbz_dev, dd, dd_dev, def, def_dev, div, div_dev, ff, ff_dev, n, rhohv, rhohv_dev, w, w_dev, z, z_dev");
}

void Generator::generate(metadata::Consumer& cons)
{
    if (format == "grib1")
        defaults_grib1();
    else if (format == "grib2")
        defaults_grib2();
    else if (format == "bufr")
        defaults_bufr();
    else if (format == "odimh5")
        defaults_odimh5();
    else
        throw wibble::exception::Consistency("generating random messages", "unknown format: " + format);

    Metadata md;
    _generate(samples.begin(), md, cons);
}

bool Generator::_generate(const Samples::const_iterator& i, Metadata& md, metadata::Consumer& cons) const
{
    if (i == samples.end())
    {
        Metadata m(md);

        // Set run from Reftime
        Item<types::Reftime> rt = md.get<types::Reftime>();
        Item<types::reftime::Position> p = rt.upcast<types::reftime::Position>();
        m.set(types::run::Minute::create(p->time->vals[3], p->time->vals[4]));

        // Set source and inline data
        char buf[5432];
        memset(buf, 0, 5432);
        wibble::sys::Buffer data(buf, 5432, false);
        m.setInlineData(format, data);

        return cons(m);
    }

    for (vector< Item<> >::const_iterator j = i->second.begin();
            j != i->second.end(); ++j)
    {
        md.set(*j);
        Samples::const_iterator next = i;
        ++next;
        if (!_generate(next, md, cons)) return false;
    }
    return true;
}

}
}
}

// vim:set ts=4 sw=4:
