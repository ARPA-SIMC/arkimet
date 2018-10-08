#include "arki/metadata/test-generator.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/types/reftime.h"
#include "arki/types/run.h"
#include <cstring>

using namespace std;
using namespace arki::types;

namespace arki {
namespace metadata {
namespace test {

Generator::Generator(const std::string& format)
    : format(format)
{
}

Generator::~Generator()
{
    for (Samples::iterator i = samples.begin(); i != samples.end(); ++i)
        for (std::vector<types::Type*>::iterator j = i->second.begin(); j != i->second.end(); ++j)
            delete *j;
}

bool Generator::has(types::Code code)
{
    return samples.find(code) != samples.end();
}

void Generator::add(unique_ptr<types::Type> item)
{
    Code code = item->type_code();
    samples[code].push_back(item.release());
}

void Generator::add(const Type& item)
{
    samples[item.type_code()].push_back(item.clone());
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
    add_if_missing(TYPE_ORIGIN, "GRIB1(200,0,1)");
    add_if_missing(TYPE_PRODUCT, "GRIB1(200,0,2)");
    add_if_missing(TYPE_LEVEL, "GRIB1(102)");
    add_if_missing(TYPE_TIMERANGE, "GRIB1(1)");
    add_if_missing(TYPE_REFTIME, "2010-06-01T00:00:00Z");
    add_if_missing(TYPE_AREA, "GRIB(Ni=205, Nj=85, latfirst=30000000, latlast=72000000, lonfirst=-60000000, lonlast=42000000, type=0)");
}

void Generator::defaults_grib2()
{
    format = "grib2";
    add_if_missing(TYPE_ORIGIN, "GRIB2(250, 200, 0, 1, 2)");
    add_if_missing(TYPE_PRODUCT, "GRIB2(250, 0, 1, 2)");
    add_if_missing(TYPE_LEVEL, "GRIB2S(103, 0, 10)");
    add_if_missing(TYPE_TIMERANGE, "Timedef(3h, 0, 3h)");
    add_if_missing(TYPE_REFTIME, "2010-06-01T00:00:00Z");
    add_if_missing(TYPE_AREA, "GRIB(Ni=511, Nj=415, latfirst=-16125000, latlast=9750000, latp=-40000000, lonfirst=344250000, lonlast=16125000, lonp=10000000, rot=0, tn=1)");
    add_if_missing(TYPE_PRODDEF, "GRIB(mc=ti, mt=0, pf=1, tf=16, ty=3)");
}

void Generator::defaults_bufr()
{
    format = "bufr";
    add_if_missing(TYPE_ORIGIN, "BUFR(200, 0)");
    add_if_missing(TYPE_PRODUCT, "BUFR(0, 255, 1, t=synop)");
    add_if_missing(TYPE_REFTIME, "2010-06-01T00:00:00Z");
    add_if_missing(TYPE_AREA, "GRIB(blo=60, lat=3388000, lon=-553000, sta=150)");
}

void Generator::defaults_odimh5()
{
    format = "odimh5";
    add_if_missing(TYPE_ORIGIN, "ODIMH5(wmo, rad, plc)");
    add_if_missing(TYPE_PRODUCT, "ODIMH5(PVOL, SCAN)");
    add_if_missing(TYPE_LEVEL, "ODIMH5(0, 27)");
    add_if_missing(TYPE_REFTIME, "2010-06-01T00:00:00Z");
    add_if_missing(TYPE_AREA, "ODIMH5(lat=44456700, lon=11623600, radius=1000)");
    add_if_missing(TYPE_TASK, "task");
    add_if_missing(TYPE_QUANTITY, "ACRR, BRDR, CLASS, DBZH, DBZV, HGHT, KDP, LDR, PHIDP, QIND, RATE, RHOHV, SNR, SQI, TH, TV, UWND, VIL, VRAD, VWND, WRAD, ZDR, ad, ad_dev, chi2, dbz, dbz_dev, dd, dd_dev, def, def_dev, div, div_dev, ff, ff_dev, n, rhohv, rhohv_dev, w, w_dev, z, z_dev");
}

void Generator::generate(metadata_dest_func cons)
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
        throw std::runtime_error("cannot generate random messages: unknown format: " + format);

    Metadata md;
    _generate(samples.begin(), md, cons);
}

bool Generator::_generate(const Samples::const_iterator& i, Metadata& md, metadata_dest_func cons) const
{
    if (i == samples.end())
    {
        unique_ptr<Metadata> m(new Metadata(md));

        // Set run from Reftime
        const types::Reftime* rt = md.get<types::Reftime>();
        const types::reftime::Position* p = dynamic_cast<const types::reftime::Position*>(rt);
        m->set(types::run::Minute::create(p->time.ho, p->time.mi));

        // Set source and inline data
        m->set_source_inline(format, metadata::DataManager::get().to_data(format, vector<uint8_t>(5432)));

        return cons(move(m));
    }

    for (vector<Type*>::const_iterator j = i->second.begin(); j != i->second.end(); ++j)
    {
        md.set(**j);
        Samples::const_iterator next = i;
        ++next;
        if (!_generate(next, md, cons)) return false;
    }
    return true;
}

}
}
}
