#include "arki/matcher/origin.h"
#include "arki/matcher/utils.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace matcher {

std::string MatchOrigin::name() const { return "origin"; }

MatchOriginGRIB1::MatchOriginGRIB1(int centre, int subcentre, int process)
    : centre(centre), subcentre(subcentre), process(process)
{
}

MatchOriginGRIB1::MatchOriginGRIB1(const std::string& pattern)
{
    OptionalCommaList args(pattern);
    centre    = args.getInt(0, -1);
    subcentre = args.getInt(1, -1);
    process   = args.getInt(2, -1);
}

MatchOriginGRIB1* MatchOriginGRIB1::clone() const
{
    return new MatchOriginGRIB1(centre, subcentre, process);
}

bool MatchOriginGRIB1::matchItem(const Type& o) const
{
    const types::Origin* v = dynamic_cast<const types::Origin*>(&o);
    if (!v)
        return false;
    if (v->style() != origin::Style::GRIB1)
        return false;
    unsigned v_centre, v_subcentre, v_process;
    v->get_GRIB1(v_centre, v_subcentre, v_process);
    if (centre != -1 && (unsigned)centre != v_centre)
        return false;
    if (subcentre != -1 && (unsigned)subcentre != v_subcentre)
        return false;
    if (process != -1 && (unsigned)process != v_process)
        return false;
    return true;
}

bool MatchOriginGRIB1::match_buffer(types::Code code, const uint8_t* data,
                                    unsigned size) const
{
    if (code != TYPE_ORIGIN)
        return false;
    if (size < 1)
        return false;
    if (Origin::style(data, size) != origin::Style::GRIB1)
        return false;
    unsigned v_centre, v_subcentre, v_process;
    Origin::get_GRIB1(data, size, v_centre, v_subcentre, v_process);
    if (centre != -1 && (unsigned)centre != v_centre)
        return false;
    if (subcentre != -1 && (unsigned)subcentre != v_subcentre)
        return false;
    if (process != -1 && (unsigned)process != v_process)
        return false;
    return true;
}

std::string MatchOriginGRIB1::toString() const
{
    CommaJoiner res;
    res.add("GRIB1");
    if (centre != -1)
        res.add(centre);
    else
        res.addUndef();
    if (subcentre != -1)
        res.add(subcentre);
    else
        res.addUndef();
    if (process != -1)
        res.add(process);
    else
        res.addUndef();
    return res.join();
}

MatchOriginGRIB2::MatchOriginGRIB2(int centre, int subcentre, int processtype,
                                   int bgprocessid, int processid)
    : centre(centre), subcentre(subcentre), processtype(processtype),
      bgprocessid(bgprocessid), processid(processid)
{
}

MatchOriginGRIB2::MatchOriginGRIB2(const std::string& pattern)
{
    OptionalCommaList args(pattern);
    centre      = args.getInt(0, -1);
    subcentre   = args.getInt(1, -1);
    processtype = args.getInt(2, -1);
    bgprocessid = args.getInt(3, -1);
    processid   = args.getInt(4, -1);
}

MatchOriginGRIB2* MatchOriginGRIB2::clone() const
{
    return new MatchOriginGRIB2(centre, subcentre, processtype, bgprocessid,
                                processid);
}

bool MatchOriginGRIB2::matchItem(const Type& o) const
{
    const types::Origin* v = dynamic_cast<const types::Origin*>(&o);
    if (!v)
        return false;
    if (v->style() != origin::Style::GRIB2)
        return false;
    unsigned v_centre, v_subcentre, v_processtype, v_bgprocessid, v_processid;
    v->get_GRIB2(v_centre, v_subcentre, v_processtype, v_bgprocessid,
                 v_processid);
    if (centre != -1 && (unsigned)centre != v_centre)
        return false;
    if (subcentre != -1 && (unsigned)subcentre != v_subcentre)
        return false;
    if (processtype != -1 && (unsigned)processtype != v_processtype)
        return false;
    if (bgprocessid != -1 && (unsigned)bgprocessid != v_bgprocessid)
        return false;
    if (processid != -1 && (unsigned)processid != v_processid)
        return false;
    return true;
}

bool MatchOriginGRIB2::match_buffer(types::Code code, const uint8_t* data,
                                    unsigned size) const
{
    if (code != TYPE_ORIGIN)
        return false;
    if (size < 1)
        return false;
    if (Origin::style(data, size) != origin::Style::GRIB2)
        return false;

    unsigned v_centre, v_subcentre, v_processtype, v_bgprocessid, v_processid;
    Origin::get_GRIB2(data, size, v_centre, v_subcentre, v_processtype,
                      v_bgprocessid, v_processid);
    if (centre != -1 && (unsigned)centre != v_centre)
        return false;
    if (subcentre != -1 && (unsigned)subcentre != v_subcentre)
        return false;
    if (processtype != -1 && (unsigned)processtype != v_processtype)
        return false;
    if (bgprocessid != -1 && (unsigned)bgprocessid != v_bgprocessid)
        return false;
    if (processid != -1 && (unsigned)processid != v_processid)
        return false;
    return true;
}

std::string MatchOriginGRIB2::toString() const
{
    CommaJoiner res;
    res.add("GRIB2");
    if (centre != -1)
        res.add(centre);
    else
        res.addUndef();
    if (subcentre != -1)
        res.add(subcentre);
    else
        res.addUndef();
    if (processtype != -1)
        res.add(processtype);
    else
        res.addUndef();
    if (bgprocessid != -1)
        res.add(bgprocessid);
    else
        res.addUndef();
    if (processid != -1)
        res.add(processid);
    else
        res.addUndef();
    return res.join();
}

MatchOriginBUFR::MatchOriginBUFR(int centre, int subcentre)
    : centre(centre), subcentre(subcentre)
{
}

MatchOriginBUFR::MatchOriginBUFR(const std::string& pattern)
{
    OptionalCommaList args(pattern);
    centre    = args.getInt(0, -1);
    subcentre = args.getInt(1, -1);
}

MatchOriginBUFR* MatchOriginBUFR::clone() const
{
    return new MatchOriginBUFR(centre, subcentre);
}

bool MatchOriginBUFR::matchItem(const Type& o) const
{
    const types::Origin* v = dynamic_cast<const types::Origin*>(&o);
    if (!v)
        return false;
    if (v->style() != origin::Style::BUFR)
        return false;
    unsigned v_centre, v_subcentre;
    v->get_BUFR(v_centre, v_subcentre);
    if (centre != -1 && (unsigned)centre != v_centre)
        return false;
    if (subcentre != -1 && (unsigned)subcentre != v_subcentre)
        return false;
    return true;
}

bool MatchOriginBUFR::match_buffer(types::Code code, const uint8_t* data,
                                   unsigned size) const
{
    if (code != TYPE_ORIGIN)
        return false;
    if (size < 1)
        return false;
    if (Origin::style(data, size) != origin::Style::BUFR)
        return false;

    unsigned v_centre, v_subcentre;
    Origin::get_BUFR(data, size, v_centre, v_subcentre);
    if (centre != -1 && (unsigned)centre != v_centre)
        return false;
    if (subcentre != -1 && (unsigned)subcentre != v_subcentre)
        return false;
    return true;
}

std::string MatchOriginBUFR::toString() const
{
    CommaJoiner res;
    res.add("BUFR");
    if (centre != -1)
        res.add(centre);
    else
        res.addUndef();
    if (subcentre != -1)
        res.add(subcentre);
    else
        res.addUndef();
    return res.join();
}

MatchOriginODIMH5::MatchOriginODIMH5(const std::string& WMO,
                                     const std::string& RAD,
                                     const std::string& PLC)
    : WMO(WMO), RAD(RAD), PLC(PLC)
{
}

MatchOriginODIMH5::MatchOriginODIMH5(const std::string& pattern)
{
    OptionalCommaList args(pattern);
    WMO = args.getString(0, "");
    RAD = args.getString(1, "");
    PLC = args.getString(2, "");
}

MatchOriginODIMH5* MatchOriginODIMH5::clone() const
{
    return new MatchOriginODIMH5(WMO, RAD, PLC);
}

bool MatchOriginODIMH5::matchItem(const Type& o) const
{
    const types::Origin* v = dynamic_cast<const types::Origin*>(&o);
    if (!v)
        return false;
    if (v->style() != origin::Style::ODIMH5)
        return false;
    std::string v_WMO, v_RAD, v_PLC;
    v->get_ODIMH5(v_WMO, v_RAD, v_PLC);
    if (WMO.size() && (WMO != v_WMO))
        return false;
    if (RAD.size() && (RAD != v_RAD))
        return false;
    if (PLC.size() && (PLC != v_PLC))
        return false;
    return true;
}

bool MatchOriginODIMH5::match_buffer(types::Code code, const uint8_t* data,
                                     unsigned size) const
{
    if (code != TYPE_ORIGIN)
        return false;
    if (size < 1)
        return false;
    if (Origin::style(data, size) != origin::Style::ODIMH5)
        return false;

    std::string v_WMO, v_RAD, v_PLC;
    Origin::get_ODIMH5(data, size, v_WMO, v_RAD, v_PLC);
    if (WMO.size() && (WMO != v_WMO))
        return false;
    if (RAD.size() && (RAD != v_RAD))
        return false;
    if (PLC.size() && (PLC != v_PLC))
        return false;
    return true;
}

std::string MatchOriginODIMH5::toString() const
{
    CommaJoiner res;
    res.add("ODIMH5");
    if (WMO.size())
        res.add(WMO);
    else
        res.addUndef();
    if (RAD.size())
        res.add(RAD);
    else
        res.addUndef();
    if (PLC.size())
        res.add(PLC);
    else
        res.addUndef();
    return res.join();
}

/*============================================================================*/

Implementation* MatchOrigin::parse(const std::string& pattern)
{
    size_t beg = 0;
    size_t pos = pattern.find(',', beg);
    string name;
    string rest;
    if (pos == string::npos)
        name = str::strip(pattern.substr(beg));
    else
    {
        name = str::strip(pattern.substr(beg, pos - beg));
        rest = pattern.substr(pos + 1);
    }
    switch (types::Origin::parseStyle(name))
    {
        case types::Origin::Style::GRIB1:  return new MatchOriginGRIB1(rest);
        case types::Origin::Style::GRIB2:  return new MatchOriginGRIB2(rest);
        case types::Origin::Style::BUFR:   return new MatchOriginBUFR(rest);
        case types::Origin::Style::ODIMH5: return new MatchOriginODIMH5(rest);
        default:
            throw std::invalid_argument("cannot parse type of origin to match: "
                                        "unsupported origin style: " +
                                        name);
    }
}

void MatchOrigin::init()
{
    MatcherType::register_matcher(
        "origin", TYPE_ORIGIN, (MatcherType::subexpr_parser)MatchOrigin::parse);
}

} // namespace matcher
} // namespace arki
