#include "arki/exceptions.h"
#include "arki/types/origin.h"
#include "arki/types/utils.h"
#include "arki/core/binary.h"
#include "arki/utils/string.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include <iomanip>
#include <sstream>
#include <cstring>
#include <stdexcept>

#define CODE TYPE_ORIGIN
#define TAG "origin"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Origin>::type_tag = TAG;
const types::Code traits<Origin>::type_code = CODE;
const size_t traits<Origin>::type_sersize_bytes = SERSIZELEN;

Origin::Style Origin::parseStyle(const std::string& str)
{
    if (str == "GRIB1") return Style::GRIB1;
    if (str == "GRIB2") return Style::GRIB2;
    if (str == "BUFR") return Style::BUFR;
    if (str == "ODIMH5") return Style::ODIMH5;
    throw_consistency_error("parsing Origin style", "cannot parse Origin style '"+str+"': only GRIB1, GRIB2 and BUFR are supported");
}

std::string Origin::formatStyle(Origin::Style s)
{
    switch (s)
    {
        case Style::GRIB1: return "GRIB1";
        case Style::GRIB2: return "GRIB2";
        case Style::BUFR: return "BUFR";
        case Style::ODIMH5:     return "ODIMH5";
        default:
            std::stringstream str;
            str << "(unknown " << (int)s << ")";
            return str.str();
    }
}

unique_ptr<Origin> Origin::decode(core::BinaryDecoder& dec)
{
    Style s = (Style)dec.pop_uint(1, "origin style");
    switch (s)
    {
        case Style::GRIB1:
        {
            uint8_t ce = dec.pop_uint(1, "GRIB1 origin centre");
            uint8_t sc = dec.pop_uint(1, "GRIB1 origin subcentre");
            uint8_t pr = dec.pop_uint(1, "GRIB1 origin process");
            return upcast<Origin>(origin::GRIB1::create(ce, sc, pr));
        }
        case Style::GRIB2:
        {
            unsigned short ce = dec.pop_uint(2, "GRIB2 origin centre");
            unsigned short sc = dec.pop_uint(2, "GRIB2 origin subcentre");
            unsigned char pt = dec.pop_uint(1, "GRIB2 origin process type");
            unsigned char bgid = dec.pop_uint(1, "GRIB2 origin background process ID");
            unsigned char prid = dec.pop_uint(1, "GRIB2 origin process ID");
            return upcast<Origin>(origin::GRIB2::create(ce, sc, pt, bgid, prid));
        }
        case Style::BUFR:
        {
            uint8_t ce = dec.pop_uint(1, "BUFR origin centre");
            uint8_t sc = dec.pop_uint(1, "BUFR origin subcentre");
            return upcast<Origin>(origin::BUFR::create(ce, sc));
        }
        case Style::ODIMH5:
        {
            uint16_t wmosize = dec.pop_varint<uint16_t>("ODIMH5 wmo length");
            std::string wmo = dec.pop_string(wmosize, "ODIMH5 wmo");

            uint16_t radsize = dec.pop_varint<uint16_t>("ODIMH5 rad length");
            std::string rad = dec.pop_string(radsize, "ODIMH5 rad");

            uint16_t plcsize = dec.pop_varint<uint16_t>("ODIMH5 plc length");
            std::string plc = dec.pop_string(plcsize, "ODIMH5 plc");

            return upcast<Origin>(origin::ODIMH5::create(wmo, rad, plc));
        }
		default:
			throw_consistency_error("parsing Origin", "style is " + formatStyle(s) + " but we can only decode GRIB1, GRIB2 and BUFR");
	}
}

unique_ptr<Origin> Origin::decodeString(const std::string& val)
{
	string inner;
	Origin::Style style = outerParse<Origin>(val, inner);
	switch (style)
	{
        case Style::GRIB1: {
            NumberList<3> nums(inner, "Origin");
            return upcast<Origin>(origin::GRIB1::create(nums.vals[0], nums.vals[1], nums.vals[2]));
        }
        case Style::GRIB2: {
            NumberList<5> nums(inner, "Origin");
            return upcast<Origin>(origin::GRIB2::create(nums.vals[0], nums.vals[1], nums.vals[2], nums.vals[3], nums.vals[4]));
        }
        case Style::BUFR: {
            NumberList<2> nums(inner, "Origin");
            return upcast<Origin>(origin::BUFR::create(nums.vals[0], nums.vals[1]));
        }
        case Style::ODIMH5: {
            std::vector<std::string> values;
            str::Split split(inner, ",");
            for (str::Split::const_iterator i = split.begin(); i != split.end(); ++i)
                values.push_back(*i);

            if (values.size()!=3)
                throw std::logic_error("OdimH5 origin has not enough values");

            values[0] = str::strip(values[0]);
            values[1] = str::strip(values[1]);
            values[2] = str::strip(values[2]);

            return upcast<Origin>(origin::ODIMH5::create(values[0], values[1], values[2]));
        }
		default:
			throw_consistency_error("parsing Origin", "unknown Origin style " + formatStyle(style));
	}
}

std::unique_ptr<Origin> Origin::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    switch (style_from_structure(keys, val))
    {
        case Style::GRIB1: return upcast<Origin>(origin::GRIB1::decode_structure(keys, val));
        case Style::GRIB2: return upcast<Origin>(origin::GRIB2::decode_structure(keys, val));
        case Style::BUFR: return upcast<Origin>(origin::BUFR::decode_structure(keys, val));
        case Style::ODIMH5: return upcast<Origin>(origin::ODIMH5::decode_structure(keys, val));
        default: throw std::runtime_error("Unknown Origin style");
    }
}

unique_ptr<Origin> Origin::createGRIB1(unsigned char centre, unsigned char subcentre, unsigned char process)
{
    return upcast<Origin>(origin::GRIB1::create(centre, subcentre, process));
}
unique_ptr<Origin> Origin::createGRIB2(unsigned short centre, unsigned short subcentre,
                                             unsigned char processtype, unsigned char bgprocessid, unsigned char processid)
{
    return upcast<Origin>(origin::GRIB2::create(centre, subcentre, processtype, bgprocessid, processid));
}
unique_ptr<Origin> Origin::createBUFR(unsigned char centre, unsigned char subcentre)
{
    return upcast<Origin>(origin::BUFR::create(centre, subcentre));
}
unique_ptr<Origin> Origin::createODIMH5(const std::string& wmo, const std::string& rad, const std::string& plc)
{
    return upcast<Origin>(origin::ODIMH5::create(wmo, rad, plc));
}

namespace origin {

GRIB1::~GRIB1() { /* cache_grib1.uncache(this); */ }

Origin::Style GRIB1::style() const { return Style::GRIB1; }

void GRIB1::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Origin::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_centre, 1);
    enc.add_unsigned(m_subcentre, 1);
    enc.add_unsigned(m_process, 1);
}
std::ostream& GRIB1::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
		 << setfill('0')
	         << setw(3) << (int)m_centre << ", "
		 << setw(3) << (int)m_subcentre << ", "
		 << setw(3) << (int)m_process
		 << setfill(' ')
		 << ")";
}
void GRIB1::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Origin::serialise_local(e, keys, f);
    e.add(keys.origin_centre, m_centre);
    e.add(keys.origin_subcentre, m_subcentre);
    e.add(keys.origin_process, m_process);
}

std::unique_ptr<GRIB1> GRIB1::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return GRIB1::create(
            val.as_int(keys.origin_centre, "origin centre"),
            val.as_int(keys.origin_subcentre, "origin subcentre"),
            val.as_int(keys.origin_process, "origin process"));
}
std::string GRIB1::exactQuery() const
{
    char buf[64];
    snprintf(buf, 64, "GRIB1,%d,%d,%d", (int)m_centre, (int)m_subcentre, (int)m_process);
    return buf;
}

int GRIB1::compare_local(const Origin& o) const
{
    if (int res = Origin::compare_local(o)) return res;
	// We should be the same kind, so upcast
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v)
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a GRIB1 Origin, but is a ") + typeid(&o).name() + " instead");

	if (int res = m_centre - v->m_centre) return res;
	if (int res = m_subcentre - v->m_subcentre) return res;
	return m_process - v->m_process;
}

bool GRIB1::equals(const Type& o) const
{
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v) return false;
	return m_centre == v->m_centre && m_subcentre == v->m_subcentre && m_process == v->m_process;
}

GRIB1* GRIB1::clone() const
{
    GRIB1* res = new GRIB1;
    res->m_centre = m_centre;
    res->m_subcentre = m_subcentre;
    res->m_process = m_process;
    return res;
}

unique_ptr<GRIB1> GRIB1::create(unsigned char centre, unsigned char subcentre, unsigned char process)
{
    GRIB1* res = new GRIB1;
    res->m_centre = centre;
    res->m_subcentre = subcentre;
    res->m_process = process;
    return unique_ptr<GRIB1>(res);
}

GRIB2::~GRIB2() { /* cache_grib2.uncache(this); */ }

Origin::Style GRIB2::style() const { return Style::GRIB2; }

void GRIB2::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Origin::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_centre, 2);
    enc.add_unsigned(m_subcentre, 2);
    enc.add_unsigned(m_processtype, 1);
    enc.add_unsigned(m_bgprocessid, 1);
    enc.add_unsigned(m_processid, 1);
}
std::ostream& GRIB2::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
		 << setfill('0')
		 << setw(5) << (int)m_centre << ", "
		 << setw(5) << (int)m_subcentre << ", "
		 << setw(3) << (int)m_processtype << ", "
		 << setw(3) << (int)m_bgprocessid << ", "
		 << setw(3) << (int)m_processid
		 << setfill(' ')
		 << ")";
}
void GRIB2::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Origin::serialise_local(e, keys, f);
    e.add(keys.origin_centre, m_centre);
    e.add(keys.origin_subcentre, m_subcentre);
    e.add(keys.origin_process_type, m_processtype);
    e.add(keys.origin_background_process_id, m_bgprocessid);
    e.add(keys.origin_process_id, m_processid);
}

std::unique_ptr<GRIB2> GRIB2::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return GRIB2::create(
            val.as_int(keys.origin_centre, "origin centre"),
            val.as_int(keys.origin_subcentre, "origin subcentre"),
            val.as_int(keys.origin_process_type, "origin process type"),
            val.as_int(keys.origin_background_process_id, "origin bg process id"),
            val.as_int(keys.origin_process_id, "origin process id"));
}
std::string GRIB2::exactQuery() const
{
    char buf[64];
    snprintf(buf, 64, "GRIB2,%d,%d,%d,%d,%d", (int)m_centre, (int)m_subcentre, (int)m_processtype, (int)m_bgprocessid, (int)m_processid);
    return buf;
}

int GRIB2::compare_local(const Origin& o) const
{
    if (int res = Origin::compare_local(o)) return res;
	// We should be the same kind, so upcast
	const GRIB2* v = dynamic_cast<const GRIB2*>(&o);
	if (!v)
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a GRIB2 Origin, but is a ") + typeid(&o).name() + " instead");

	if (int res = m_centre - v->m_centre) return res;
	if (int res = m_subcentre - v->m_subcentre) return res;
	if (int res = m_processtype - v->m_processtype) return res;
	if (int res = m_bgprocessid - v->m_bgprocessid) return res;
	return m_processid - v->m_processid;
}
bool GRIB2::equals(const Type& o) const
{
	const GRIB2* v = dynamic_cast<const GRIB2*>(&o);
	if (!v) return false;
	return m_centre == v->m_centre && m_subcentre == v->m_subcentre 
		&& m_processtype == v->m_processtype && m_bgprocessid == v->m_bgprocessid
		&& m_processid == v->m_processid;
}

GRIB2* GRIB2::clone() const
{
    GRIB2* res = new GRIB2;
    res->m_centre = m_centre;
    res->m_subcentre = m_subcentre;
    res->m_processtype = m_processtype;
    res->m_bgprocessid = m_bgprocessid;
    res->m_processid = m_processid;
    return res;
}

unique_ptr<GRIB2> GRIB2::create(
			  unsigned short centre, unsigned short subcentre,
			  unsigned char processtype, unsigned char bgprocessid, unsigned char processid)
{
    GRIB2* res = new GRIB2;
    res->m_centre = centre;
    res->m_subcentre = subcentre;
    res->m_processtype = processtype;
    res->m_bgprocessid = bgprocessid;
    res->m_processid = processid;
    return unique_ptr<GRIB2>(res);
}

BUFR::~BUFR() { /* cache_bufr.uncache(this); */ }

Origin::Style BUFR::style() const { return Style::BUFR; }

void BUFR::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Origin::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_centre, 1);
    enc.add_unsigned(m_subcentre, 1);
}
std::ostream& BUFR::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
		 << setfill('0')
	         << setw(3) << (int)m_centre << ", "
		 << setw(3) << (int)m_subcentre
		 << setfill(' ')
		 << ")";
}
void BUFR::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Origin::serialise_local(e, keys, f);
    e.add(keys.origin_centre, m_centre);
    e.add(keys.origin_subcentre, m_subcentre);
}

std::unique_ptr<BUFR> BUFR::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return BUFR::create(
            val.as_int(keys.origin_centre, "origin centre"),
            val.as_int(keys.origin_subcentre, "origin subcentre"));
}
std::string BUFR::exactQuery() const
{
    char buf[32];
    snprintf(buf, 32, "BUFR,%d,%d", (int)m_centre, (int)m_subcentre);
    return buf;
}

int BUFR::compare_local(const Origin& o) const
{
    if (int res = Origin::compare_local(o)) return res;
	// We should be the same kind, so upcast
	// TODO: if upcast fails, we might still be ok as we support comparison
	// between origins of different style: we do need a two-phase upcast
	const BUFR* v = dynamic_cast<const BUFR*>(&o);
	if (!v)
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a BUFR Origin, but is a ") + typeid(&o).name() + " instead");

	if (int res = m_centre - v->m_centre) return res;
	return m_subcentre - v->m_subcentre;
}
bool BUFR::equals(const Type& o) const
{
	const BUFR* v = dynamic_cast<const BUFR*>(&o);
	if (!v) return false;
	return m_centre == v->m_centre && m_subcentre == v->m_subcentre;
}

BUFR* BUFR::clone() const
{
    BUFR* res = new BUFR;
    res->m_centre = m_centre;
    res->m_subcentre = m_subcentre;
    return res;
}

unique_ptr<BUFR> BUFR::create(unsigned char centre, unsigned char subcentre)
{
    BUFR* res = new BUFR;
    res->m_centre = centre;
    res->m_subcentre = subcentre;
    return unique_ptr<BUFR>(res);
}

ODIMH5::~ODIMH5() { /* cache_grib1.uncache(this); */ }

Origin::Style ODIMH5::style() const { return Style::ODIMH5; }

void ODIMH5::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Origin::encodeWithoutEnvelope(enc);
    enc.add_varint(m_WMO.size());
    enc.add_raw(m_WMO);
    enc.add_varint(m_RAD.size());
    enc.add_raw(m_RAD);
    enc.add_varint(m_PLC.size());
    enc.add_raw(m_PLC);
}
std::ostream& ODIMH5::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
	         << m_WMO << ", "
		 << m_RAD << ", "
		 << m_PLC << ")";
}
void ODIMH5::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Origin::serialise_local(e, keys, f);
    e.add(keys.origin_wmo, m_WMO);
    e.add(keys.origin_rad, m_RAD);
    e.add(keys.origin_plc, m_PLC);
}

std::unique_ptr<ODIMH5> ODIMH5::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return ODIMH5::create(
            val.as_string(keys.origin_wmo, "origin wmo"),
            val.as_string(keys.origin_rad, "origin rad"),
            val.as_string(keys.origin_plc, "origin plc"));
}
std::string ODIMH5::exactQuery() const
{
    stringstream res;
    res << "ODIMH5," << m_WMO << "," << m_RAD << "," << m_PLC;
    return res.str();
}

int ODIMH5::compare_local(const Origin& o) const
{
    if (int res = Origin::compare_local(o)) return res;
	// We should be the same kind, so upcast
	const ODIMH5* v = dynamic_cast<const ODIMH5*>(&o);
	if (!v)
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a GRIB1 Origin, but is a ") + typeid(&o).name() + " instead");

	if (int res = m_WMO.compare(v->m_WMO)) return res;
	if (int res = m_RAD.compare(v->m_RAD)) return res;
	return m_PLC.compare(v->m_PLC);
}

bool ODIMH5::equals(const Type& o) const
{
	const ODIMH5* v = dynamic_cast<const ODIMH5*>(&o);
	if (!v) return false;
	return m_WMO == v->m_WMO && m_RAD == v->m_RAD && m_PLC == v->m_PLC;
}

ODIMH5* ODIMH5::clone() const
{
    ODIMH5* res = new ODIMH5;
    res->m_WMO = m_WMO;
    res->m_RAD = m_RAD;
    res->m_PLC = m_PLC;
    return res;
}

unique_ptr<ODIMH5> ODIMH5::create(const std::string& wmo, const std::string& rad, const std::string& plc)
{
    ODIMH5* res = new ODIMH5;
    res->m_WMO = wmo;
    res->m_RAD = rad;
    res->m_PLC = plc;
    return unique_ptr<ODIMH5>(res);
}

}

void Origin::init()
{
    MetadataType::register_type<Origin>();
}

}
}

#include <arki/types/styled.tcc>
