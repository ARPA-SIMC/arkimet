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

int Origin::compare(const Type& o) const
{
    int res = Encoded::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const Origin* v = dynamic_cast<const Origin*>(&o);
    if (!v)
    {
        std::stringstream ss;
        ss << "cannot compare metadata types: second element claims to be `Origin`, but it is `" << typeid(&o).name() << "' instead";
        throw std::runtime_error(ss.str());
    }

    auto sty = style();

    // Compare style
    if (int res = (int)sty - (int)v->style()) return res;

    // Styles are the same, compare the rest
    switch (sty)
    {
        case origin::Style::GRIB1:
        {
            // TODO: can probably do lexicographical compare of raw data here?
            unsigned c, s, p, vc, vs, vp;
            get_GRIB1(c, s, p);
            v->get_GRIB1(vc, vs, vp);
            if (int res = c - vc) return res;
            if (int res = s - vs) return res;
            return p - vp;
        }
        case origin::Style::GRIB2:
        {
            // TODO: can probably do lexicographical compare of raw data here?
            unsigned ce, su, pt, bg, pi, vce, vsu, vpt, vbg, vpi;
            get_GRIB2(ce, su, pt, bg, pi);
            v->get_GRIB2(vce, vsu, vpt, vbg, vpi);
            if (int res = ce - vce) return res;
            if (int res = su - vsu) return res;
            if (int res = pt - vpt) return res;
            if (int res = bg - vbg) return res;
            return pi - vpi;
        }
        case origin::Style::BUFR:
        {
            // TODO: can probably do lexicographical compare of raw data here?
            unsigned c, s, vc, vs;
            get_BUFR(c, s);
            v->get_BUFR(vc, vs);
            if (int res = c - vc) return res;
            return s - vs;
        }
        case origin::Style::ODIMH5:
        {
            std::string WMO, RAD, PLC, vWMO, vRAD, vPLC;
            get_ODIMH5(WMO, RAD, PLC);
            v->get_ODIMH5(vWMO, vRAD, vPLC);
            if (int res = WMO.compare(vWMO)) return res;
            if (int res = RAD.compare(vRAD)) return res;
            return PLC.compare(vPLC);
        }
        default:
            throw_consistency_error("parsing Origin", "unknown Origin style " + formatStyle(sty));
    }
}

origin::Style Origin::style(const uint8_t* data, unsigned size)
{
    return (origin::Style)data[0];
}

void Origin::get_GRIB1(const uint8_t* data, unsigned size, unsigned& centre, unsigned& subcentre, unsigned& process)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    centre = dec.pop_uint(1, "GRIB1 origin centre");
    subcentre = dec.pop_uint(1, "GRIB1 origin subcentre");
    process = dec.pop_uint(1, "GRIB1 origin process");
}

void Origin::get_GRIB2(const uint8_t* data, unsigned size, unsigned& centre, unsigned& subcentre, unsigned& processtype, unsigned& bgprocessid, unsigned& processid)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    centre = dec.pop_uint(2, "GRIB2 origin centre");
    subcentre = dec.pop_uint(2, "GRIB2 origin subcentre");
    processtype = dec.pop_uint(1, "GRIB2 origin process type");
    bgprocessid = dec.pop_uint(1, "GRIB2 origin background process ID");
    processid = dec.pop_uint(1, "GRIB2 origin process ID");
}

void Origin::get_BUFR(const uint8_t* data, unsigned size, unsigned& centre, unsigned& subcentre)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    centre = dec.pop_uint(1, "BUFR origin centre");
    subcentre = dec.pop_uint(1, "BUFR origin subcentre");
}

void Origin::get_ODIMH5(const uint8_t* data, unsigned size, std::string& WMO, std::string& RAD, std::string& PLC)
{
    core::BinaryDecoder dec(data + 1, size - 1);

    uint16_t wmosize = dec.pop_varint<uint16_t>("ODIMH5 wmo length");
    WMO = dec.pop_string(wmosize, "ODIMH5 wmo");

    uint16_t radsize = dec.pop_varint<uint16_t>("ODIMH5 rad length");
    RAD = dec.pop_string(radsize, "ODIMH5 rad");

    uint16_t plcsize = dec.pop_varint<uint16_t>("ODIMH5 plc length");
    PLC = dec.pop_string(plcsize, "ODIMH5 plc");
}

origin::Style Origin::parseStyle(const std::string& str)
{
    if (str == "GRIB1") return origin::Style::GRIB1;
    if (str == "GRIB2") return origin::Style::GRIB2;
    if (str == "BUFR") return origin::Style::BUFR;
    if (str == "ODIMH5") return origin::Style::ODIMH5;
    throw_consistency_error("parsing Origin style", "cannot parse Origin style '"+str+"': only GRIB1, GRIB2 and BUFR are supported");
}

std::string Origin::formatStyle(origin::Style s)
{
    switch (s)
    {
        case origin::Style::GRIB1: return "GRIB1";
        case origin::Style::GRIB2: return "GRIB2";
        case origin::Style::BUFR: return "BUFR";
        case origin::Style::ODIMH5:     return "ODIMH5";
        default:
            std::stringstream str;
            str << "(unknown " << (int)s << ")";
            return str.str();
    }
}

std::ostream& Origin::writeToOstream(std::ostream& o) const
{
    auto sty = style();
    switch (sty)
    {
        case origin::Style::GRIB1:
        {
            unsigned c, s, p;
            get_GRIB1(c, s, p);
            return o << formatStyle(sty) << "("
                     << setfill('0')
                     << setw(3) << (int)c << ", "
                     << setw(3) << (int)s << ", "
                     << setw(3) << (int)p
                     << setfill(' ')
                     << ")";
        }
        case origin::Style::GRIB2:
        {
            unsigned ce, su, pt, bg, pi;
            get_GRIB2(ce, su, pt, bg, pi);
            return o << formatStyle(sty) << "("
                     << setfill('0')
                     << setw(5) << (int)ce << ", "
                     << setw(5) << (int)su << ", "
                     << setw(3) << (int)pt << ", "
                     << setw(3) << (int)bg << ", "
                     << setw(3) << (int)pi
                     << setfill(' ')
                     << ")";
        }
        case origin::Style::BUFR:
        {
            unsigned c, s;
            get_BUFR(c, s);
            return o << formatStyle(sty) << "("
                     << setfill('0')
                     << setw(3) << (int)c << ", "
                     << setw(3) << (int)s
                     << setfill(' ')
                     << ")";
        }
        case origin::Style::ODIMH5:
        {
            std::string WMO, RAD, PLC;
            get_ODIMH5(WMO, RAD, PLC);
            return o << formatStyle(sty) << "("
                     << WMO << ", "
                     << RAD << ", "
                     << PLC << ")";
        }
        default:
            throw_consistency_error("parsing Origin", "unknown Origin style " + formatStyle(sty));
    }
}

void Origin::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    auto sty = style();
    e.add(keys.type_style, formatStyle(sty));
    switch (sty)
    {
        case origin::Style::GRIB1:
        {
            unsigned c, s, p;
            get_GRIB1(c, s, p);
            e.add(keys.origin_centre, c);
            e.add(keys.origin_subcentre, s);
            e.add(keys.origin_process, p);
            break;
        }
        case origin::Style::GRIB2:
        {
            unsigned ce, su, pt, bg, pi;
            get_GRIB2(ce, su, pt, bg, pi);
            e.add(keys.origin_centre, ce);
            e.add(keys.origin_subcentre, su);
            e.add(keys.origin_process_type, pt);
            e.add(keys.origin_background_process_id, bg);
            e.add(keys.origin_process_id, pi);
            break;
        }
        case origin::Style::BUFR:
        {
            unsigned c, s;
            get_BUFR(c, s);
            e.add(keys.origin_centre, c);
            e.add(keys.origin_subcentre, s);
            break;
        }
        case origin::Style::ODIMH5:
        {
            std::string WMO, RAD, PLC;
            get_ODIMH5(WMO, RAD, PLC);
            e.add(keys.origin_wmo, WMO);
            e.add(keys.origin_rad, RAD);
            e.add(keys.origin_plc, PLC);
            break;
        }
        default:
            throw_consistency_error("parsing Origin", "unknown Origin style " + formatStyle(sty));
    }
}

std::unique_ptr<Origin> Origin::decode(core::BinaryDecoder& dec)
{
    dec.ensure_size(1, "Origin style");
    std::unique_ptr<Origin> res(new Origin(dec.buf, dec.size));
    dec.skip(dec.size);
    return res;
}

std::unique_ptr<Origin> Origin::decodeString(const std::string& val)
{
    std::string inner;
    origin::Style sty = outerParse<Origin>(val, inner);
    switch (sty)
    {
        case origin::Style::GRIB1: {
            NumberList<3> nums(inner, "Origin");
            return createGRIB1(nums.vals[0], nums.vals[1], nums.vals[2]);
        }
        case origin::Style::GRIB2: {
            NumberList<5> nums(inner, "Origin");
            return createGRIB2(nums.vals[0], nums.vals[1], nums.vals[2], nums.vals[3], nums.vals[4]);
        }
        case origin::Style::BUFR: {
            NumberList<2> nums(inner, "Origin");
            return createBUFR(nums.vals[0], nums.vals[1]);
        }
        case origin::Style::ODIMH5: {
            std::vector<std::string> values;
            str::Split split(inner, ",");
            for (str::Split::const_iterator i = split.begin(); i != split.end(); ++i)
                values.push_back(*i);

            if (values.size()!=3)
                throw std::logic_error("OdimH5 origin has not enough values");

            values[0] = str::strip(values[0]);
            values[1] = str::strip(values[1]);
            values[2] = str::strip(values[2]);

            return createODIMH5(values[0], values[1], values[2]);
        }
        default:
            throw_consistency_error("parsing Origin", "unknown Origin style " + formatStyle(sty));
    }
}

std::unique_ptr<Origin> Origin::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    origin::Style sty = parseStyle(val.as_string(keys.type_style, "type style"));
    switch (sty)
    {
        case origin::Style::GRIB1:
            return createGRIB1(
                    val.as_int(keys.origin_centre, "origin centre"),
                    val.as_int(keys.origin_subcentre, "origin subcentre"),
                    val.as_int(keys.origin_process, "origin process"));
        case origin::Style::GRIB2:
            return createGRIB2(
                    val.as_int(keys.origin_centre, "origin centre"),
                    val.as_int(keys.origin_subcentre, "origin subcentre"),
                    val.as_int(keys.origin_process_type, "origin process type"),
                    val.as_int(keys.origin_background_process_id, "origin bg process id"),
                    val.as_int(keys.origin_process_id, "origin process id"));
        case origin::Style::BUFR:
            return createBUFR(
                    val.as_int(keys.origin_centre, "origin centre"),
                    val.as_int(keys.origin_subcentre, "origin subcentre"));
        case origin::Style::ODIMH5:
            return createODIMH5(
                    val.as_string(keys.origin_wmo, "origin wmo"),
                    val.as_string(keys.origin_rad, "origin rad"),
                    val.as_string(keys.origin_plc, "origin plc"));
        default: throw std::runtime_error("Unknown Origin style");
    }
}

std::string Origin::exactQuery() const
{
    switch (style())
    {
        case origin::Style::GRIB1:
        {
            char buf[64];
            unsigned c, s, p;
            get_GRIB1(c, s, p);
            snprintf(buf, 64, "GRIB1,%d,%d,%d", (int)c, (int)s, (int)p);
            return buf;
        }
        case origin::Style::GRIB2:
        {
            unsigned ce, su, pt, bg, pi;
            get_GRIB2(ce, su, pt, bg, pi);
            char buf[64];
            snprintf(buf, 64, "GRIB2,%d,%d,%d,%d,%d", (int)ce, (int)su, (int)pt, (int)bg, (int)pi);
            return buf;
        }
        case origin::Style::BUFR:
        {
            unsigned c, s;
            get_BUFR(c, s);
            char buf[32];
            snprintf(buf, 32, "BUFR,%d,%d", (int)c, (int)s);
            return buf;
        }
        case origin::Style::ODIMH5:
        {
            std::string WMO, RAD, PLC;
            get_ODIMH5(WMO, RAD, PLC);
            std::stringstream res;
            res << "ODIMH5," << WMO << "," << RAD << "," << PLC;
            return res.str();
        }
        default:
            throw_consistency_error("parsing Origin", "unknown Origin style " + formatStyle(style()));
    }
}

std::unique_ptr<Origin> Origin::createGRIB1(unsigned char centre, unsigned char subcentre, unsigned char process)
{
    uint8_t* buf = new uint8_t[4];
    buf[0] = (uint8_t)origin::Style::GRIB1;
    buf[1] = centre;
    buf[2] = subcentre;
    buf[3] = process;
    return std::unique_ptr<Origin>(new Origin(buf, 4));
}

std::unique_ptr<Origin> Origin::createGRIB2(unsigned short centre, unsigned short subcentre,
                                            unsigned char processtype, unsigned char bgprocessid, unsigned char processid)
{
    uint8_t* buf = new uint8_t[8];
    buf[0] = (uint8_t)origin::Style::GRIB2;
    core::BinaryEncoder::set_unsigned(buf + 1, centre, 2);
    core::BinaryEncoder::set_unsigned(buf + 3, subcentre, 2);
    buf[5] = processtype;
    buf[6] = bgprocessid;
    buf[7] = processid;
    return std::unique_ptr<Origin>(new Origin(buf, 8));
}

std::unique_ptr<Origin> Origin::createBUFR(unsigned char centre, unsigned char subcentre)
{
    uint8_t* buf = new uint8_t[3];
    buf[0] = (uint8_t)origin::Style::BUFR;
    buf[1] = centre;
    buf[2] = subcentre;
    return std::unique_ptr<Origin>(new Origin(buf, 3));
}

std::unique_ptr<Origin> Origin::createODIMH5(const std::string& wmo, const std::string& rad, const std::string& plc)
{
    // TODO: optimize encoding by precomputing buffer size and not using a vector?
    // to do that, we first need a function that estimates the size of the varints
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_unsigned((unsigned)origin::Style::ODIMH5, 1);
    enc.add_varint(wmo.size());
    enc.add_raw(wmo);
    enc.add_varint(rad.size());
    enc.add_raw(rad);
    enc.add_varint(plc.size());
    enc.add_raw(plc);
    return std::unique_ptr<Origin>(new Origin(buf));
}

void Origin::init()
{
    MetadataType::register_type<Origin>();
}

}
}
