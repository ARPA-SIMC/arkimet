#include "arki/exceptions.h"
#include "arki/types/product.h"
#include "arki/types/utils.h"
#include "arki/core/binary.h"
#include "arki/utils/string.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/libconfig.h"
#include <sstream>
#include <iomanip>
#include <stdexcept>
#include <algorithm>

#ifdef HAVE_VM2
#include "arki/utils/vm2.h"
#endif

#define CODE TYPE_PRODUCT
#define TAG "product"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Product>::type_tag = TAG;
const types::Code traits<Product>::type_code = CODE;
const size_t traits<Product>::type_sersize_bytes = SERSIZELEN;

Product::Style Product::parseStyle(const std::string& str)
{
    if (str == "GRIB1") return Style::GRIB1;
    if (str == "GRIB2") return Style::GRIB2;
    if (str == "BUFR") return Style::BUFR;
    if (str == "ODIMH5") return Style::ODIMH5;
    if (str == "VM2") return Style::VM2;
    throw_consistency_error("parsing Product style", "cannot parse Product style '"+str+"': only GRIB1, GRIB2 and BUFR are supported");
}

std::string Product::formatStyle(Product::Style s)
{
    switch (s)
    {
        case Style::GRIB1: return "GRIB1";
        case Style::GRIB2: return "GRIB2";
        case Style::BUFR: return "BUFR";
        case Style::ODIMH5: return "ODIMH5";
        case Style::VM2: return "VM2";
        default: 
            std::stringstream str;
            str << "(unknown " << (int)s << ")";
            return str.str();
    }
}

product::Style Product::style(const uint8_t* data, unsigned size)
{
    return (product::Style)data[0];
}

void Product::get_GRIB1(const uint8_t* data, unsigned size, unsigned& origin, unsigned& table, unsigned& product)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    origin  = dec.pop_uint(1, "GRIB1 origin");
    table   = dec.pop_uint(1, "GRIB1 table");
    product = dec.pop_uint(1, "GRIB1 product");
}

void Product::get_GRIB2(const uint8_t* data, unsigned size, unsigned& centre, unsigned& discipline, unsigned& category, unsigned& number, unsigned& table_version, unsigned& local_table_version)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    centre    = dec.pop_uint(2, "GRIB2 centre");
    discipline = dec.pop_uint(1, "GRIB2 discipline");
    category   = dec.pop_uint(1, "GRIB2 category");
    number     = dec.pop_uint(1, "GRIB2 number");
    table_version = 4;
    if (dec) table_version = dec.pop_uint(1, "GRIB2 table version");
    local_table_version = 255;
    if (dec) local_table_version = dec.pop_uint(1, "GRIB2 local table version");
}

void Product::get_BUFR(const uint8_t* data, unsigned size, unsigned& type, unsigned& subtype, unsigned& localsubtype, ValueBag& values)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    type         = dec.pop_uint(1, "GRIB1 type");
    subtype      = dec.pop_uint(1, "GRIB1 subtype");
    localsubtype = dec.pop_uint(1, "GRIB1 localsubtype");
    values = ValueBag::decode(dec);
}

void Product::get_ODIMH5(const uint8_t* data, unsigned size, std::string& obj, std::string& prod)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    size_t len;
    len = dec.pop_varint<size_t>("ODIMH5 obj len");
    obj = dec.pop_string(len, "ODIMH5 obj");
    len = dec.pop_varint<size_t>("ODIMH5 product len");
    prod = dec.pop_string(len, "ODIMH5 product ");
}

void Product::get_VM2(const uint8_t* data, unsigned size, unsigned& variable_id)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    variable_id = dec.pop_uint(4, "VM2 variable id");
}

int Product::compare(const Type& o) const
{
    int res = Encoded::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const Product* v = dynamic_cast<const Product*>(&o);
    if (!v)
    {
        std::stringstream ss;
        ss << "cannot compare metadata types: second element claims to be `Product`, but it is `" << typeid(&o).name() << "' instead";
        throw std::runtime_error(ss.str());
    }

    auto sty = style();

    // Compare style
    if (int res = (int)sty - (int)v->style()) return res;

    // Styles are the same, compare the rest
    //
    // We can safely reinterpret_cast, avoiding an expensive dynamic_cast,
    // since we checked the style.
    switch (sty)
    {
        case product::Style::GRIB1:
            return reinterpret_cast<const product::GRIB1*>(this)->compare_local(
                    *reinterpret_cast<const product::GRIB1*>(v));
        case product::Style::GRIB2:
            return reinterpret_cast<const product::GRIB2*>(this)->compare_local(
                    *reinterpret_cast<const product::GRIB2*>(v));
        case product::Style::BUFR:
            return reinterpret_cast<const product::BUFR*>(this)->compare_local(
                    *reinterpret_cast<const product::BUFR*>(v));
        case product::Style::ODIMH5:
            return reinterpret_cast<const product::ODIMH5*>(this)->compare_local(
                    *reinterpret_cast<const product::ODIMH5*>(v));
        case product::Style::VM2:
            return reinterpret_cast<const product::VM2*>(this)->compare_local(
                    *reinterpret_cast<const product::VM2*>(v));
        default:
            throw_consistency_error("parsing Product", "unknown Product style " + formatStyle(sty));
    }
}

unique_ptr<Product> Product::decode(core::BinaryDecoder& dec, bool reuse_buffer)
{
    dec.ensure_size(1, "Product style");
    Style sty = static_cast<product::Style>(dec.buf[0]);
    std::unique_ptr<Product> res;
    switch (sty)
    {
        case product::Style::GRIB1:
            dec.ensure_size(4, "GRIB1 data");
            if (reuse_buffer)
                res.reset(new product::GRIB1(dec.buf, dec.size, false));
            else
                res.reset(new product::GRIB1(dec.buf, dec.size));
            dec.skip(dec.size);
            break;
        case product::Style::GRIB2:
            dec.ensure_size(6, "GRIB2 data");
            if (reuse_buffer)
                res.reset(new product::GRIB2(dec.buf, dec.size, false));
            else
                res.reset(new product::GRIB2(dec.buf, dec.size));
            dec.skip(dec.size);
            break;
        case product::Style::BUFR:
            dec.ensure_size(4, "BUFR data");
            if (reuse_buffer)
                res.reset(new product::BUFR(dec.buf, dec.size, false));
            else
                res.reset(new product::BUFR(dec.buf, dec.size));
            dec.skip(dec.size);
            break;
        case product::Style::ODIMH5:
            dec.ensure_size(4, "ODIMH5 data");
            if (reuse_buffer)
                res.reset(new product::ODIMH5(dec.buf, dec.size, false));
            else
                res.reset(new product::ODIMH5(dec.buf, dec.size));
            dec.skip(dec.size);
            break;
        case product::Style::VM2:
            dec.ensure_size(5, "VM2 data");
            if (reuse_buffer)
                res.reset(new product::VM2(dec.buf, dec.size, false));
            else
                res.reset(new product::VM2(dec.buf, dec.size));
            dec.skip(dec.size);
            break;
        default:
            throw_consistency_error("parsing Timerange", "unknown Timerange style " + formatStyle(sty));
    }
    return res;
}

/*
static double parseDouble(const std::string& val)
{
	double result;
	std::istringstream ss(val);
	ss >> result;
	return result;
}
*/

unique_ptr<Product> Product::decodeString(const std::string& val)
{
    std::string inner;
    product::Style style = outerParse<Product>(val, inner);
    switch (style)
    {
        //case Product::NONE: return Product();
        case Style::GRIB1: {
            NumberList<3> nums(inner, "Product");
            return createGRIB1(nums.vals[0], nums.vals[1], nums.vals[2]);
        }
        case Style::GRIB2: {
            NumberList<6, 4> nums(inner, "Product", true);
            unsigned char table_version = nums.found > 4 ? nums.vals[4] : 4;
            unsigned char local_table_version = nums.found > 5 ? nums.vals[5] : 255;
            return createGRIB2(nums.vals[0], nums.vals[1], nums.vals[2], nums.vals[3],
                    table_version, local_table_version);
        }
        case Style::BUFR: {
            NumberList<3> nums(inner, "Product", true);
            inner = str::strip(nums.tail);
            if (!inner.empty() && inner[0] == ',')
                inner = str::strip(nums.tail.substr(1));
            return createBUFR(nums.vals[0], nums.vals[1], nums.vals[2], ValueBag::parse(inner));
        }
        case Style::ODIMH5: {
            std::vector<std::string> values;
            split(inner, values, ",");

            if (values.size() != 2)
                throw std::logic_error("OdimH5 product has not enogh values");

            std::string o   = str::strip(values[0]);
            std::string p   = str::strip(values[1]);
            /*REMOVED: double       p1  = parseDouble(values[2]); */
            /*REMOVED: double       p2  = parseDouble(values[3]); */

            return createODIMH5(o, p);
        }
        case Style::VM2: {
            const char* innerptr = inner.c_str();
            char* endptr;
            unsigned long variable_id = strtoul(innerptr, &endptr, 10); 
            if (innerptr == endptr)
                throw std::runtime_error("cannot parse " + inner + ": expected a number, but found \"" + inner +"\"");
            return createVM2(variable_id);
        }
        default:
            throw_consistency_error("parsing Product", "unknown Product style " + formatStyle(style));
    }
}

std::unique_ptr<Product> Product::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    product::Style sty = parseStyle(val.as_string(keys.type_style, "type style"));
    switch (sty)
    {
        case Style::GRIB1:
            return createGRIB1(
                    val.as_int(keys.product_origin, "product origin"),
                    val.as_int(keys.product_table, "product table"),
                    val.as_int(keys.product_product, "product product"));
        case Style::GRIB2:
        {
            int tv = 4;
            int ltv = 255;
            if (val.has_key(keys.product_table_version, structured::NodeType::INT))
                tv = val.as_int(keys.product_table_version, "product table version");
            if (val.has_key(keys.product_local_table_version, structured::NodeType::INT))
                ltv = val.as_int(keys.product_local_table_version, "product local table version");
            return createGRIB2(
                    val.as_int(keys.product_centre, "product centre"),
                    val.as_int(keys.product_discipline, "product discipline"),
                    val.as_int(keys.product_category, "product category"),
                    val.as_int(keys.product_number, "product number"),
                    tv, ltv);
        }
        case Style::BUFR:
            if (val.has_key(keys.product_value, structured::NodeType::MAPPING))
            {
                ValueBag values;
                val.sub(keys.product_value, "product value", [&](const structured::Reader& value) {
                    values = ValueBag::parse(value);
                });
                return createBUFR(
                        val.as_int(keys.product_type, "product type"),
                        val.as_int(keys.product_subtype, "product subtype"),
                        val.as_int(keys.product_local_subtype, "product localsubtype"),
                        values);
            } else {
                return createBUFR(
                        val.as_int(keys.product_type, "product type"),
                        val.as_int(keys.product_subtype, "product subtype"),
                        val.as_int(keys.product_local_subtype, "product localsubtype"));
            }
        case Style::ODIMH5:
            return createODIMH5(
                    val.as_string(keys.product_object, "product object"),
                    val.as_string(keys.product_product, "product name"));
        case Style::VM2:
            return createVM2(val.as_int(keys.product_id, "product id"));
        default:
            throw std::runtime_error("Unknown Product style");
    }
}

std::unique_ptr<Product> Product::createGRIB1(unsigned char origin, unsigned char table, unsigned char product)
{
    uint8_t* buf = new uint8_t[4];
    buf[0] = (uint8_t)product::Style::GRIB1;
    buf[1] = origin;
    buf[2] = table;
    buf[3] = product;
    return std::unique_ptr<Product>(new product::GRIB1(buf, 4, true));
}

std::unique_ptr<Product> Product::createGRIB2(
        unsigned short centre,
        unsigned char discipline,
        unsigned char category,
        unsigned char number,
        unsigned char table_version,
        unsigned char local_table_version)
{
    unsigned size = 6;
    if (table_version != 4 || local_table_version != 255)
    {
        ++size;
        if (local_table_version != 255)
            ++size;
    }
    uint8_t* buf = new uint8_t[size];
    buf[0] = (uint8_t)product::Style::GRIB2;
    core::BinaryEncoder::set_unsigned(buf + 1, centre, 2);
    buf[3] = discipline;
    buf[4] = category;
    buf[5] = number;
    if (size > 6)
    {
        buf[6] = table_version;
        if (size > 7)
            buf[7] = local_table_version;
    }
    return std::unique_ptr<Product>(new product::GRIB2(buf, size, true));
}

std::unique_ptr<Product> Product::createBUFR(unsigned char type, unsigned char subtype, unsigned char localsubtype)
{
    uint8_t* buf = new uint8_t[4];
    buf[0] = (uint8_t)product::Style::BUFR;
    buf[1] = type;
    buf[2] = subtype;
    buf[3] = localsubtype;
    return std::unique_ptr<Product>(new product::BUFR(buf, 4, true));
}

std::unique_ptr<Product> Product::createBUFR(unsigned char type, unsigned char subtype, unsigned char localsubtype, const ValueBag& name)
{
    // TODO: optimize encoding by precomputing buffer size and not using a vector?
    // to do that, we first need a function that estimates the size of the ValueBag
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_unsigned((unsigned)product::Style::BUFR, 1);
    enc.add_unsigned(type, 1);
    enc.add_unsigned(subtype, 1);
    enc.add_unsigned(localsubtype, 1);
    name.encode(enc);
    return std::unique_ptr<Product>(new product::BUFR(buf));
}

std::unique_ptr<Product> Product::createODIMH5(const std::string& obj, const std::string& prod)
{
    // TODO: optimize encoding by precomputing buffer size and not using a vector?
    // to do that, we first need a function that estimates the size of the varints
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_unsigned((unsigned)product::Style::ODIMH5, 1);
    enc.add_varint(obj.size());
    enc.add_raw(obj);
    enc.add_varint(prod.size());
    enc.add_raw(prod);
    return std::unique_ptr<Product>(new product::ODIMH5(buf));
}

std::unique_ptr<Product> Product::createVM2(unsigned variable_id)
{
    uint8_t* buf = new uint8_t[5];
    buf[0] = (uint8_t)product::Style::VM2;
    core::BinaryEncoder::set_unsigned(buf + 1, variable_id, 4);
    return std::unique_ptr<Product>(new product::VM2(buf, 5, true));
}


void Product::init()
{
    MetadataType::register_type<Product>();
}

namespace product {

/*
 * GRIB1
 */

int GRIB1::compare_local(const GRIB1& v) const
{
    // TODO: can probably do lexicographical compare of raw data here?
    unsigned o, t, p, vo, vt, vp;
    get_GRIB1(o, t, p);
    v.get_GRIB1(vo, vt, vp);
    if (int res = o - vo) return res;
    if (int res = t - vt) return res;
    return p - vp;
}

std::ostream& GRIB1::writeToOstream(std::ostream& o) const
{
    unsigned ori, tab, pro;
    get_GRIB1(ori, tab, pro);
    return o << formatStyle(Style::GRIB1) << "("
             << setfill('0')
             << setw(3) << ori << ", "
             << setw(3) << tab << ", "
             << setw(3) << pro
             << setfill(' ')
             << ")";
}

void GRIB1::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.add(keys.type_style, formatStyle(Style::GRIB1));
    unsigned o, t, p;
    get_GRIB1(o, t, p);
    e.add(keys.product_origin, o);
    e.add(keys.product_table, t);
    e.add(keys.product_product, p);
}

std::string GRIB1::exactQuery() const
{
    unsigned o, t, p;
    get_GRIB1(o, t, p);

    char buf[128];
    snprintf(buf, 128, "GRIB1,%u,%u,%u", o, t, p);
    return buf;
}


/*
 * GRIB2
 */

int GRIB2::compare_local(const GRIB2& o) const
{
    // TODO: can probably do lexicographical compare of raw data here?
    unsigned ce, di, ca, nu, ta, lo;
    unsigned vce, vdi, vca, vnu, vta, vlo;
    get_GRIB2(ce, di, ca, nu, ta, lo);
    o.get_GRIB2(vce, vdi, vca, vnu, vta, vlo);
    if (int res = ce - vce) return res;
    if (int res = di - vdi) return res;
    if (int res = ca - vca) return res;
    if (int res = nu - vnu) return res;
    if (int res = ta - vta) return res;
    return lo - vlo;
}

std::ostream& GRIB2::writeToOstream(std::ostream& o) const
{
    unsigned ce, di, ca, nu, ta, lo;
    get_GRIB2(ce, di, ca, nu, ta, lo);
    o << formatStyle(Style::GRIB2) << "("
      << setfill('0')
      << setw(5) << ce << ", "
      << setw(3) << di << ", "
      << setw(3) << ca << ", "
      << setw(3) << nu;
    if (ta != 4 || ta != 255)
    {
        o << ", " << setw(3) << ta;
        if (ta != 255)
            o << ", " << setw(3) << lo;
    }
    o << setfill(' ')
      << ")";
    return o;
}

void GRIB2::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.add(keys.type_style, formatStyle(Style::GRIB2));
    unsigned ce, di, ca, nu, ta, lo;
    get_GRIB2(ce, di, ca, nu, ta, lo);
    e.add(keys.product_centre, ce);
    e.add(keys.product_discipline, di);
    e.add(keys.product_category, ca);
    e.add(keys.product_number, nu);
    e.add(keys.product_table_version, ta);
    e.add(keys.product_local_table_version, lo);
}

std::string GRIB2::exactQuery() const
{
    unsigned ce, di, ca, nu, ta, lo;
    get_GRIB2(ce, di, ca, nu, ta, lo);

    std::stringstream ss;
    ss << "GRIB2," << ce << "," << di << "," << ca << "," << nu;
    if (ta != 4 || lo != 255)
    {
        ss << "," << ta;
        if (lo != 255)
            ss << "," << lo;
    }
    return ss.str();
}


/*
 * BUFR
 */

int BUFR::compare_local(const BUFR& o) const
{
    // TODO: can probably do lexicographical compare of raw data here?
    unsigned ty, su, lo, vty, vsu, vlo;
    ValueBag va, vva;
    get_BUFR(ty, su, lo, va);
    o.get_BUFR(vty, vsu, vlo, vva);
    if (int res = ty - vty) return res;
    if (int res = su - vsu) return res;
    if (int res = lo - vlo) return res;
    return va.compare(vva);
}

std::ostream& BUFR::writeToOstream(std::ostream& o) const
{
    unsigned ty, su, lo;
    ValueBag va;
    get_BUFR(ty, su, lo, va);
    o << formatStyle(Style::BUFR) << "("
      << setfill('0')
      << setw(3) << ty << ", "
      << setw(3) << su << ", "
      << setw(3) << lo
      << setfill(' ');
    if (va.empty())
        o << ")";
    else
        o << ", " << va << ")";
    return o;
}

void BUFR::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.add(keys.type_style, formatStyle(Style::BUFR));
    unsigned ty, su, lo;
    ValueBag va;
    get_BUFR(ty, su, lo, va);
    e.add(keys.product_type, ty);
    e.add(keys.product_subtype, su);
    e.add(keys.product_local_subtype, lo);
    if (!va.empty())
    {
        e.add(keys.product_value);
        va.serialise(e);
    }
}

std::string BUFR::exactQuery() const
{
    unsigned ty, su, lo;
    ValueBag va;
    get_BUFR(ty, su, lo, va);

    std::stringstream ss;
    ss << "BUFR," << ty << "," << su << "," << lo;
    if (!va.empty())
        ss << ":" << va.toString();
    return ss.str();
}


/*
 * ODIMH5
 */

int ODIMH5::compare_local(const ODIMH5& o) const
{
    std::string ob, pr, vob, vpr;
    get_ODIMH5(ob, pr);
    o.get_ODIMH5(vob, vpr);
    if (int res = ob.compare(vob)) return res;
    if (int res = pr.compare(vpr)) return res;
    return 0;
}

std::ostream& ODIMH5::writeToOstream(std::ostream& o) const
{
    std::string ob, pr;
    get_ODIMH5(ob, pr);
    return o << formatStyle(Style::ODIMH5) << "("
            << ob << ", "
            << pr
            << ")";
}

void ODIMH5::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.add(keys.type_style, formatStyle(Style::ODIMH5));
    std::string ob, pr;
    get_ODIMH5(ob, pr);
    e.add(keys.product_object, ob);
    e.add(keys.product_product, pr);
}

std::string ODIMH5::exactQuery() const
{
    std::string ob, pr;
    get_ODIMH5(ob, pr);

    std::ostringstream ss;
    ss << formatStyle(style()) << ","
       << ob << ","
       << pr;
    return ss.str();
}


/*
 * VM2
 */

bool VM2::equals(const Type& o) const
{
    const VM2* v = dynamic_cast<const VM2*>(&o);
    if (!v) return false;
    if (size < 5 || v->size < 5) return size != v->size;
    // Compare only first 5 bytes, ignoring optional derived values appended to
    // the encoded data
    return memcmp(data, v->data, std::min(size, 5u)) == 0;
}

int VM2::compare_local(const VM2& o) const
{
    unsigned vi, vvi;
    get_VM2(vi);
    o.get_VM2(vvi);
    if (vi == vvi) return 0;
    return (vi > vvi ? 1 : -1);
}

std::ostream& VM2::writeToOstream(std::ostream& o) const
{
    unsigned vi;
    get_VM2(vi);
    o << formatStyle(Style::VM2) << "(" << vi;
    auto dv = derived_values();
    if (!dv.empty())
        o << ", " << dv.toString();
    return o << ")";
}

void VM2::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.add(keys.type_style, formatStyle(Style::VM2));
    unsigned vi;
    get_VM2(vi);
    e.add(keys.product_id, vi);
    auto dv = derived_values();
    if (!dv.empty()) {
        e.add(keys.product_value);
        dv.serialise(e);
    }
}

std::string VM2::exactQuery() const
{
    unsigned vi;
    get_VM2(vi);

    std::stringstream ss;
    ss << "VM2," << vi;
    auto dv = derived_values();
    if (!dv.empty())
        ss << ":" << dv.toString();
    return ss.str();
}

void VM2::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    enc.add_raw(data, size);

    // Also add derived values to the binary representation, since we are
    // encoding for transmission
    if (size <= 5)
    {
        auto dv = derived_values();
        if (!dv.empty())
            dv.encode(enc);
    }
}

void VM2::encode_for_indexing(core::BinaryEncoder& enc) const
{
    enc.add_raw(data, min(size, 5u));
}

ValueBag VM2::derived_values() const
{
#ifdef HAVE_VM2
    if (size > 5u)
    {
        core::BinaryDecoder dec(data + 5, size - 5);
        return ValueBag::decode(dec);
    } else {
        unsigned variable_id;
        get_VM2(variable_id);
        return utils::vm2::get_variable(variable_id);
    }
#else
    return ValueBag();
#endif
}


}

}
}
