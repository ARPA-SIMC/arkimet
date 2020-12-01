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

Product* Product::clone() const
{
    return new Product(data, size);
}

product::Style Product::style() const
{
    return (product::Style)data[0];
}

void Product::get_GRIB1(unsigned& origin, unsigned& table, unsigned& product) const
{
    core::BinaryDecoder dec(data + 1, size - 1);
    origin  = dec.pop_uint(1, "GRIB1 origin");
    table   = dec.pop_uint(1, "GRIB1 table");
    product = dec.pop_uint(1, "GRIB1 product");
}

void Product::get_GRIB2(unsigned& centre, unsigned& discipline, unsigned& category, unsigned& number, unsigned& table_version, unsigned& local_table_version) const
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

void Product::get_BUFR(unsigned& type, unsigned& subtype, unsigned& localsubtype, ValueBag& values) const
{
    core::BinaryDecoder dec(data + 1, size - 1);
    type         = dec.pop_uint(1, "GRIB1 type");
    subtype      = dec.pop_uint(1, "GRIB1 subtype");
    localsubtype = dec.pop_uint(1, "GRIB1 localsubtype");
    values = ValueBag::decode(dec);
}

void Product::get_ODIMH5(std::string& obj, std::string& prod) const
{
    core::BinaryDecoder dec(data + 1, size - 1);
    size_t len;
    len = dec.pop_varint<size_t>("ODIMH5 obj len");
    obj = dec.pop_string(len, "ODIMH5 obj");
    len = dec.pop_varint<size_t>("ODIMH5 product len");
    prod = dec.pop_string(len, "ODIMH5 product ");
}

void Product::get_VM2(unsigned& variable_id) const
{
    core::BinaryDecoder dec(data + 1, size - 1);
    variable_id = dec.pop_uint(4, "VM2 variable id");
}

ValueBag Product::get_VM2_derived_values(unsigned variable_id)
{
#ifdef HAVE_VM2
    return utils::vm2::get_variable(variable_id);
#else
    return ValueBag();
#endif
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
    switch (sty)
    {
        case product::Style::GRIB1:
        {
            // TODO: can probably do lexicographical compare of raw data here?
            unsigned o, t, p, vo, vt, vp;
            get_GRIB1(o, t, p);
            v->get_GRIB1(vo, vt, vp);
            if (int res = o - vo) return res;
            if (int res = t - vt) return res;
            return p - vp;
        }
        case product::Style::GRIB2:
        {
            // TODO: can probably do lexicographical compare of raw data here?
            unsigned ce, di, ca, nu, ta, lo;
            unsigned vce, vdi, vca, vnu, vta, vlo;
            get_GRIB2(ce, di, ca, nu, ta, lo);
            v->get_GRIB2(vce, vdi, vca, vnu, vta, vlo);
            if (int res = ce - vce) return res;
            if (int res = di - vdi) return res;
            if (int res = ca - vca) return res;
            if (int res = nu - vnu) return res;
            if (int res = ta - vta) return res;
            return lo - vlo;
        }
        case product::Style::BUFR:
        {
            // TODO: can probably do lexicographical compare of raw data here?
            unsigned ty, su, lo, vty, vsu, vlo;
            ValueBag va, vva;
            get_BUFR(ty, su, lo, va);
            v->get_BUFR(vty, vsu, vlo, vva);
            if (int res = ty - vty) return res;
            if (int res = su - vsu) return res;
            if (int res = lo - vlo) return res;
            return va.compare(vva);
        }
        case product::Style::ODIMH5:
        {
            std::string ob, pr, vob, vpr;
            get_ODIMH5(ob, pr);
            v->get_ODIMH5(vob, vpr);
            if (int res = ob.compare(vob)) return res;
            if (int res = pr.compare(vpr)) return res;
            return 0;
        }
        case product::Style::VM2:
        {
            unsigned vi, vvi;
            get_VM2(vi);
            v->get_VM2(vvi);
            if (vi == vvi) return 0;
            return (vi > vvi ? 1 : -1);
        }
        default:
            throw_consistency_error("parsing Product", "unknown Product style " + formatStyle(sty));
    }
}

std::ostream& Product::writeToOstream(std::ostream& o) const
{
    auto sty = style();
    switch (sty)
    {
        case product::Style::GRIB1:
        {
            unsigned ori, tab, pro;
            get_GRIB1(ori, tab, pro);
            return o << formatStyle(sty) << "("
                     << setfill('0')
                     << setw(3) << ori << ", "
                     << setw(3) << tab << ", "
                     << setw(3) << pro
                     << setfill(' ')
                     << ")";
        }
        case product::Style::GRIB2:
        {
            unsigned ce, di, ca, nu, ta, lo;
            get_GRIB2(ce, di, ca, nu, ta, lo);
            o << formatStyle(sty) << "("
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
        case product::Style::BUFR:
        {
            unsigned ty, su, lo;
            ValueBag va;
            get_BUFR(ty, su, lo, va);
            o << formatStyle(sty) << "("
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
        case product::Style::ODIMH5:
        {
            std::string ob, pr;
            get_ODIMH5(ob, pr);
            return o << formatStyle(sty) << "("
                    << ob << ", "
                    << pr
                    << ")";
        }
        case product::Style::VM2:
        {
            unsigned vi;
            get_VM2(vi);
            o << formatStyle(sty) << "(" << vi;
            auto dv = get_VM2_derived_values(vi);
            if (!dv.empty())
                o << ", " << dv.toString();
            return o << ")";
        }
        default:
            throw_consistency_error("parsing Product", "unknown Product style " + formatStyle(sty));
    }
}

void Product::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    auto sty = style();
    e.add(keys.type_style, formatStyle(sty));
    switch (sty)
    {
        case product::Style::GRIB1:
        {
            unsigned o, t, p;
            get_GRIB1(o, t, p);
            e.add(keys.product_origin, o);
            e.add(keys.product_table, t);
            e.add(keys.product_product, p);
            break;
        }
        case product::Style::GRIB2:
        {
            unsigned ce, di, ca, nu, ta, lo;
            get_GRIB2(ce, di, ca, nu, ta, lo);
            e.add(keys.product_centre, ce);
            e.add(keys.product_discipline, di);
            e.add(keys.product_category, ca);
            e.add(keys.product_number, nu);
            e.add(keys.product_table_version, ta);
            e.add(keys.product_local_table_version, lo);
            break;
        }
        case product::Style::BUFR:
        {
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
            break;
        }
        case product::Style::ODIMH5:
        {
            std::string ob, pr;
            get_ODIMH5(ob, pr);
            e.add(keys.product_object, ob);
            e.add(keys.product_product, pr);
            break;
        }
        case product::Style::VM2:
        {
            unsigned vi;
            get_VM2(vi);
            e.add(keys.product_id, vi);
            auto dv = get_VM2_derived_values(vi);
            if (!dv.empty()) {
                e.add(keys.product_value);
                dv.serialise(e);
            }
            break;
        }
        default:
            throw_consistency_error("parsing Product", "unknown Product style " + formatStyle(sty));
    }
}

std::string Product::exactQuery() const
{
    switch (style())
    {
        case product::Style::GRIB1:
        {
            unsigned o, t, p;
            get_GRIB1(o, t, p);

            char buf[128];
            snprintf(buf, 128, "GRIB1,%u,%u,%u", o, t, p);
            return buf;
        }
        case product::Style::GRIB2:
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
        case product::Style::BUFR:
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
        case product::Style::ODIMH5:
        {
            std::string ob, pr;
            get_ODIMH5(ob, pr);

            std::ostringstream ss;
            ss << formatStyle(style()) << ","
               << ob << ","
               << pr;
            return ss.str();
        }
        case product::Style::VM2:
        {
            unsigned vi;
            get_VM2(vi);

            std::stringstream ss;
            ss << "VM2," << vi;
            auto dv = get_VM2_derived_values(vi);
            if (!dv.empty())
                ss << ":" << dv.toString();
            return ss.str();
        }
        default:
            throw_consistency_error("parsing Product", "unknown Product style " + formatStyle(style()));
    }
}

unique_ptr<Product> Product::decode(core::BinaryDecoder& dec)
{
    dec.ensure_size(1, "Product style");
    std::unique_ptr<Product> res(new Product(dec.buf, dec.size));
    dec.skip(dec.size);
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
    return std::unique_ptr<Product>(new Product(buf, 4));
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
    return std::unique_ptr<Product>(new Product(buf, size));
}

std::unique_ptr<Product> Product::createBUFR(unsigned char type, unsigned char subtype, unsigned char localsubtype)
{
    uint8_t* buf = new uint8_t[4];
    buf[0] = (uint8_t)product::Style::BUFR;
    buf[1] = type;
    buf[2] = subtype;
    buf[3] = localsubtype;
    return std::unique_ptr<Product>(new Product(buf, 4));
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
    return std::unique_ptr<Product>(new Product(buf));
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
    return std::unique_ptr<Product>(new Product(buf));
}

std::unique_ptr<Product> Product::createVM2(unsigned variable_id)
{
    uint8_t* buf = new uint8_t[5];
    buf[0] = (uint8_t)product::Style::VM2;
    core::BinaryEncoder::set_unsigned(buf + 1, variable_id, 4);
    // TODO: also add derived values to the binary representation? Really? derived_values().encode(enc);
    // yes, but then implement encode_for_indexing
    // better: reverse encoding: do not keep derived values in ram, pull them
    // in only when serializing for transmission
    return std::unique_ptr<Product>(new Product(buf, 5));
}


void Product::init()
{
    MetadataType::register_type<Product>();
}

}
}

#include <arki/types/styled.tcc>
