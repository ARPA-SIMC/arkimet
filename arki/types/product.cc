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

// Deprecated
int Product::getMaxIntCount() { return 4; }

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

unique_ptr<Product> Product::decode(core::BinaryDecoder& dec)
{
    Style s = (Style)dec.pop_uint(1, "product");
    switch (s)
    {
        case Style::GRIB1: {
            uint8_t origin  = dec.pop_uint(1, "GRIB1 origin");
            uint8_t table   = dec.pop_uint(1, "GRIB1 table");
            uint8_t product = dec.pop_uint(1, "GRIB1 product");
            return createGRIB1(origin, table, product);
        }
        case Style::GRIB2: {
            uint16_t centre    = dec.pop_uint(2, "GRIB2 centre");
            uint8_t discipline = dec.pop_uint(1, "GRIB2 discipline");
            uint8_t category   = dec.pop_uint(1, "GRIB2 category");
            uint8_t number     = dec.pop_uint(1, "GRIB2 number");
            uint8_t table_version = 4;
            if (dec) table_version = dec.pop_uint(1, "GRIB2 table version");
            uint8_t local_table_version = 255;
            if (dec) local_table_version = dec.pop_uint(1, "GRIB2 local table version");
            return createGRIB2(centre, discipline, category, number, table_version, local_table_version);
        }
        case Style::BUFR: {
            uint8_t type         = dec.pop_uint(1, "GRIB1 type");
            uint8_t subtype      = dec.pop_uint(1, "GRIB1 subtype");
            uint8_t localsubtype = dec.pop_uint(1, "GRIB1 localsubtype");
            ValueBag values = ValueBag::decode(dec);
            return createBUFR(type, subtype, localsubtype, values);
        }
        case Style::ODIMH5: {
            size_t len;
            len = dec.pop_varint<size_t>("ODIMH5 obj len");
            string o = dec.pop_string(len, "ODIMH5 obj");
            len = dec.pop_varint<size_t>("ODIMH5 product len");
            string p = dec.pop_string(len, "ODIMH5 product ");
            return createODIMH5(o, p);
        }
        case Style::VM2: {
            return createVM2(dec.pop_uint(4, "VM2 variable id"));
        }
        default:
            throw_consistency_error("parsing Product", "style is " + formatStyle(s) + "but we can only decode GRIB1, GRIB2 and BUFR");
    }
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
	string inner;
	Product::Style style = outerParse<Product>(val, inner);
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
    switch (style_from_structure(keys, val))
    {
        case Style::GRIB1: return upcast<Product>(product::GRIB1::decode_structure(keys, val));
        case Style::GRIB2: return upcast<Product>(product::GRIB2::decode_structure(keys, val));
        case Style::BUFR: return upcast<Product>(product::BUFR::decode_structure(keys, val));
        case Style::ODIMH5: return upcast<Product>(product::ODIMH5::decode_structure(keys, val));
        case Style::VM2: return upcast<Product>(product::VM2::decode_structure(keys, val));
        default: throw std::runtime_error("Unknown Product style");
    }
}


std::unique_ptr<Product> Product::createGRIB1(unsigned char origin, unsigned char table, unsigned char product)
{
    return upcast<Product>(product::GRIB1::create(origin, table, product));
}
std::unique_ptr<Product> Product::createGRIB2(
        unsigned short centre,
        unsigned char discipline,
        unsigned char category,
        unsigned char number,
        unsigned char table_version,
        unsigned char local_table_version)
{
    return upcast<Product>(product::GRIB2::create(centre, discipline, category, number, table_version, local_table_version));
}
std::unique_ptr<Product> Product::createBUFR(unsigned char type, unsigned char subtype, unsigned char localsubtype)
{
    return upcast<Product>(product::BUFR::create(type, subtype, localsubtype));
}
std::unique_ptr<Product> Product::createBUFR(unsigned char type, unsigned char subtype, unsigned char localsubtype, const ValueBag& name)
{
    return upcast<Product>(product::BUFR::create(type, subtype, localsubtype, name));
}
std::unique_ptr<Product> Product::createODIMH5(const std::string& obj, const std::string& prod)
{
    return upcast<Product>(product::ODIMH5::create(obj, prod));
}
std::unique_ptr<Product> Product::createVM2(unsigned variable_id)
{
    return upcast<Product>(product::VM2::create(variable_id));
}


namespace product {

Product::Style GRIB1::style() const { return Style::GRIB1; }
void GRIB1::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Product::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_origin, 1);
    enc.add_unsigned(m_table, 1);
    enc.add_unsigned(m_product, 1);
}
std::ostream& GRIB1::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
	     << setfill('0')
	     << setw(3) << (int)m_origin << ", "
	     << setw(3) << (int)m_table << ", "
	     << setw(3) << (int)m_product
	     << setfill(' ')
	     << ")";
}
void GRIB1::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Product::serialise_local(e, keys, f);
    e.add(keys.product_origin, m_origin);
    e.add(keys.product_table, m_table);
    e.add(keys.product_product, m_product);
}

std::unique_ptr<GRIB1> GRIB1::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return GRIB1::create(
            val.as_int(keys.product_origin, "product origin"),
            val.as_int(keys.product_table, "product table"),
            val.as_int(keys.product_product, "product product"));
}

std::string GRIB1::exactQuery() const
{
    char buf[128];
    snprintf(buf, 128, "GRIB1,%d,%d,%d", (int)m_origin, (int)m_table, (int)m_product);
    return buf;
}

int GRIB1::compare_local(const Product& o) const
{
    if (int res = Product::compare_local(o)) return res;

    // We should be the same kind, so upcast
    const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
    if (!v)
        throw_consistency_error(
            "comparing metadata types",
            string("second element claims to be a GRIB1 Product, but it is a ") + typeid(&o).name() + " instead");

    if (int res = m_origin - v->m_origin) return res;
    if (int res = m_table - v->m_table) return res;
	return m_product - v->m_product;
}

bool GRIB1::equals(const Type& o) const
{
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v) return false;
	return m_origin == v->m_origin && m_table == v->m_table && m_product == v->m_product;
}

GRIB1* GRIB1::clone() const
{
    GRIB1* res = new GRIB1;
    res->m_origin = m_origin;
    res->m_table = m_table;
    res->m_product = m_product;
    return res;
}

unique_ptr<GRIB1> GRIB1::create(unsigned char origin, unsigned char table, unsigned char product)
{
    GRIB1* res = new GRIB1;
    res->m_origin = origin;
    res->m_table = table;
    res->m_product = product;
    return unique_ptr<GRIB1>(res);
}

std::vector<int> GRIB1::toIntVector() const
{
	vector<int> res;
	res.push_back(m_origin);
	res.push_back(m_table);
	res.push_back(m_product);
	return res;
}


Product::Style GRIB2::style() const { return Style::GRIB2; }
void GRIB2::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Product::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_centre, 2);
    enc.add_unsigned(m_discipline, 1);
    enc.add_unsigned(m_category, 1);
    enc.add_unsigned(m_number, 1);
    if (m_table_version != 4 || m_local_table_version != 255)
    {
        enc.add_unsigned(m_table_version, 1);
        if (m_local_table_version != 255)
            enc.add_unsigned(m_local_table_version, 1);
    }
}
std::ostream& GRIB2::writeToOstream(std::ostream& o) const
{
    o << formatStyle(style()) << "("
      << setfill('0')
      << setw(5) << (int)m_centre << ", "
      << setw(3) << (int)m_discipline << ", "
      << setw(3) << (int)m_category << ", "
      << setw(3) << (int)m_number;
    if (m_table_version != 4 || m_local_table_version != 255)
    {
        o << ", " << setw(3) << (int)m_table_version;
        if (m_local_table_version != 255)
            o << ", " << setw(3) << (int)m_local_table_version;
    }
    o << setfill(' ')
      << ")";
    return o;
}
void GRIB2::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Product::serialise_local(e, keys, f);
    e.add(keys.product_centre, m_centre);
    e.add(keys.product_discipline, m_discipline);
    e.add(keys.product_category, m_category);
    e.add(keys.product_number, m_number);
    e.add(keys.product_table_version, m_table_version);
    e.add(keys.product_local_table_version, m_local_table_version);
}

std::unique_ptr<GRIB2> GRIB2::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    int tv = 4;
    int ltv = 255;
    if (val.has_key(keys.product_table_version, structured::NodeType::INT))
        tv = val.as_int(keys.product_table_version, "product table version");
    if (val.has_key(keys.product_local_table_version, structured::NodeType::INT))
        ltv = val.as_int(keys.product_local_table_version, "product local table version");
    return GRIB2::create(
            val.as_int(keys.product_centre, "product centre"),
            val.as_int(keys.product_discipline, "product discipline"),
            val.as_int(keys.product_category, "product category"),
            val.as_int(keys.product_number, "product number"),
            tv, ltv);
}

std::string GRIB2::exactQuery() const
{
    stringstream ss;
    ss << "GRIB2," << (int)m_centre << "," << (int)m_discipline << "," << (int)m_category << "," << (int)m_number;
    if (m_table_version != 4 || m_local_table_version != 255)
    {
        ss << "," << (int)m_table_version;
        if (m_local_table_version != 255)
            ss << "," << (int)m_local_table_version;
    }
    return ss.str();
}

int GRIB2::compare_local(const Product& o) const
{
    if (int res = Product::compare_local(o)) return res;

    // We should be the same kind, so upcast
    const GRIB2* v = dynamic_cast<const GRIB2*>(&o);
    if (!v)
        throw_consistency_error(
            "comparing metadata types",
            string("second element claims to be a GRIB2 Product, but it is a ") + typeid(&o).name() + " instead");

    if (int res = m_centre - v->m_centre) return res;
    if (int res = m_discipline - v->m_discipline) return res;
    if (int res = m_category - v->m_category) return res;
    if (int res = m_number - v->m_number) return res;
    if (int res = m_table_version - v->m_table_version) return res;
    return m_local_table_version - v->m_local_table_version;
}

bool GRIB2::equals(const Type& o) const
{
    const GRIB2* v = dynamic_cast<const GRIB2*>(&o);
    if (!v) return false;
    return m_centre == v->m_centre
        && m_discipline == v->m_discipline
        && m_category == v->m_category
        && m_number == v->m_number
        && m_table_version == v->m_table_version
        && m_local_table_version == v->m_local_table_version;
}

GRIB2* GRIB2::clone() const
{
    GRIB2* res = new GRIB2;
    res->m_centre = m_centre;
    res->m_discipline = m_discipline;
    res->m_category = m_category;
    res->m_number = m_number;
    res->m_table_version = m_table_version;
    res->m_local_table_version = m_local_table_version;
    return res;
}

unique_ptr<GRIB2> GRIB2::create(unsigned short centre, unsigned char discipline, unsigned char category, unsigned char number, unsigned char table_version, unsigned char local_table_version)
{
    GRIB2* res = new GRIB2;
    res->m_centre = centre;
    res->m_discipline = discipline;
    res->m_category = category;
    res->m_number = number;
    res->m_table_version = table_version;
    res->m_local_table_version = local_table_version;
    return unique_ptr<GRIB2>(res);
}

std::vector<int> GRIB2::toIntVector() const
{
    vector<int> res;
    res.push_back(m_centre);
    res.push_back(m_discipline);
    res.push_back(m_category);
    res.push_back(m_number);
    res.push_back(m_table_version);
    res.push_back(m_local_table_version);
    return res;
}


Product::Style BUFR::style() const { return Style::BUFR; }

void BUFR::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Product::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_type, 1);
    enc.add_unsigned(m_subtype, 1);
    enc.add_unsigned(m_localsubtype, 1);
    m_values.encode(enc);
}
std::ostream& BUFR::writeToOstream(std::ostream& o) const
{
	o << formatStyle(style()) << "("
	  << setfill('0')
	  << setw(3) << (int)m_type << ", "
	  << setw(3) << (int)m_subtype << ", "
	  << setw(3) << (int)m_localsubtype
	  << setfill(' ');
	if (m_values.empty())
		o << ")";
	else
		o << ", " << m_values << ")";
	return o;
}
void BUFR::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Product::serialise_local(e, keys, f);
    e.add(keys.product_type, m_type);
    e.add(keys.product_subtype, m_subtype);
    e.add(keys.product_local_subtype, m_localsubtype);
    if (!m_values.empty())
    {
        e.add(keys.product_value);
        m_values.serialise(e);
    }
}

std::unique_ptr<BUFR> BUFR::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    if (val.has_key(keys.product_value, structured::NodeType::MAPPING))
    {
        ValueBag values;
        val.sub(keys.product_value, "product value", [&](const structured::Reader& value) {
            values = ValueBag::parse(value);
        });
        return BUFR::create(
                val.as_int(keys.product_type, "product type"),
                val.as_int(keys.product_subtype, "product subtype"),
                val.as_int(keys.product_local_subtype, "product localsubtype"),
                values);
    } else {
        return BUFR::create(
                val.as_int(keys.product_type, "product type"),
                val.as_int(keys.product_subtype, "product subtype"),
                val.as_int(keys.product_local_subtype, "product localsubtype"));
    }
}

std::string BUFR::exactQuery() const
{
    stringstream ss;
    ss << "BUFR," << type() << "," << subtype() << "," << localsubtype();
    if (!m_values.empty())
        ss << ":" << m_values.toString();
    return ss.str();
}

int BUFR::compare_local(const Product& o) const
{
    if (int res = Product::compare_local(o)) return res;

    // We should be the same kind, so upcast
    const BUFR* v = dynamic_cast<const BUFR*>(&o);
    if (!v)
        throw_consistency_error(
            "comparing metadata types",
            string("second element claims to be a BUFR Product, but it is a ") + typeid(&o).name() + " instead");

    if (int res = m_type - v->m_type) return res;
    if (int res = m_subtype - v->m_subtype) return res;
    if (int res = m_localsubtype - v->m_localsubtype) return res;
    return m_values.compare(v->m_values);
}
bool BUFR::equals(const Type& o) const
{
	const BUFR* v = dynamic_cast<const BUFR*>(&o);
	if (!v) return false;
	return m_type == v->m_type
	    && m_subtype == v->m_subtype
	    && m_localsubtype == v->m_localsubtype
	    && m_values == v->m_values;
}

BUFR* BUFR::clone() const
{
    BUFR* res = new BUFR;
    res->m_type = m_type;
    res->m_subtype = m_subtype;
    res->m_localsubtype = m_localsubtype;
    res->m_values = m_values;
    return res;
}

unique_ptr<BUFR> BUFR::create(unsigned char type, unsigned char subtype, unsigned char localsubtype)
{
    BUFR* res = new BUFR;
    res->m_type = type;
    res->m_subtype = subtype;
    res->m_localsubtype = localsubtype;
    return unique_ptr<BUFR>(res);
}

unique_ptr<BUFR> BUFR::create(unsigned char type, unsigned char subtype, unsigned char localsubtype, const ValueBag& values)
{
	BUFR* res = new BUFR;
	res->m_type = type;
	res->m_subtype = subtype;
	res->m_localsubtype = localsubtype;
	res->m_values = values;
    return unique_ptr<BUFR>(res);
}

void BUFR::addValues(const ValueBag& newvalues)
{
    m_values.update(newvalues);
}

std::vector<int> BUFR::toIntVector() const
{
	return vector<int>();
}


Product::Style ODIMH5::style() const { return Style::ODIMH5; }

void ODIMH5::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Product::encodeWithoutEnvelope(enc);
    enc.add_varint(m_obj.size());
    enc.add_raw(m_obj);
    enc.add_varint(m_prod.size());
    enc.add_raw(m_prod);
    /*REMOVED: enc.addDouble(m_prodpar1); */
    /*REMOVED: enc.addDouble(m_prodpar2); */
}

std::ostream& ODIMH5::writeToOstream(std::ostream& o) const
{
	return o << formatStyle(style()) << "("
		<< m_obj << ", "
		<< m_prod 
		/* REMOVED: << "," << std::fixed << std::setprecision(5) << m_prodpar1 << "," << std::fixed << std::setprecision(5) << m_prodpar2 << ")" */
		<< ")"
		;
}

void ODIMH5::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Product::serialise_local(e, keys, f);
    e.add(keys.product_object, m_obj);
    e.add(keys.product_product, m_prod);
}

std::unique_ptr<ODIMH5> ODIMH5::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return ODIMH5::create(
            val.as_string(keys.product_object, "product object"),
            val.as_string(keys.product_product, "product name"));
}

std::string ODIMH5::exactQuery() const
{
	std::ostringstream ss;
	ss 	<< formatStyle(style()) << ","
		<< m_obj << ","
		<< m_prod 
		/*REMOVED: << "," << std::fixed << std::setprecision(5) << m_prodpar1 << "," << std::fixed << std::setprecision(5) << m_prodpar2 */
		;
	return ss.str();
}

int ODIMH5::compare_local(const Product& o) const
{
    if (int res = Product::compare_local(o)) return res;

    // We should be the same kind, so upcast
    const ODIMH5* v = dynamic_cast<const ODIMH5*>(&o);
    if (!v)
        throw_consistency_error(
                "comparing metadata types",
                string("second element claims to be a ODIMH5 Product, but it is a ") + typeid(&o).name() + " instead");

    if (int resi = m_obj.compare(v->m_obj)) 	return resi;
	if (int resi = m_prod.compare(v->m_prod)) 	return resi;
	/*REMOVED: if (double resd = m_prodpar1 - v->m_prodpar1) 	return resd > 0 ? 1 : -1; */
	/*REMOVED: if (double resd = m_prodpar2 - v->m_prodpar2) 	return resd > 0 ? 1 : -1; */
	return 0;
}

bool ODIMH5::equals(const Type& o) const
{
	const ODIMH5* v = dynamic_cast<const ODIMH5*>(&o);
	if (!v) return false;
	return (m_obj == v->m_obj) && (m_prod == v->m_prod) 
		/*REMOVED: && (m_prodpar1 == v->m_prodpar1) && (m_prodpar2 == v->m_prodpar2) */
		;
}

ODIMH5* ODIMH5::clone() const
{
    ODIMH5* res = new ODIMH5;
    res->m_obj = m_obj;
    res->m_prod = m_prod;
    return res;
}

unique_ptr<ODIMH5> ODIMH5::create(const std::string& obj, const std::string& prod
	/*PRODPAR: , double prodpar1, double prodpar2 */
)
{
    ODIMH5* res = new ODIMH5;
    res->m_obj = obj;
    res->m_prod = prod;
    /*REMOVED: res->m_prodpar1 = prodpar1; */
    /*REMOVED: res->m_prodpar2  = prodpar2; */
    return unique_ptr<ODIMH5>(res);
}

std::vector<int> ODIMH5::toIntVector() const
{
	return vector<int>();
}


const ValueBag& VM2::derived_values() const {
    if (m_derived_values.get() == 0) {
#ifdef HAVE_VM2
        m_derived_values.reset(new ValueBag(utils::vm2::get_variable(m_variable_id)));
#else
        m_derived_values.reset(new ValueBag);
#endif
    }
    return *m_derived_values;
}

Product::Style VM2::style() const { return Style::VM2; }

void VM2::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Product::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_variable_id, 4);
    derived_values().encode(enc);
}
std::ostream& VM2::writeToOstream(std::ostream& o) const
{
	o << formatStyle(style()) << "(" << m_variable_id;
    if (!derived_values().empty())
        o << ", " << derived_values().toString();
    return o << ")";
}
void VM2::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Product::serialise_local(e, keys, f);
    e.add(keys.product_id, m_variable_id);
    if (!derived_values().empty()) {
        e.add(keys.product_value);
        derived_values().serialise(e);
    }
}
std::string VM2::exactQuery() const
{
    stringstream ss;
    ss << "VM2," << m_variable_id;
    if (!derived_values().empty())
        ss << ":" << derived_values().toString();
    return ss.str();
}
int VM2::compare_local(const Product& o) const
{
    if (int res = Product::compare_local(o)) return res;

    const VM2* v = dynamic_cast<const VM2*>(&o);
    if (!v)
        throw_consistency_error(
            "comparing metadata types",
            string("second element claims to be a VM2 Product, but it is a ") + typeid(&o).name() + " instead");
    if (m_variable_id == v->m_variable_id) return 0;
    return (m_variable_id > v->m_variable_id ? 1 : -1);
}
bool VM2::equals(const Type& o) const
{
    const VM2* v = dynamic_cast<const VM2*>(&o);
    if (!v) return false;
    return m_variable_id == v->m_variable_id;
}

VM2* VM2::clone() const
{
    VM2* res = new VM2;
    res->m_variable_id = m_variable_id;
    return res;
}

unique_ptr<VM2> VM2::create(unsigned variable_id)
{
    VM2* res = new VM2;
    res->m_variable_id = variable_id;
    return unique_ptr<VM2>(res);
}

std::unique_ptr<VM2> VM2::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return VM2::create(val.as_int(keys.product_id, "product id"));
}


std::vector<int> VM2::toIntVector() const
{
	return vector<int>();
}

}

void Product::init()
{
    MetadataType::register_type<Product>();
}

}
}

#include <arki/types/styled.tcc>
