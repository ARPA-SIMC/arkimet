/*
 * scan/odimh5 - Scan a ODIMH5 file for metadata
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * Author: Guido Billi <guidobilli@gmail.com>
 */

#include "./odimh5.h"

#include <arki/metadata.h>

#include <arki/types/origin.h>
#include <arki/types/reftime.h>
#include <arki/types/task.h>
#include <arki/types/quantity.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/area.h>

#include <arki/runtime/config.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
using namespace wibble;

#include <arki/utils/lua.h>
#include <arki/scan/any.h>

/*============================================================================*/

#include <radarlib/radar.hpp>
using namespace OdimH5v20;
#include <radarlib/debug.hpp>
using namespace Radar;

/*============================================================================*/

#include <cstring>
#include <unistd.h>
#include <fstream>
#include <vector>
#include <stdexcept>
#include <set>
#include <string>
#include <sstream>
#include <memory>
#include <iostream>
using namespace std;

/*============================================================================*/
/* DEBUG FUNCTIONS */
/*============================================================================*/

template <class T> static void DEBUGVECTOR(const std::string& str, const std::vector<T>& values)
{
	#ifdef _DEBUG
	std::ostringstream ss;
	ss << str << ": ";
	for (size_t i=0; i<values.size(); i++)
		ss << values[i] << " ";
	DEBUG(ss.str());
	#endif
}

template <class TYPE> static void DEBUGSET(const std::string& str, const std::set<TYPE> & values)
{
	#ifdef _DEBUG
	typedef typename std::set<TYPE>::const_iterator iterator;
	std::ostringstream ss;
	ss << str << ": ";
	for (iterator i = values.begin(); i != values.end(); i++)
		ss << *i << " ";
	DEBUG(ss.str());
	#endif
}

/*============================================================================*/
/* VALIDATOR */
/*============================================================================*/

namespace arki { namespace scan { namespace odimh5 {

/* taken from: http://www.hdfgroup.org/HDF5/doc/H5.format.html#Superblock */
static const unsigned char hdf5sign[8] = { 0x89, 0x48, 0x44, 0x46, 0x0d, 0x0a, 0x1a, 0x0a };

struct OdimH5Validator : public Validator
{
	virtual ~OdimH5Validator() { }

	virtual void validate(int fd, off_t offset, size_t size, const std::string& fname) const
	{
		/* we check that file header is a valid HDF5 header */

		char buf[8];
		ssize_t res;

		if ((res = pread(fd, buf, 8, offset)) == -1)
			throw wibble::exception::System("reading 8 bytes of ODIMH5 header from " + fname);

		if (memcmp(buf, hdf5sign, 8) != 0)
			throw wibble::exception::Consistency("checking ODIMH5 file " + fname, "signature does not match with supposed value");
	}

	virtual void validate(const void* buf, size_t size) const
	{
		/* we check that file header is a valid HDF5 header */

		if (size < 8)
			throw wibble::exception::Consistency("checking ODIMH5 buffer", "buffer is shorter than 8 bytes");

		if (memcmp(buf, hdf5sign, 8) != 0)
			throw wibble::exception::Consistency("checking ODIMH5 buffer", "buffer does not start with hdf5 signature");
	}
};

static OdimH5Validator odimh5_validator;

const Validator& validator() { return odimh5_validator; }

} } }

/*============================================================================*/
/* LUA INTERFACE */
/*============================================================================*/

namespace arki { namespace scan {

struct OdimH5Lua : public Lua
{
	/**
	 * Create the 'grib' object in the interpreter, to access the grib data of
	 * the current grib read by the scanner
	 */
	OdimH5Lua(OdimH5* scanner);

	/**
	 * Load scan function from file, returning the stored function ID
	 *
	 * It can return -1 if \a fname did not define a `scan' function. This
	 * can happen, for example, if one adds a .lua file that only provides
	 * a library of conveniency functions
	 */
	int load_function(const std::string& fname);

	/**
	 * Same as load_function, but reads Lua code from a string instead of a
	 * file. The file name is still provided as a description of the code,
	 * to use in error messages.
	 */
	int load_function(const std::string& fname, const std::string& code);

	/**
	 * Run previously loaded scan function, passing the given metadata
	 *
	 * Return the error message, or an empty string if no error
	 */
	std::string run_function(int id, Metadata& md);

	/**arki.
	 * arki.If code is non-empty, load scan code from it. Else, load scan code
	 * from configuration files. Store scan function IDs in \a ids
	 */
	void load_scan_code(const std::string& code, const std::string& tag, vector<int>& ids)
	{
		/// Load the grib1 scanning functions
		if (!code.empty())
		{
			int id = load_function(tag + " scan instructions", code);
			if (id != -1) ids.push_back(id);
			return;
		}
		vector<string> files = runtime::rcFiles("scan-" + tag, "ARKI_SCAN_" + str::toupper(tag));
		for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
		{
			// Skip non-lua files
			if (! str::endsWith(*i, ".lua")) continue;
			int id = load_function(*i);
			if (id != -1) ids.push_back(id);
		}
	}
};

OdimH5Lua::OdimH5Lua(OdimH5* scanner)
{
	//	/// Create the 'grib' Lua object
	//
	//	// Create the metatable for the grib object
	//	luaL_newmetatable(L, "grib");
	//
	//	// Set the __index metamethod to the arkilua_lookup_grib function
	//	lua_pushstring(L, "__index");
	//	lua_pushcfunction(L, Grib::arkilua_lookup_grib);
	//	lua_settable(L, -3);
	//
	//	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	//	Grib** s = (Grib**)lua_newuserdata(L, sizeof(Grib*));
	//	*s = scanner;
	//
	//	// Set the metatable for the userdata
	//	luaL_getmetatable(L, "grib");
	//	lua_setmetatable(L, -2);
	//
	//	// Set the userdata object as the global 'grib' variable
	//	lua_setglobal(L, "grib");
	//
	//	// Pop the metatable from the stack
	//	lua_pop(L, 1);
	//
	//
	//	/// Create the 'gribl' Lua object
	//
	//	// Create the metatable for the grib object
	//	luaL_newmetatable(L, "gribl");
	//
	//	// Set the __index metamethod to the arkilua_lookup_grib function
	//	lua_pushstring(L, "__index");
	//	lua_pushcfunction(L, Grib::arkilua_lookup_gribl);
	//	lua_settable(L, -3);
	//
	//	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	//	s = (Grib**)lua_newuserdata(L, sizeof(Grib*));
	//	*s = scanner;
	//
	//	// Set the metatable for the userdata
	//	luaL_getmetatable(L, "gribl");
	//	lua_setmetatable(L, -2);
	//
	//	// Set the userdata object as the global 'grib' variable
	//	lua_setglobal(L, "gribl");
	//
	//	// Pop the metatable from the stack
	//	lua_pop(L, 1);
	//
	//
	//	/// Create the 'gribs' Lua object
	//
	//	// Create the metatable for the grib object
	//	luaL_newmetatable(L, "gribs");
	//
	//	// Set the __index metamethod to the arkilua_lookup_grib function
	//	lua_pushstring(L, "__index");
	//	lua_pushcfunction(L, Grib::arkilua_lookup_gribs);
	//	lua_settable(L, -3);
	//
	//	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	//	s = (Grib**)lua_newuserdata(L, sizeof(Grib*));
	//	*s = scanner;
	//
	//	// Set the metatable for the userdata
	//	luaL_getmetatable(L, "gribs");
	//	lua_setmetatable(L, -2);
	//
	//	// Set the userdata object as the global 'grib' variable
	//	lua_setglobal(L, "gribs");
	//
	//	// Pop the metatable from the stack
	//	lua_pop(L, 1);
	//
	//
	//	/// Create the 'gribd' Lua object
	//
	//	// Create the metatable for the grib object
	//	luaL_newmetatable(L, "gribd");
	//
	//	// Set the __index metamethod to the arkilua_lookup_grib function
	//	lua_pushstring(L, "__index");
	//	lua_pushcfunction(L, Grib::arkilua_lookup_gribd);
	//	lua_settable(L, -3);
	//
	//	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	//	s = (Grib**)lua_newuserdata(L, sizeof(Grib*));
	//	*s = scanner;
	//
	//	// Set the metatable for the userdata
	//	luaL_getmetatable(L, "gribd");
	//	lua_setmetatable(L, -2);
	//
	//	// Set the userdata object as the global 'grib' variable
	//	lua_setglobal(L, "gribd");
	//
	//	// Pop the metatable from the stack
	//	lua_pop(L, 1);
}

int OdimH5Lua::load_function(const std::string& fname)
{
	//	// Compile the macro
	//        if (luaL_dofile(L, fname.c_str()))
	//        {
	//                // Copy the error, so that it will exist after the pop
	//                string error = lua_tostring(L, -1);
	//                // Pop the error from the stack
	//                lua_pop(L, 1);
	//                throw wibble::exception::Consistency("parsing " + fname, error);
	//        }
	//
	//	// Index the scan function
	//	int id = -1;
	//        lua_getglobal(L, "scan");
	//        if (lua_isfunction(L, -1))
	//                id = luaL_ref(L, LUA_REGISTRYINDEX);
	//
	//	// Return the ID, or -1 if no 'scan' function was defined
	//	return id;
	return 0;
}

int OdimH5Lua::load_function(const std::string& fname, const std::string& code)
{
	//
	//	// Compile the macro
	//	if (luaL_loadbuffer(L, code.data(), code.size(), fname.c_str()))
	//        {
	//                // Copy the error, so that it will exist after the pop
	//                string error = lua_tostring(L, -1);
	//                // Pop the error from the stack
	//                lua_pop(L, 1);
	//                throw wibble::exception::Consistency("parsing " + fname, error);
	//        }
	//
	//	// Index the scan function
	//	int id = -1;
	//        lua_getglobal(L, "scan");
	//        if (lua_isfunction(L, -1))
	//                id = luaL_ref(L, LUA_REGISTRYINDEX);
	//
	//	// Return the ID, or -1 if no 'scan' function was defined
	//	return id;
	return 0;
}

string OdimH5Lua::run_function(int id, Metadata& md)
{
	//        // Retrieve the Lua function registered for this
	//        lua_rawgeti(L, LUA_REGISTRYINDEX, id);
	//
	//	// Pass md
	//	md.lua_push(L);
	//
	//        // Call the function
	//        if (lua_pcall(L, 1, 0, 0))
	//        {
	//                string error = lua_tostring(L, -1);
	//                lua_pop(L, 1);
	//		return error;
	//	} else
	//		return std::string();
	return "";
}

//static void check_grib_error(int error, const char* context)
//{
//	if (error != GRIB_SUCCESS)
//		throw wibble::exception::Consistency(context, grib_get_error_message(error));
//}
//
//// Never returns in case of error
//static void arkilua_check_gribapi(lua_State* L, int error, const char* context)
//{
//	if (error != GRIB_SUCCESS)
//		luaL_error(L, "grib_api error \"%s\" while %s",
//					grib_get_error_message(error), context);
//}

// Lookup a grib value for grib.<fieldname>
//int OdimH5::arkilua_lookup_grib(lua_State* L)
//{
//	// Fetch the Scanner reference from the userdata value
//	luaL_checkudata(L, 1, "grib");
//	void* userdata = lua_touserdata(L, 1);
//	Grib& s = **(Grib**)userdata;
//	grib_handle* gh = s.gh;
//
//	// Get the name to lookup from lua
//	// (we use 2 because 1 is the table, since we are a __index function)
//	luaL_checkstring(L, 2);
//	const char* name = lua_tostring(L, 2);
//
//	// Get information about the type
//	int type;
//	int res = grib_get_native_type(gh, name, &type);
//	if (res == GRIB_NOT_FOUND)
//		type = GRIB_TYPE_MISSING;
//	else
//		arkilua_check_gribapi(L, res, "getting type of key");
//
//	// Look up the value and push the function result for lua
//	switch (type)
//	{
//		case GRIB_TYPE_LONG: {
//			long val;
//			arkilua_check_gribapi(L, grib_get_long(gh, name, &val), "reading long value");
//			lua_pushnumber(L, val);
//			break;
//		}
//		case GRIB_TYPE_DOUBLE: {
//			double val;
//			arkilua_check_gribapi(L, grib_get_double(gh, name, &val), "reading double value");
//			lua_pushnumber(L, val);
//			break;
//		}
//		case GRIB_TYPE_STRING: {
//			const int maxsize = 1000;
//			char buf[maxsize];
//			size_t len = maxsize;
//			arkilua_check_gribapi(L, grib_get_string(gh, name, buf, &len), "reading string value");
//			if (len > 0) --len;
//			lua_pushlstring(L, buf, len);
//			break;
//		}
//		default:
//		    lua_pushnil(L);
//			break;
//	}
//
//	// Number of values we're returning to lua
//	return 1;
//}

// Lookup a grib value for grib.<fieldname>
//int OdimH5::arkilua_lookup_gribl(lua_State* L)
//{
//	// Fetch the Scanner reference from the userdata value
//	luaL_checkudata(L, 1, "gribl");
//	void* userdata = lua_touserdata(L, 1);
//	Grib& s = **(Grib**)userdata;
//	grib_handle* gh = s.gh;
//
//	// Get the name to lookup from lua
//	// (we use 2 because 1 is the table, since we are a __index function)
//	luaL_checkstring(L, 2);
//	const char* name = lua_tostring(L, 2);
//
//	// Push the function result for lua
//	long val;
//	int res = grib_get_long(gh, name, &val);
//	if (res == GRIB_NOT_FOUND)
//	{
//		lua_pushnil(L);
//	} else {
//		arkilua_check_gribapi(L, res, "reading long value");
//		lua_pushnumber(L, val);
//	}
//
//	// Number of values we're returning to lua
//	return 1;
//}

//// Lookup a grib value for grib.<fieldname>
//int Grib::arkilua_lookup_gribs(lua_State* L)
//{
//	// Fetch the Scanner reference from the userdata value
//	luaL_checkudata(L, 1, "gribs");
//	void* userdata = lua_touserdata(L, 1);
//	Grib& s = **(Grib**)userdata;
//	grib_handle* gh = s.gh;
//
//	// Get the name to lookup from lua
//	// (we use 2 because 1 is the table, since we are a __index function)
//	luaL_checkstring(L, 2);
//	const char* name = lua_tostring(L, 2);
//
//	// Push the function result for lua
//	const int maxsize = 1000;
//	char buf[maxsize];
//	size_t len = maxsize;
//	int res = grib_get_string(gh, name, buf, &len);
//	if (res == GRIB_NOT_FOUND)
//	{
//		lua_pushnil(L);
//	} else {
//		arkilua_check_gribapi(L, res, "reading string value");
//		if (len > 0) --len;
//		lua_pushlstring(L, buf, len);
//	}
//
//	// Number of values we're returning to lua
//	return 1;
//}

// Lookup a grib value for grib.<fieldname>
//int OdimH5::arkilua_lookup_gribd(lua_State* L)
//{
//	// Fetch the Scanner reference from the userdata value
//	luaL_checkudata(L, 1, "gribd");
//	void* userdata = lua_touserdata(L, 1);
//	Grib& s = **(Grib**)userdata;
//	grib_handle* gh = s.gh;
//
//	// Get the name to lookup from lua
//	// (we use 2 because 1 is the table, since we are a __index function)
//	luaL_checkstring(L, 2);
//	const char* name = lua_tostring(L, 2);
//
//	// Push the function result for lua
//	double val;
//	int res = grib_get_double(gh, name, &val);
//	if (res == GRIB_NOT_FOUND)
//	{
//		lua_pushnil(L);
//	} else {
//		arkilua_check_gribapi(L, res, "reading double value");
//		lua_pushnumber(L, val);
//	}
//
//	// Number of values we're returning to lua
//	return 1;
//}

} }

/*============================================================================*/
/* ODIM SCANNER */
/*============================================================================*/

namespace arki { namespace scan {

OdimH5::OdimH5()
:filename()
,basename()
,odimObj(NULL)
,read(false)
{
	//	if (m_inline_data)
	//	{
	//		// If we inline the data, we can also do multigrib
	//		grib_multi_support_on(context);
	//	} else {
	//		// Multi support is off unless a child class specifies otherwise
	//		grib_multi_support_off(context);
	//	}
	//
	//	/// Load the grib1 scanning functions
	//	L->load_scan_code(grib1code, "grib1", grib1_funcs);
	//
	//	/// Load the grib2 scanning functions
	//	L->load_scan_code(grib2code, "grib2", grib2_funcs);
	//
	//	//arkilua_dumpstack(L, "Afterinit", stderr);
}

OdimH5::~OdimH5()
{
	delete odimObj;
}

void OdimH5::open(const std::string& filename)
{
	// Close the previous file if needed
	close();
	this->filename = sys::fs::abspath(filename);
	this->basename = str::basename(filename);

	try
	{
		/* create an OdimH5 factory */
		std::auto_ptr<OdimH5v20::OdimFactory> f (new OdimH5v20::OdimFactory());
		/* load OdimH5 object */
		odimObj = f->open(filename);
		read = 0;
	}
	catch (std::exception& e)
	{
		std::cerr << "scanner odim: err:" << e.what() << std::endl;
		throw;
	}
}

void OdimH5::close()
{
	filename.clear();
	try
	{
		delete odimObj;
		odimObj = NULL;
		read = 0;
	}
	catch (std::exception& e)
	{
		std::cerr << "scanner odim: err:" << e.what() << std::endl;
		throw;
	}
}

bool OdimH5::next(Metadata& md)
{
	md.create();
	setSource(md);

	try
	{
		/* NOTA: per ora la next estrare un metadato unico per un intero file */
		getOdimObjectData(this->odimObj, md);
		read++;
		return read == 1;	//la prima volta ritorno true
	}
	catch (std::exception& e)
	{
		std::cerr << "scanner odim: err:" << e.what() << std::endl;
		throw;
	}

	//	int griberror;
	//
	//	// If there's still a grib_handle around (for example, if a previous call
	//	// to next() threw an exception), deallocate it here to prevent a leak
	//	if (gh)
	//	{
	//		check_grib_error(grib_handle_delete(gh), "closing GRIB message");
	//		gh = 0;
	//	}
	//
	//	gh = grib_handle_new_from_file(context, in, &griberror);
	//	if (gh == 0 && griberror == GRIB_END_OF_FILE)
	//		return false;
	//	check_grib_error(griberror, "reading GRIB from file");
	//	if (gh == 0)
	//		return false;
	//
	//	md.create();
	//
	//	setSource(md);
	//
	//	long edition;
	//	check_grib_error(grib_get_long(gh, "editionNumber", &edition), "reading edition number");
	//	switch (edition)
	//	{
	//		case 1: scanGrib1(md); break;
	//		case 2: scanGrib2(md); break;
	//		default:
	//			throw wibble::exception::Consistency("reading grib message", "GRIB edition " + str::fmt(edition) + " is not supported");
	//	}
	//
	//	check_grib_error(grib_handle_delete(gh), "closing GRIB message");
	//	gh = 0;
	//
	//	return true;
	//	return false;
}

void OdimH5::getOdimObjectData(OdimH5v20::OdimObject* obj, Metadata& md)
{
	std::string object = obj->getObject();
	if (object == OdimH5v20::OBJECT_PVOL)
	{
		getPVOLData((OdimH5v20::PolarVolume*)obj, md);
	}
	else
	{
		std::ostringstream ss;
		ss << "Unsupported odimH5 object " << object << "!";
		throw std::logic_error(ss.str());
	}
}

void OdimH5::getPVOLData(OdimH5v20::PolarVolume* pvol, Metadata& md)
{
	std::string		object		= OdimH5v20::OBJECT_PVOL;
	std::string		prod		= OdimH5v20::PRODUCT_SCAN;
	OdimH5v20::SourceInfo	source		= pvol->getSource();
	time_t			dateTime	= pvol->getDateTime();
	std::string		task		= pvol->getTaskOrProdGen();
	std::vector<double>	eangles		= pvol->getElevationAngles();
	std::set<std::string>	quantities	= pvol->getStoredQuantities();
	double			lat		= pvol->getLatitude();
	double			lon		= pvol->getLongitude();
	double			radius		= 0;

	//--- IL TIPO DI OGGETTO va a finire in PRODUCT ---

	DEBUG("Object:    " << object);
	DEBUG("Prod:      " << prod);

	md.set(types::product::ODIMH5::create(object, prod));

	//--- SOURCE va a finire in ORIGIN ---

	DEBUG("Source:    " << source.toString());

	md.set(
		types::origin::ODIMH5::create(
			source.WMO,
			source.OperaRadarSite,
			source.Place
		)
	);

	//--- DATE E TIME vanno a finire in REFTIME ---

	DEBUG("Date/time: " << timeutils::absoluteToString(dateTime));

	int year, month, day, hour, min, sec;

	Radar::timeutils::splitYMDHMS(dateTime, year, month, day, hour, min, sec);

	md.set(types::reftime::Position::create(
		new types::Time(year, month, day, hour, min, sec)
	));

	//--- TASK ha il suo metadato ---

	DEBUG("Task:      " << task);

	if (task.size())
		md.set(types::Task::create(task));

	//--- QUANTITY ha il suo metadato ---

	DEBUGSET<std::string>("QUANTITY: ", quantities);

	if (quantities.size())
		md.set(types::Quantity::create(quantities));

	//--- EANGLE va a finire in LEVEL ---

	DEBUGVECTOR<double>("EANGLE: ", eangles);

	double mine = 360.;
	double maxe = -360;

	for (size_t i=0; i<eangles.size(); i++)
	{
		if (eangles[i] <= mine)
			mine = eangles[i];
		if (eangles[i] >= maxe)
			maxe = eangles[i];
	}

	DEBUG("EANGLE: " << mine << " / " << maxe);

	md.set(types::level::ODIMH5::create(mine, maxe));

	//--- WHERE va a finire in AREA ---

	int scanCount = pvol->getScanCount();	
	for (int i=0; i<scanCount; i++)
	{
		std::auto_ptr<PolarScan> scan( pvol->getScan(i));
		double val = scan->getRadarHorizon();
		if (val > radius)
			radius = val;
	}

	DEBUG("LAT:       " << lat << " degree");
	DEBUG("LON:       " << lon << " degree");
	DEBUG("RADIUS:    " << radius << " km");
	ValueBag areaValues;
	areaValues.set("lat", 	 Value::createInteger((int)(lat * 1000000)));	//gradi -> numero intero: per i valori in gradi teniamo 6 cifre significative
	areaValues.set("lon", 	 Value::createInteger((int)(lon * 1000000)));	//gradi -> numero intero: per i valori in gradi teniamo 6 cifre significative
	areaValues.set("radius", Value::createInteger((int)(radius * 1000)));	//km a numero intero: teniamo la misura in metri

	md.set(types::area::ODIMH5::create(areaValues));
}

void OdimH5::setSource(Metadata& md)
{
	char* buff = NULL;
	int length;
	try
	{
		/* for ODIMH5 files the source is the entire file */

		std::ifstream is;
		is.open(filename.c_str(), std::ios::binary );

		/* calcualte file size */
		is.seekg (0, ios::end);
		length = is.tellg();
		is.seekg (0, ios::beg);

		buff = new char [length];

		is.read (buff,length);
		is.close();

		md.source = types::source::Blob::create("odimh5", filename, 0, length);
		md.setCachedData(wibble::sys::Buffer(buff, length));
		buff = NULL;

		md.add_note(types::Note::create("Scanned from " + basename + ":0+" + str::fmt(length)));
	}
	catch (...)
	{
		delete[] buff;
		throw;
	}
}

//bool OdimH5::scanLua(std::vector<int> ids, Metadata& md)
//{
//	for (vector<int>::const_iterator i = ids.begin(); i != ids.end(); ++i)
//	{
//		string error = L->run_function(*i, md);
//		if (!error.empty())
//		{
//			md.add_note(types::Note::create("Scanning failed: " + error));
//			return false;
//		}
//	}
//	return true;
//}


/*============================================================================*/

} }



// vim:set ts=4 sw=4:





















