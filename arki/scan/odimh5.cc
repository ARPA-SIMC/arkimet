/*
 * scan/odimh5 - Scan a ODIMH5 file for metadata
 *
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
 * Author: Guido Billi <guidobilli@gmail.com>
 */

#include "config.h"

#include <arki/scan/odimh5.h>

#include <arki/metadata.h>

#include <arki/types/origin.h>
#include <arki/types/reftime.h>
#include <arki/types/task.h>
#include <arki/types/quantity.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/area.h>
#include <arki/types/timerange.h>

#include <arki/runtime/config.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

#include <arki/utils/files.h>
#include <arki/utils/lua.h>
#include <arki/scan/any.h>

#include <radarlib/radar.hpp>
#include <radarlib/debug.hpp>

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

namespace arki {
namespace scan {
namespace odimh5 {

/* taken from: http://www.hdfgroup.org/HDF5/doc/H5.format.html#Superblock */
static const unsigned char hdf5sign[8] = { 0x89, 0x48, 0x44, 0x46, 0x0d, 0x0a, 0x1a, 0x0a };

struct OdimH5Validator : public Validator
{
	virtual ~OdimH5Validator() { }

	virtual void validate(int fd, off64_t offset, size_t size, const std::string& fname) const
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

// OdimH5v20 scanner.
// To scan a specific object/product, override the visit() methods and set
// the attribute "supported = true" at the end of the method (otherwise, an
// exception will be thrown).
class Scanner : public OdimH5v20::utils::OdimObjectVisitor, public OdimH5v20::utils::OdimProduct2DVisitor {
 private:
	OdimH5v20::OdimObject& obj;
	Metadata& md;
	bool supported;

 public:
	Scanner(OdimH5v20::OdimObject& obj, Metadata& md) : obj(obj), md(md), supported(false) {}

	void scan() {
		using wibble::exception::Consistency;

		// reftime
		time_t reftime_t = obj.getDateTime();
		struct tm reftime_tm;
		if (gmtime_r(&reftime_t, &reftime_tm) == NULL)
			throw wibble::exception::System("scanning odimh5 file");
		md.set(types::reftime::Position::create(types::Time::create(reftime_tm)));
		// origin
		OdimH5v20::SourceInfo source = obj.getSource();
		md.set(types::origin::ODIMH5::create(source.WMO, source.OperaRadarSite, source.Place));

		visitObject(obj);

		if (!supported)
			throw wibble::exception::Consistency("scanning odimh5 file",
																					 "unsupported file");
	}

 protected:
	virtual void visit(OdimH5v20::PolarVolume& pvol) {
		// product
		md.set(types::product::ODIMH5::create(OdimH5v20::OBJECT_PVOL, OdimH5v20::PRODUCT_SCAN));
		// task
		std::string task = pvol.getTaskOrProdGen();
		if (!task.empty())
			md.set(types::Task::create(task));
		// quantity
		std::set<std::string> quantities = pvol.getStoredQuantities();
		if (!quantities.empty())
			md.set(types::Quantity::create(quantities));
		// level
		std::vector<double> eangles = pvol.getElevationAngles();
		double mine = 360., maxe = 360;
		if (!eangles.empty()) {
			mine = *std::min_element(eangles.begin(), eangles.end());
			maxe = *std::max_element(eangles.begin(), eangles.end());
		}
		md.set(types::level::ODIMH5::create(mine, maxe));
		// area
		double radius = 0;
		for (int i=0; i < pvol.getScanCount(); ++i) {
			std::auto_ptr<OdimH5v20::PolarScan> scan(pvol.getScan(i));
			double val = scan->getRadarHorizon();
			if (val > radius)
				radius = val;
		}
		ValueBag areaValues;
		areaValues.set("lat", Value::createInteger((int)(pvol.getLatitude() * 1000000)));
		areaValues.set("lon", Value::createInteger((int)(pvol.getLongitude() * 1000000)));
		areaValues.set("radius", Value::createInteger((int)(radius * 1000)));
		md.set(types::area::ODIMH5::create(areaValues));
		// set supported
		supported = true;
	}
	virtual void visit(OdimH5v20::ImageObject& img) {
		scanHorizontalObject2D(img);
	}
	virtual void visit(OdimH5v20::CompObject& comp) {
		scanHorizontalObject2D(comp);
	}
	virtual void visit(OdimH5v20::Product_PPI& prod) {
		// level
		double height = prod.getProdPar();
		md.set(types::level::ODIMH5::create(height));
		// set supported
		supported = true;
	}
	virtual void visit(OdimH5v20::Product_CAPPI& prod) {
		// level
		double height = prod.getProdPar();
		md.set(types::level::GRIB1::create(105, height));
		// set supported
		supported = true;
	}
	virtual void visit(OdimH5v20::Product_PCAPPI& prod) {
		// level
		double height = prod.getProdPar();
		md.set(types::level::GRIB1::create(105, height));
		// set supported
		supported = true;
	}
	virtual void visit(OdimH5v20::Product_LBM& prod) {
		supported = true;
	}
	virtual void visit(OdimH5v20::Product_MAX& prod) {
		supported = true;
	}
	virtual void visit(OdimH5v20::Product_RR& prod) {
		time_t p2 = prod.getEndDateTime() - prod.getStartDateTime();
		// Observed accumulation with length p2
		md.set(types::timerange::Timedef::create(0,types::timerange::Timedef::UNIT_SECOND,
																						 1,
																						 p2, types::timerange::Timedef::UNIT_SECOND));
		supported = true;
	}
	virtual void visit(OdimH5v20::Product_VIL& prod) {
		OdimH5v20::VILHeights heights = prod.getProdParVIL();
		// Layer between two height levels above ground (hm)
		md.set(types::level::GRIB1::create(106, heights.top/100, heights.bottom/100));
		supported = true;
	}
	virtual void visit(OdimH5v20::Product_ETOP& prod) {
		supported = true;
	}
	virtual void visit(OdimH5v20::Product_HSP& prod) {
		supported = true;
	}
	virtual void visit(OdimH5v20::Product_VSP& prod) {
		supported = true;
	}
	virtual void scanHorizontalObject2D(OdimH5v20::HorizontalObject_2D& hobj) {
		using wibble::exception::Consistency;

		// Only single product or the triplet (MAX, HSP, VSP) is supported
		std::string prodname;
		std::vector<std::string> prodtypes = hobj.getProductsType();
		if (prodtypes.size() == 1) {
			// Single product object
			prodname = prodtypes.at(0);
		} else if (prodtypes.size() == 3) {
			// If the object contains 3 products, then they must be MAX, HSP, VSP.
			
			std::set<std::string> prodset;
			prodset.insert(OdimH5v20::PRODUCT_MAX);
			prodset.insert(OdimH5v20::PRODUCT_HSP);
			prodset.insert(OdimH5v20::PRODUCT_VSP);
			for (std::vector<std::string>::const_iterator i = prodtypes.begin();
					 i != prodtypes.end(); ++i) {
				if (*i != OdimH5v20::PRODUCT_MAX
						&& *i != OdimH5v20::PRODUCT_HSP
						&& *i != OdimH5v20::PRODUCT_VSP)
					throw Consistency("scanning odimh5 file",
														"Unsupported product " + *i + 
														" in triple product HVMI");
			}
			// This triplet is called "HVMI"
			prodname = "HVMI";
		} else {
			// Otherwise, the object is not supported
			throw wibble::exception::Consistency("scanning odimh5 file",
																					 "Unsupported ODIMH5 object");
		}
		// product
		md.set(types::product::ODIMH5::create(hobj.getObject(), prodname));
		// task
		std::string task = hobj.getTaskOrProdGen();
		if (!task.empty())
			md.set(types::Task::create(task));
		// quantity
		std::set<std::string> quantities;
		for (int i = 0; i < hobj.getProductCount(); ++i) {
			std::auto_ptr<OdimH5v20::Product_2D> prod(hobj.getProduct(i));
			std::set<std::string> q = prod->getStoredQuantities();
			quantities.insert(q.begin(), q.end());
		}
		if (!quantities.empty())
			md.set(types::Quantity::create(quantities));
		// area (bounding box)
		double latfirst = ( hobj.getLL_Latitude() < hobj.getLR_Latitude() ? hobj.getLL_Latitude() : hobj.getLR_Latitude() );
		double lonfirst = ( hobj.getLL_Longitude() < hobj.getUL_Longitude() ? hobj.getLL_Longitude() : hobj.getUL_Longitude() );
		double latlast =  ( hobj.getUL_Latitude() > hobj.getUR_Latitude() ? hobj.getUL_Latitude() : hobj.getUR_Latitude() );
		double lonlast =  ( hobj.getLR_Longitude() > hobj.getUR_Longitude() ? hobj.getLR_Longitude() : hobj.getUR_Longitude() );
		ValueBag areaValues;
		areaValues.set("latfirst", Value::createInteger((int)(latfirst * 1000000)));
		areaValues.set("lonfirst", Value::createInteger((int)(lonfirst * 1000000)));
		areaValues.set("latlast", Value::createInteger((int)(latlast * 1000000)));
		areaValues.set("lonlast", Value::createInteger((int)(lonlast * 1000000)));
		areaValues.set("type", Value::createInteger(0));
		md.set(types::area::GRIB::create(areaValues));
		// level is scanned from the product
		for (int i = 0; i < hobj.getProductCount(); ++i) {
			std::auto_ptr<OdimH5v20::Product_2D> prod(hobj.getProduct(i));
			visitProduct2D(*prod);
		}
	}
};

}

OdimH5::OdimH5() : odimObj(0), read(false) {}

OdimH5::~OdimH5()
{
	delete odimObj;
}

void OdimH5::open(const std::string& filename)
{
	std::string basedir, relname;
  utils::files::resolve_path(filename, basedir, relname);
  open(wibble::sys::fs::abspath(filename), basedir, relname);
}

void OdimH5::open(const std::string& filename, const std::string& basedir, const std::string& relname)
{
	// Close the previous file if needed
	close();
	this->filename = filename;
	this->basedir = basedir;
	this->relname = relname;

	/* create an OdimH5 factory */
	std::auto_ptr<OdimH5v20::OdimFactory> f (new OdimH5v20::OdimFactory());
	/* load OdimH5 object */
	odimObj = f->open(filename, H5F_ACC_RDONLY);
	read = 0;
}

void OdimH5::close()
{
	filename.clear();
  basedir.clear();
  relname.clear();
	delete odimObj;
	odimObj = NULL;
	read = 0;
}

bool OdimH5::next(Metadata& md)
{
	if (read >= 1) {
		return false;
	} else {
		md.create();
		setSource(md);

		/* NOTA: per ora la next estrare un metadato unico per un intero file */
		try {
			odimh5::Scanner scanner(*odimObj, md);
			scanner.scan();
		} catch (const std::exception& e) {
			throw wibble::exception::Consistency("while scanning file " + filename,
																					 e.what());
		}

		read++;

		return true;
	}
}

void OdimH5::setSource(Metadata& md)
{
	int length;
	/* for ODIMH5 files the source is the entire file */

	std::ifstream is;
	is.open(filename.c_str(), std::ios::binary );

	/* calcualte file size */
	is.seekg (0, std::ios::end);
	length = is.tellg();
	is.seekg (0, std::ios::beg);

	std::auto_ptr<char> buff(new char [length]);

	is.read(buff.get(),length);
	is.close();

	md.source = types::source::Blob::create("odimh5", basedir, relname, 0, length);
	md.setCachedData(wibble::sys::Buffer(buff.release(), length));

	md.add_note(types::Note::create("Scanned from " + relname + ":0+" + wibble::str::fmt(length)));
}

}
}

// vim:set ts=4 sw=4:
