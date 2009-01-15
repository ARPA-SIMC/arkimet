/*
 * arki-scan-grib - Scan a GRIB 1 file for metadata.
 *
 * Copyright (C) 2007  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <wibble/exception.h>
#include <grib/GRIB.h>
#include <iostream>
#include <sstream>

#include <arki/metadata.h>
#include <arki/utils.h>

using namespace std;
using namespace arki;

void scan(const std::string& filename)
{
	GRIB_FILE file;
	if (file.OpenRead(filename) != 0)
		throw wibble::exception::File(filename, "opening file for reading");

	while (1)
	{
		GRIB_MESSAGE msg;
		int res = file.ReadMessage(msg);
		if (res == 1)
			break;
		if (res != 0)
			throw wibble::exception::File(filename, "reading GRIB message");

		Metadata md;
		md.create();

		// Set source
		md.setSource(Source::createBlob("grib1", utils::basename(filename), msg.recpos, msg.reclen));

		// Set reference time
		md.setReferenceTime(Time(msg.gtime.year, msg.gtime.month, msg.gtime.day, msg.gtime.hour, msg.gtime.minute, 0));

		// Set time range
		md.setTimeRange(TimeRange::createGRIB(msg.gtime.timerange, msg.gtime.timeunit, msg.gtime.p1, msg.gtime.p2));

		// Set originating centre and product
		if (msg.get_pds_size() >= 26)
		{
			unsigned char* pds = msg.get_pds_values(0, 26);
			// Table version
			int table = pds[3];
			// Originating Centre
			int centre = pds[4];
			// Generating process or model ID (centre dependent)
			int process = pds[5];
			// Identification of sub-centre (Table 0 - Part 2)
			int subcentre = pds[25];
			// Product
			int product = pds[8];
			delete[] pds;
			md.setOrigin(Origin::createGRIB1(centre, subcentre, process));
			md.setProduct(Product::createGRIB1(centre, table, product));
			md.setLevel(Level::createGRIB(pds[9], pds[10] << 8 | pds[11]));
		}

		md.write(cout, "(stdout)");
	}

	file.Close();
}

int main(int argc, const char* argv[])
{
	try {
		for (int i = 1; i < argc; ++i)
		{
			scan(argv[i]);
		}
		return 0;
	} catch (std::exception& e) {
		cerr << e.what() << endl;
		return 1;
	}
}

// vim:set ts=4 sw=4:
