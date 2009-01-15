/*
 * store-grib - Store a GRIB 1 message in the given GRIB storage file.
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
#include <wibble/commandline/parser.h>
#include <grib/GRIB.h>
#include <iostream>
#include <fstream>

#include <arki/metadata.h>
#include <arki/utils.h>

#include "config.h"

using namespace std;
using namespace arki;

void store(Metadata& md,
			const std::string& inputdir,
			ostream& mdoutput,
			ostream& dataoutput,
			const std::string& mdoutputname,
			const std::string& dataoutputname)
{
	string fileName;
	size_t offset;
	size_t size;

	Source s = md.getSource();
	s.getBlob(fileName, offset, size);

	// Open the input file
	string file = inputdir + "/" + fileName;
	ifstream in;
	in.open(file.c_str(), ios::in);
	if (!in.is_open() || in.fail())
		throw wibble::exception::File(file, "opening file for reading");
	in.seekg(offset);
	if (in.fail())
		throw wibble::exception::File(file, "moving to position " + utils::fmt(offset));

	// Read the data
	char buf[size];
	in.read(buf, size);
	if (in.fail())
		throw wibble::exception::File(file, "reading " + utils::fmt(size) + " bytes from the file");

	// Get the write position in the file
	offset = dataoutput.tellp();
	if (dataoutput.fail())
		throw wibble::exception::File(dataoutputname, "reading the current position");
	md.setSource(Source::createBlob(s.format, utils::basename(dataoutputname), offset, size));

	// Write the data
	dataoutput.write(buf, size);
	if (dataoutput.fail())
		throw wibble::exception::File(dataoutputname, "writing " + utils::fmt(size) + " bytes from the file");

	// Write the metadata
	md.write(mdoutput, mdoutputname);
}

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	StringOption* arcfile;
	StringOption* sourcedir;

	Options() : StandardParserWithManpage("store-grib", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] -o archivefile metadatafile1 [metadatafile2 ...]";
		description = "Store the GRIB messages identified by the given metadata files into an archive file";

		arcfile = add<StringOption>("output", 'o', "output", "archivefile",
					"archive file name (without extension) where the messages have to be stored"
					" (this option is mandatory)");
		sourcedir = add<StringOption>("source", 's', "source", "dir",
					"source directory where the input GRIB files are located"
					" (default: the current directory)");
	}
};

}
}

int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	try {
		opts.parse(argc, argv);

		// Get commandline parameters
		if (!opts.arcfile->isSet())
			throw wibble::exception::BadOption("option -o or --output is mandatory");
		string outputFile = opts.arcfile->stringValue();

		string inputdir;
		if (opts.sourcedir->isSet())
			inputdir = opts.sourcedir->stringValue();
		else
			inputdir = ".";

		// Open the output files
		ofstream mdfile;
		ofstream datafile;

		string mdfilename = outputFile + ".metadata";
		mdfile.open(mdfilename.c_str(), ios::out | ios::app | ios::binary);
		if (!mdfile.is_open() || !mdfile.good())
			throw wibble::exception::File(mdfilename, "opening file for appending data");

		string datafilename = outputFile + ".grib";
		datafile.open(datafilename.c_str(), ios::out | ios::app | ios::binary);
		if (!datafile.is_open() || !datafile.good())
			throw wibble::exception::File(datafilename, "opening file for appending data");

		while (opts.hasNext())
		{
			string inputmdname = opts.next();
			Metadata md;
			if (inputmdname == "-")
			{
				while (!cin.eof())
				{
					md.read(cin, "(stdin)");
					store(md, inputdir, mdfile, datafile, mdfilename, datafilename);
				}
			}
			else
			{
				ifstream inputmd;
				inputmd.open(inputmdname.c_str(), ios::in);
				if (!inputmd.is_open() || inputmd.fail())
					throw wibble::exception::File(inputmdname, "opening file for reading");
				while (md.read(inputmd, inputmdname))
				{
					store(md, inputdir, mdfile, datafile, mdfilename, datafilename);
				}
			}
		}

		mdfile.close();
		datafile.close();

		return 0;
	} catch (wibble::exception::BadOption& e) {
		cerr << e.desc() << endl;
		opts.outputHelp(cerr);
		return 1;
	} catch (std::exception& e) {
		cerr << e.what() << endl;
		return 1;
	}
}

// vim:set ts=4 sw=4:
