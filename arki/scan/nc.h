#ifndef ARKI_SCAN_NC_H
#define ARKI_SCAN_NC_H

/*
 * scan/nc - Scan a NetCDF file for metadata
 *
 * Copyright (C) 2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * Author: Emanuele Di Giacomo <edigiacomo@arpa.emr.it>
 */

#include <string>
#include <vector>
#include <unistd.h>

namespace arki {
class Metadata;

namespace scan {
struct Validator;

namespace netcdf {
const Validator& validator();
struct Backend;
}

class NetCDF
{
protected:
    std::string filename;
    std::string basedir;
    std::string relname;
    unsigned lineno;
    netcdf::Backend* backend;

public:
    NetCDF();
    virtual ~NetCDF();

    /**
     * Access a file with VM2 data
     */
    void open(const std::string& filename);

    /// Alternate version with explicit basedir/relname separation
    void open(const std::string& filename, const std::string& basedir, const std::string& relname);

    /**
     * Close the input file.
     *
     * This is optional: the file will be closed by the destructor if needed.
     */
    void close();

    /**
     * Scan the next VM2 in the file.
     *
     * @returns
     *   true if it found a VM2 message,
     *   false if there are no more VM2 messages in the file
     */
    bool next(Metadata& md);
};

}
}

// vim:set ts=4 sw=4:
#endif