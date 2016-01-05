/*
 * utils/h5 - HDF5 helpers
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
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "h5.h"
#include <arki/exceptions.h>
#include <sstream>

using namespace std;

namespace arki {
namespace utils {
namespace h5 {

namespace {

herr_t h5_error_stack_to_sstream(unsigned n, const H5E_error2_t *err_desc, void* client_data)
{
    static const unsigned MSG_SIZE = 64;
    char buf[MSG_SIZE];
    std::stringstream& out = *(std::stringstream*)client_data;

    if (H5Eget_class_name(err_desc->cls_id, buf, MSG_SIZE)<0)
        return -1;
    out << "[" << buf << ":";

    if (H5Eget_msg(err_desc->maj_num, NULL, buf, MSG_SIZE)<0)
        return -1;
    out << buf << ":";

    if (H5Eget_msg(err_desc->min_num, NULL, buf, MSG_SIZE)<0)
        return -1;
    out << buf << "]";

    return 0;
}

}

/// Read HDF5's error stack and throw the right exception for the current situation
void throw_error(const std::string& context, hid_t stack)
{
    stringstream s;
    H5Ewalk(stack, H5E_WALK_UPWARD, h5_error_stack_to_sstream, &s);
    throw_consistency_error(context, s.str());
}


void MuteErrors::throw_error(const std::string& context)
{
    h5::throw_error(context, stack);
}

bool Attr::read_string(std::string& res)
{
    // Create the memory datatype, with the size of our temporary buffer
    Type memtype(H5Tcopy(H5T_C_S1));
    if (memtype < 0) return false;

    char buf[512];
    if (H5Tset_size(memtype, 512) < 0) return false;

    // Read the data.
    if (H5Aread(h, memtype, buf) < 0) return false;

    res = buf;

    return true;
}

}
}
}
