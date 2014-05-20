#ifndef ARKI_UTILS_H5_H
#define ARKI_UTILS_H5_H

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
#include <hdf5.h>
#include <string>

namespace arki {
namespace utils {
namespace h5 {

void throw_error(const std::string& context, hid_t stack=H5E_DEFAULT) __attribute__ ((noreturn));

struct MuteErrors
{
    hid_t stack;
    H5E_auto2_t old_func;
    void *old_client_data;

    MuteErrors(hid_t stack=H5E_DEFAULT)
        : stack(stack)
    {
        H5Eget_auto(stack, &old_func, &old_client_data);
        H5Eset_auto(stack, NULL, NULL);
    }

    ~MuteErrors()
    {
        H5Eset_auto(stack, old_func, old_client_data);
    }

    /// Read HDF5's error stack and throw the right exception for the current situation
    void throw_error(const std::string& context) __attribute__ ((noreturn));
};

/// HDF5 RAII wrappers

template<herr_t HCLOSE(hid_t)>
struct Handle
{
    hid_t h;

    Handle() : h(-1) {}
    Handle(hid_t h) : h(h) {}

    ~Handle()
    {
        if (h >= 0) HCLOSE(h);
    }

    operator hid_t() { return h; }
};

struct File : public Handle<H5Fclose>
{
    File() : Handle<H5Fclose>() {}
    File(hid_t h) : Handle<H5Fclose>(h) {}
};

struct Group : public Handle<H5Gclose>
{
    Group() : Handle<H5Gclose>() {}
    Group(hid_t h) : Handle<H5Gclose>(h) {}
};

struct Space : public Handle<H5Sclose>
{
    Space() : Handle<H5Sclose>() {}
    Space(hid_t h) : Handle<H5Sclose>(h) {}
};

struct Type : public Handle<H5Tclose>
{
    Type() : Handle<H5Tclose>() {}
    Type(hid_t h) : Handle<H5Tclose>(h) {}
};

struct Attr : public Handle<H5Aclose>
{
    Attr() : Handle<H5Aclose>() {}
    Attr(hid_t h) : Handle<H5Aclose>(h) {}

    bool read_string(std::string& res);
};

}
}
}

#endif

