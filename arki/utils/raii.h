#ifndef ARKI_UTILS_RAII_H
#define ARKI_UTILS_RAII_H

/*
 * utils/raii - RAII helpers
 *
 * Copyright (C) 2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

namespace arki {
namespace utils {
namespace raii {

/**
 * Set a value when this object goes out of scope.
 */
template<typename T>
struct SetOnExit
{
    T& ref;
    T val;

    SetOnExit(T& ref, const T& val) : ref(ref), val(val) {}
    ~SetOnExit() { ref = val; }
};

template<typename T>
struct DeleteAndZeroOnExit
{
    T*& ptr;

    DeleteAndZeroOnExit(T*& ptr) : ptr(ptr) {}
    ~DeleteAndZeroOnExit()
    {
        delete ptr;
        ptr = 0;
    }
};

template<typename T>
struct DeleteArrayAndZeroOnExit
{
    T*& ptr;

    DeleteArrayAndZeroOnExit(T*& ptr) : ptr(ptr) {}
    ~DeleteArrayAndZeroOnExit()
    {
        delete[] ptr;
        ptr = 0;
    }
};

}
}
}

// vim:set ts=4 sw=4:
#endif
