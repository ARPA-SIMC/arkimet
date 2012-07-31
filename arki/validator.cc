/*
 * validator - Arkimet validators
 *
 * Copyright (C) 2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/validator.h>
//#include <arki/types.h>
//#include <wibble/string.h>

using namespace std;

namespace arki {

ValidatorRepository::~ValidatorRepository()
{
    for (iterator i = begin(); i != end(); ++i)
        delete i->second;
}

void ValidatorRepository::add(std::auto_ptr<Validator> v)
{
    iterator i = find(v->name);
    if (i == end())
        insert(make_pair(v->name, v.release()));
    else
    {
        delete i->second;
        i->second = v.release();
    }
}

const ValidatorRepository& ValidatorRepository::get()
{
    static ValidatorRepository* instance = 0;
    if (!instance)
    {
        instance = new ValidatorRepository;
        // TODO: add validators
    }
    return *instance;
}

}
// vim:set ts=4 sw=4:
