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
#include <arki/metadata.h>
#include <arki/types/reftime.h>
#include <wibble/grcal/grcal.h>
//#include <wibble/string.h>

using namespace std;
using namespace wibble;

namespace arki {

namespace validators {

FailAlways::FailAlways()
{
    name = "fail_always";
    desc = "validator that always fails";
}
bool FailAlways::operator()(const Metadata& v, std::vector<std::string>& errors) const
{
    errors.push_back("fail_always: message does not validate and never will");
    return false;
}

DailyImport::DailyImport()
{
    name = "daily_import";
    desc = "Checks that the reference time is less than a week old and not more than a day into the future";
}
bool DailyImport::operator()(const Metadata& v, std::vector<std::string>& errors) const
{
    UItem<types::reftime::Position> rt = v.get<types::reftime::Position>();

    // Ensure we can get position-type reftime information
    if (!rt.defined())
    {
        errors.push_back(name + ": reference time information not found");
        return false;
    }

    int today[6];
    grcal::date::today(today);

    // Compare until the start of today
    int secs = grcal::date::duration(rt->time->vals, today);
    //printf("TODAY %d %d %d %d %d %d\n", today[0], today[1], today[2], today[3], today[4], today[5]);
    //printf("VAL   %s\n", rt->time->toSQL().c_str());
    //printf("SECS %d\n", secs);
    if (secs > 3600*24*7)
    {
        errors.push_back(name + ": reference time is older than 7 days");
        return false;
    }
    if (secs >= 0)
    {
        return true;
    }

    // Secs was negative, so we compare again from the end of today
    secs = grcal::date::duration(today, rt->time->vals);
    if (secs > 3600*24)
    {
        errors.push_back(name + ": reference time is more than one day into the future");
        return false;
    }

    return true;
}

}

Validator::~Validator() {}

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
