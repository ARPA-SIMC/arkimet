#include "config.h"

#include <arki/validator.h>
#include <arki/metadata.h>
#include <arki/types/reftime.h>
#include <arki/wibble/grcal/grcal.h>
//#include <arki/wibble/string.h>

using namespace std;
using namespace arki::types;

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
    const reftime::Position* rt = v.get<reftime::Position>();

    // Ensure we can get position-type reftime information
    if (!rt)
    {
        errors.push_back(name + ": reference time information not found");
        return false;
    }

    int today[6];
    wibble::grcal::date::now(today);

    // Compare until the start of today
    int secs = wibble::grcal::date::duration(rt->time.vals, today);
    //printf("TODAY %d %d %d %d %d %d\n", today[0], today[1], today[2], today[3], today[4], today[5]);
    //printf("VAL   %s\n", rt->time.toSQL().c_str());
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
    secs = wibble::grcal::date::duration(today, rt->time.vals);
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

void ValidatorRepository::add(std::unique_ptr<Validator> v)
{
    iterator i = find(v->name);
    if (i == end())
    {
        Validator* pv = v.release();
        insert(make_pair(pv->name, pv));
    }
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
        instance->add(unique_ptr<Validator>(new validators::DailyImport));
    }
    return *instance;
}

}
// vim:set ts=4 sw=4:
