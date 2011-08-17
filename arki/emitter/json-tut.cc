/*
 * Copyright (C) 2010--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/tests/test-utils.h>
#include <arki/emitter/json.h>
#include <arki/emitter/memory.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::emitter;

struct arki_emitter_json_shar {
    arki_emitter_json_shar()
    {
    }
};
TESTGRP(arki_emitter_json);

// null value
template<> template<>
void to::test<1>()
{
    stringstream str;
    JSON json(str);
    json.add_null();
    ensure_equals(str.str(), "null");

    str.seekg(0);
    Memory m;
    JSON::parse(str, m);
    ensure_equals(m.root().is_null(), true);
}

// bool value
template<> template<>
void to::test<2>()
{
    {
        stringstream str;
        JSON json(str);
        json.add(true);
        ensure_equals(str.str(), "true");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_bool(), true);
        ensure_equals(m.root().get_bool(), true);
    }

    {
        stringstream str;
        JSON json(str);
        json.add(false);
        ensure_equals(str.str(), "false");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_bool(), true);
        ensure_equals(m.root().get_bool(), false);
    }
}

// int value
template<> template<>
void to::test<3>()
{
    {
        stringstream str;
        JSON json(str);
        json.add(1);
        ensure_equals(str.str(), "1");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_int(), true);
        ensure_equals(m.root().get_int(), 1);
    }

    {
        stringstream str;
        JSON json(str);
        json.add(-1234567);
        ensure_equals(str.str(), "-1234567");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_int(), true);
        ensure_equals(m.root().get_int(), -1234567);
    }
}

// double value
template<> template<>
void to::test<4>()
{
    {
        stringstream str;
        JSON json(str);
        json.add(1.1);
        ensure_equals(str.str(), "1.1");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_double(), true);
        ensure_equals(m.root().get_double(), 1.1);
    }

    {
        stringstream str;
        JSON json(str);
        json.add(-1.1);
        ensure_equals(str.str(), "-1.1");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_double(), true);
        ensure_equals(m.root().get_double(), -1.1);
    }

    {
        stringstream str;
        JSON json(str);
        json.add(1.0);
        ensure_equals(str.str(), "1.0");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_double(), true);
        ensure_equals(m.root().get_double(), 1.0);
    }

    {
        stringstream str;
        JSON json(str);
        json.add(-1.0);
        ensure_equals(str.str(), "-1.0");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_double(), true);
        ensure_equals(m.root().get_double(), -1.0);
    }
}

// string value
template<> template<>
void to::test<5>()
{
    {
        stringstream str;
        JSON json(str);
        json.add("");
        ensure_equals(str.str(), "\"\"");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_string(), true);
        ensure_equals(m.root().get_string(), "");
    }

    {
        stringstream str;
        JSON json(str);
        json.add("antani");
        ensure_equals(str.str(), "\"antani\"");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_string(), true);
        ensure_equals(m.root().get_string(), "antani");
    }
}

// list value
template<> template<>
void to::test<6>()
{
    {
        stringstream str;
        JSON json(str);
        json.start_list();
        json.add("");
        json.add(1);
        json.add(1.0);
        json.end_list();
        ensure_equals(str.str(), "[\"\",1,1.0]");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_list(), true);
        ensure_equals(m.root().get_list()[0].is_string(), true);
        ensure_equals(m.root().get_list()[0].get_string(), "");
        ensure_equals(m.root().get_list()[1].is_int(), true);
        ensure_equals(m.root().get_list()[1].get_int(), 1);
        ensure_equals(m.root().get_list()[2].is_double(), true);
        ensure_equals(m.root().get_list()[2].get_double(), 1.0);
    }
}

// mapping value
template<> template<>
void to::test<7>()
{
    {
        stringstream str;
        JSON json(str);
        json.start_mapping();
        json.add("", 1);
        json.add("antani", 1.0);
        json.end_mapping();
        ensure_equals(str.str(), "{\"\":1,\"antani\":1.0}");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_mapping(), true);
        ensure_equals(m.root().get_mapping()[""].is_int(), true);
        ensure_equals(m.root().get_mapping()[""].get_int(), 1);
        ensure_equals(m.root().get_mapping()["antani"].is_double(), true);
        ensure_equals(m.root().get_mapping()["antani"].get_double(), 1.0);
    }
}

}

// vim:set ts=4 sw=4:
