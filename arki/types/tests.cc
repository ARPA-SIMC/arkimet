/**
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

#include "config.h"

#include <arki/types/tests.h>
#include <arki/types/source.h>
#include <arki/utils/codec.h>
#include <wibble/sys/fs.h>
#include <sstream>

using namespace std;
using namespace arki;
using namespace wibble;
using namespace wibble::tests;

namespace arki {
namespace tests {

void TestItemSerializes::check(WIBBLE_TEST_LOCPRM) const
{
    // Binary encoding, without envelope
    std::string enc;
    utils::codec::Encoder e(enc);
    act->encodeWithoutEnvelope(e);
    size_t inner_enc_size = enc.size();
    wassert(actual(types::decodeInner(code, (const unsigned char*)enc.data(), enc.size())) == act);

    // Binary encoding, with envelope
    enc = act->encodeWithEnvelope();
    // Rewritten in the next two lines due to, it seems, a bug in old gccs
    // inner_ensure_equals(types::decode((const unsigned char*)enc.data(), enc.size()).upcast<T>(), act);
    Item<> decoded = types::decode((const unsigned char*)enc.data(), enc.size());
    wassert(actual(decoded) == act);

    const unsigned char* buf = (const unsigned char*)enc.data();
    size_t len = enc.size();
    wassert(actual(types::decodeEnvelope(buf, len)) == code);
    wassert(actual(len) == inner_enc_size);
    wassert(actual(types::decodeInner(code, buf, len)) == act);

    // String encoding
    wassert(actual(types::decodeString(code, wibble::str::fmt(act))) == act);

    // JSON encoding
    {
        std::stringstream jbuf;
        emitter::JSON json(jbuf);
        act->serialise(json);
        jbuf.seekg(0);
        emitter::Memory parsed;
        emitter::JSON::parse(jbuf, parsed);
        wassert(actual(parsed.root().is_mapping()).istrue());
        arki::Item<> iparsed = types::decodeMapping(parsed.root().get_mapping());
        wassert(actual(iparsed) == act);
    }
}

void TestItemCompares::check(WIBBLE_TEST_LOCPRM) const
{
    wassert(actual(act) == act);
    wassert(actual(higher1) == higher1);
    wassert(actual(higher2) == higher2);

    wassert(actual(act) < higher1);
    wassert(actual(act) <= higher1);
    wassert(not (actual(act) == higher1));
    wassert(actual(act) != higher1);
    wassert(not (actual(act) >= higher1));
    wassert(not (actual(act) > higher1));

    wassert(not (actual(higher1) < higher2));
    wassert(actual(higher1) <= higher2);
    wassert(actual(higher1) == higher2);
    wassert(not (actual(higher1) != higher2));
    wassert(actual(higher1) >= higher2);
    wassert(not (actual(higher1) > higher2));
}

void TestSourceblobIs::check(WIBBLE_TEST_LOCPRM) const
{
    arki::Item<types::source::Blob> blob = act.upcast<types::source::Blob>();
    if (!blob.defined())
    {
        std::stringstream ss;
        ss << "metadata item '" << blob << "' is not a source::Blob";
        wibble_test_location.fail_test(ss.str());
    }

    wassert(actual(blob->format) == format);
    wassert(actual(blob->basedir) == basedir);
    wassert(actual(blob->filename) == fname);
    wassert(actual(blob->offset) == ofs);
    wassert(actual(blob->size) == size);
    wassert(actual(blob->absolutePathname()) == sys::fs::abspath(str::joinpath(basedir, fname)));
}

void TestGenericType::check(WIBBLE_TEST_LOCPRM) const
{
    WIBBLE_TEST_INFO(tinfo);

    tinfo() << "current: " << sample;
    UItem<> i_sample;
    wrunchecked(i_sample = types::decodeString(code, sample));
    wassert(actual(i_sample->serialisationCode()) == code);
    wassert(actual(i_sample).serializes());
    wassert(actual(i_sample) == sample);

    vector< Item<> > ilowers;
    for (std::vector<std::string>::const_iterator i = lower.begin();
            i != lower.end(); ++i)
    {
        tinfo() << "current: " << *i;
        UItem<> ii;
        wrunchecked(ii = types::decodeString(code, *i));
        wassert(actual(ii->serialisationCode()) == code);
        wassert(actual(ii).serializes());
        wassert(actual(ii) == *i);
        ilowers.push_back(ii);
    }

    vector< Item<> > ihighers;
    for (std::vector<std::string>::const_iterator i = higher.begin();
            i != higher.end(); ++i)
    {
        tinfo() << "current: " << *i;
        UItem<> ii;
        wrunchecked(ii = types::decodeString(code, *i));
        wassert(actual(ii->serialisationCode()) == code);
        wassert(actual(ii).serializes());
        wassert(actual(ii) == *i);
        ihighers.push_back(ii);
    }
}

}
}
