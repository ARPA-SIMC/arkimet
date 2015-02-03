/*
 * Copyright (C) 2012--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/tests.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <wibble/sys/fs.h>
#include "data.h"
#include "data/lines.h"

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::dataset;
using namespace arki::types;
using namespace wibble;
using namespace wibble::tests;

struct arki_dataset_data_shar {
};

TESTGRP(arki_dataset_data);

namespace {

class TestItem
{
public:
    std::string relname;

    TestItem(const std::string& relname) : relname(relname) {}
};

}

// Test the item cache
template<> template<>
void to::test<1>()
{
    data::impl::Cache<TestItem, 3> cache;

    // Empty cache does not return things
    wassert(actual(cache.get("foo") == 0).istrue());

    // Add returns the item we added
    TestItem* foo = new TestItem("foo");
    wassert(actual(cache.add(auto_ptr<TestItem>(foo)) == foo).istrue());

    // Get returns foo
    wassert(actual(cache.get("foo") == foo).istrue());

    // Add two more items
    TestItem* bar = new TestItem("bar");
    wassert(actual(cache.add(auto_ptr<TestItem>(bar)) == bar).istrue());
    TestItem* baz = new TestItem("baz");
    wassert(actual(cache.add(auto_ptr<TestItem>(baz)) == baz).istrue());

    // With max_size=3, the cache should hold them all
    wassert(actual(cache.get("foo") == foo).istrue());
    wassert(actual(cache.get("bar") == bar).istrue());
    wassert(actual(cache.get("baz") == baz).istrue());

    // Add an extra item: the last recently used was foo, which gets popped
    TestItem* gnu = new TestItem("gnu");
    wassert(actual(cache.add(auto_ptr<TestItem>(gnu)) == gnu).istrue());

    // Foo is not in cache anymore, bar baz and gnu are
    wassert(actual(cache.get("foo") == 0).istrue());
    wassert(actual(cache.get("bar") == bar).istrue());
    wassert(actual(cache.get("baz") == baz).istrue());
    wassert(actual(cache.get("gnu") == gnu).istrue());
}

namespace {
struct TestSegments
{
    ConfigFile cfg;
    std::string pathname;
    //const testdata::Fixture& td;

    /**
     * cfg_segments can be:
     *  - dir: directory segments
     *  - (nothing): AutoSegmentManager
     */
    TestSegments(const std::string& cfg_segments=std::string())
         : pathname("testsegment")
    {
        if (sys::fs::isdir(pathname))
            sys::fs::rmtree(pathname);
        else
            sys::fs::deleteIfExists(pathname);

        cfg.setValue("path", pathname);
        if (!cfg_segments.empty())
            cfg.setValue("segments", cfg_segments);
    }

    void test_repack(WIBBLE_TEST_LOCPRM)
    {
        ConfigFile cfg1(cfg);
        cfg1.setValue("mockdata", "true");
        auto_ptr<data::SegmentManager> segment_manager(data::SegmentManager::get(cfg1));

        // Import 2 gigabytes of data in a single segment
        metadata::Collection mds;
        data::Writer* w = segment_manager->get_writer("test.grib");
        for (unsigned i = 0; i < 2048; ++i)
        {
            auto_ptr<Metadata> md(new Metadata(testdata::make_large_mock("grib", 1024*1024, i / (30 * 24), (i/24) % 30, i % 24)));
            w->append(*md);
            wassert(actual(md->source().style()) == Source::BLOB);
            md->drop_cached_data();
            mds.eat(md);
        }

        // Repack it
        segment_manager->repack("test.grib", mds);
    }
};
}

template<> template<>
void to::test<2>()
{
    TestSegments ts;
    wruntest(ts.test_repack);
}

template<> template<>
void to::test<3>()
{
    TestSegments ts("dir");
    wruntest(ts.test_repack);
}

}
