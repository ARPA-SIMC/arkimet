/*
 * Copyright (C) 2007,2008,2009  Enrico Zini <enrico@enricozini.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

#include <arki/tests/test-utils.h>
#include <arki/dataset.h>
#include <arki/dataset/http.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/scan/grib.h>
#include <arki/dispatcher.h>
#include <arki/types/source.h>
#include <arki/utils.h>
#include <wibble/string.h>
#include <wibble/stream/posix.h>

#include <sstream>
#include <iostream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace wibble;

struct MetadataCollector : public vector<Metadata>, public MetadataConsumer
{
    bool operator()(Metadata& md)
    {
        push_back(md);
        return true;
    }
};

struct arki_server_shar {
    ConfigFile config;

    arki_server_shar()
    {
    }
};
TESTGRP(arki_server);

// Query the configuration
template<> template<>
void to::test<1>()
{
    dataset::HTTP::readConfig("http://localhost:7117", config);

    ensure(config.section("test200") != 0);
    ensure_equals(config.section("test200")->value("server"), "http://localhost:7117");

    ensure(config.section("test80") != 0);
    ensure_equals(config.section("test80")->value("server"), "http://localhost:7117");

    ensure(config.section("error") != 0);
    ensure_equals(config.section("error")->value("server"), "http://localhost:7117");
}

// Test querying the datasets
template<> template<>
void to::test<2>()
{
    dataset::HTTP::readConfig("http://localhost:7117", config);
    auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test200")));
    MetadataCollector mdc;

    testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
    ensure_equals(mdc.size(), 1u);

    // Check that the source record that comes out is ok
    UItem<Source> source = mdc[0].source;
    ensure_equals(source->style(), Source::BLOB);
    ensure_equals(source->format, "grib1");
    UItem<source::Blob> blob = source.upcast<source::Blob>();
    ensure(str::endsWith(blob->filename, "test200/2007/07-08.grib1"));
    ensure_equals(blob->offset, 0u);
    ensure_equals(blob->size, 7218u);

    mdc.clear();
    testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
    ensure_equals(mdc.size(), 0u);

    mdc.clear();
    testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98"), false), mdc);
    ensure_equals(mdc.size(), 0u);
}

// Test querying the summary
template<> template<>
void to::test<3>()
{
    dataset::HTTP::readConfig("http://localhost:7117", config);
    auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test200")));

    Summary summary;
    testds->querySummary(Matcher::parse("origin:GRIB1,200"), summary);
    ensure_equals(summary.count(), 1u);
}

// Test querying with postprocessing
template<> template<>
void to::test<4>()
{
    dataset::HTTP::readConfig("http://localhost:7117", config);
    auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test200")));

    ensure(dynamic_cast<dataset::HTTP*>(testds.get()) != 0);

    // Contrarily to ondisk, HTTP can use a stringstream
    stringstream str;
    dataset::ByteQuery bq;
    bq.setPostprocess(Matcher::parse("origin:GRIB1,200"), "say ciao");
    testds->queryBytes(bq, str);
    ensure_equals(str.str(), "ciao\n");
}

// Test the server giving an error
template<> template<>
void to::test<5>()
{
    dataset::HTTP::readConfig("http://localhost:7117", config);
    auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test200")));

    dataset::HTTP* htd = dynamic_cast<dataset::HTTP*>(testds.get());
    ensure(htd != 0);

    // Try it on metadata
    MetadataCollector mdc;
    htd->produce_one_wrong_query();
    try {
        testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
        ensure(false);
    } catch (std::exception& e) {}
    ensure_equals(mdc.size(), 0u);

    // Try it on summaries
    Summary summary;
    htd->produce_one_wrong_query();
    try {
        testds->querySummary(Matcher::parse("origin:GRIB1,200"), summary);
        ensure(false);
    } catch (std::exception& e) {}
    ensure_equals(summary.count(), 0u);

    // Try it on streams
    stringstream str;
    htd->produce_one_wrong_query();
    try {
	dataset::ByteQuery bq;
	bq.setPostprocess(Matcher::parse("origin:GRIB1,200"), "say ciao");
        testds->queryBytes(bq, str);
        ensure(false);
    } catch (std::exception& e) {}
    ensure_equals(str.str().size(), 0u);
}

// Test expanding a query
template<> template<>
void to::test<6>()
{
    ensure_equals(dataset::HTTP::expandMatcher("origin:GRIB1,200;product:t", "http://localhost:7117"),
          "origin:GRIB1,200; product:GRIB1,200,2,11 or GRIB1,98,128,130 or GRIB1,98,128,167 or GRIB1,200,200,11");
    try {
        dataset::HTTP::expandMatcher("origin:GRIB1,200;product:pippo", "http://localhost:7117");
        ensure(false);
    } catch (...) {
        ensure(true);
    }
}

}

// vim:set ts=4 sw=4:
