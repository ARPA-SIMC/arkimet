#include <arki/types/tests.h>
#include <arki/dataset.h>
#include <arki/dataset/http.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/scan/grib.h>
#include <arki/dispatcher.h>
#include <arki/types/source/blob.h>
#include <arki/utils.h>
#include <arki/runtime/io.h>
#include <arki/runtime/processor.h>
#include <arki/wibble/string.h>
#include <arki/wibble/stream/posix.h>

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
using namespace wibble::tests;

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

// Test querying the datasets, metadata only
template<> template<>
void to::test<2>()
{
    dataset::HTTP::readConfig("http://localhost:7117", config);
    unique_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test200")));
    metadata::Collection mdc(*testds, Matcher::parse("origin:GRIB1,200"));
    ensure_equals(mdc.size(), 1u);

    // Check that the source record that comes out is ok
    wassert(actual_type(mdc[0].source()).is_source_url("grib1", "http://localhost:7117/dataset/test200/query"));

    mdc.clear();
    mdc.add(*testds, Matcher::parse("origin:GRIB1,80"));
    ensure_equals(mdc.size(), 0u);

    mdc.clear();
    mdc.add(*testds, Matcher::parse("origin:GRIB1,98"));
    ensure_equals(mdc.size(), 0u);
}

// Test querying the datasets, with inline data
template<> template<>
void to::test<3>()
{
    dataset::HTTP::readConfig("http://localhost:7117", config);
    unique_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test200")));
    metadata::Collection mdc(*testds, dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), true));
    ensure_equals(mdc.size(), 1u);

    // Check that the source record that comes out is ok
    wassert(actual_type(mdc[0].source()).is_source_inline("grib1", 7218));
    wassert(actual(mdc[0].getData().size()) == 7218);

    mdc.clear();
    mdc.add(*testds, dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), true));
    ensure_equals(mdc.size(), 0u);

    // Try again, but let ProcessorMaker build the query
    runtime::Tempfile output;
    runtime::ProcessorMaker pm;
    pm.data_inline = true;
    unique_ptr<runtime::DatasetProcessor> proc = pm.make(Matcher::parse("origin:GRIB1,200"), output);
    proc->process(*testds, "test200");
    proc->end();
    mdc.clear();
    Metadata::readFile(output.name(), mdc);
    wassert(actual(mdc.size()) == 1);
    wassert(actual_type(mdc[0].source()).is_source_inline("grib1", 7218));
    wassert(actual(mdc[0].getData().size()) == 7218);
}

// Test querying the summary
template<> template<>
void to::test<4>()
{
    dataset::HTTP::readConfig("http://localhost:7117", config);
    unique_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test200")));

    Summary summary;
    testds->querySummary(Matcher::parse("origin:GRIB1,200"), summary);
    ensure_equals(summary.count(), 1u);
}

// Test querying with postprocessing
template<> template<>
void to::test<5>()
{
    dataset::HTTP::readConfig("http://localhost:7117", config);
    unique_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test200")));

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
void to::test<6>()
{
    dataset::HTTP::readConfig("http://localhost:7117", config);
    unique_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test200")));

    dataset::HTTP* htd = dynamic_cast<dataset::HTTP*>(testds.get());
    ensure(htd != 0);

    // Try it on metadata
    metadata::Collection mdc;
    htd->produce_one_wrong_query();
    try {
        mdc.add(*testds, Matcher::parse("origin:GRIB1,200"));
        ensure(false);
    } catch (std::exception& e) {
        ensure_not_contains(e.what(), "<html>");
        ensure_contains(e.what(), "subexpression 'MISCHIEF' does not contain a colon");
    }
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
    ensure_equals(str.str(), "");
    ensure_equals(str.str().size(), 0u);
}

// Test expanding a query
template<> template<>
void to::test<7>()
{
    ensure_equals(dataset::HTTP::expandMatcher("origin:GRIB1,200;product:t", "http://localhost:7117"),
          "origin:GRIB1,200; product:GRIB1,200,2,11 or GRIB1,98,128,130 or GRIB1,98,128,167 or GRIB1,200,200,11 or GRIB2,200,0,200,11");
    try {
        dataset::HTTP::expandMatcher("origin:GRIB1,200;product:pippo", "http://localhost:7117");
        ensure(false);
    } catch (...) {
        ensure(true);
    }
}

// Test querying the datasets via macro
template<> template<>
void to::test<8>()
{
    ConfigFile cfg;
    cfg.setValue("name", "noop");
    cfg.setValue("type", "remote");
    cfg.setValue("path", "http://localhost:7117");
    cfg.setValue("qmacro", "test200");
    unique_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(cfg));
    metadata::Collection mdc(*testds, Matcher());
    ensure_equals(mdc.size(), 1u);
    // Check that the source record that comes out is ok
    wassert(actual_type(mdc[0].source()).is_source_url("grib1", "http://localhost:7117/dataset/test200/query"));
}

// Test querying the summary
template<> template<>
void to::test<9>()
{
    ConfigFile cfg;
    cfg.setValue("name", "noop");
    cfg.setValue("type", "remote");
    cfg.setValue("path", "http://localhost:7117");
    cfg.setValue("qmacro", "test200");
    unique_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(cfg));

    Summary summary;
    testds->querySummary(Matcher(), summary);
    ensure_equals(summary.count(), 1u);
}

// Test a postprocessor that outputs data and then exits with error
template<> template<>
void to::test<10>()
{
    ConfigFile cfg;
    dataset::HTTP::readConfig("http://localhost:7117/dataset/test200", cfg);
    unique_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*cfg.section("test200")));

    // Querying it should get the partial output and no error
    stringstream str;
    dataset::ByteQuery bq;
    bq.setPostprocess(Matcher::parse(""), "outthenerr");
    testds->queryBytes(bq, str);
    ensure_contains(str.str(), "So far, so good");

    // The postprocessor stderr should not appear
    ensure_not_contains(str.str(), "Argh");

    // And we should not get a server error after the normal stream has started
    ensure_not_contains(str.str(), "500 Server error");
}

// Test data integrity of postprocessed queries through a server (used to fail
// after offset 0xc00)
template<> template<>
void to::test<11>()
{
    using namespace arki::dataset;

    // Get the normal data
    string plain;
    {
        ConfigFile cfg;
        cfg.setValue("type", "test");
        cfg.setValue("path", "test80");
        cfg.setValue("name", "test80");
        cfg.setValue("step", "daily");
        cfg.setValue("filter", "origin:GRIB1,80");
        cfg.setValue("postprocess", "cat,echo,say,checkfiles,error,outthenerr");
        unique_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(cfg));

        DataQuery dq(Matcher::parse(""), true);
        ds->query_data(dq, [&](unique_ptr<Metadata> md) {
            md->makeInline();
            plain += md->encodeBinary();
            const auto& data = md->getData();
            plain.append((const char*)data.data(), data.size());
            return true;
        });
    }

    // Capture the data after going through the postprocessor
    stringstream postprocessed;
    {
        dataset::HTTP::readConfig("http://localhost:7117", config);
        unique_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("test80")));
        ensure(dynamic_cast<dataset::HTTP*>(testds.get()) != 0);

        dataset::ByteQuery bq;
        bq.setPostprocess(Matcher::parse("origin:GRIB1,80"), "cat");
        testds->queryBytes(bq, postprocessed);
    }

    ensure_equals(plain.size(), postprocessed.str().size());

    /*
    size_t diffs = 0;
    for (size_t i = 0; i < plain.size(); ++i)
    {
        if (plain[i] != postprocessed.str()[i])
        {
            fprintf(stderr, "Difference at offset %d %x %u!=%u\n", (int)i, (int)i, (int)plain[i], (int)postprocessed.str()[i]);
            if (++diffs > 20)
                break;
        }
    }

    ensure(plain == postprocessed.str());
    */

    metadata::Collection mdc1, mdc2;
    {
        stringstream s(plain);
        Metadata::readFile(s, metadata::ReadContext("", "plain"), mdc1);
    }
    {
        stringstream s(postprocessed.str());
        Metadata::readFile(s, metadata::ReadContext("", "postprocessed"), mdc2);
    }

    // Remove those metadata that contain test-dependent timestamps
    std::vector<Note> nonotes;
    mdc1[0].unset(types::TYPE_ASSIGNEDDATASET);
    mdc2[0].unset(types::TYPE_ASSIGNEDDATASET);
    mdc1[0].set_notes(nonotes);
    mdc2[0].set_notes(nonotes);

    // Compare YAML versions so we get readable output
    string yaml1, yaml2;
    {
        stringstream s1;
        mdc1[0].writeYaml(s1);
        yaml1 = s1.str();
    }
    {
        stringstream s1;
        mdc2[0].writeYaml(s1);
        yaml2 = s1.str();
    }
    ensure_equals(yaml1, yaml2);

    // Compare data
    const auto& d1 = mdc1[0].getData();
    const auto& d2 = mdc2[0].getData();

    ensure(d1 == d2);
}


}
