#include "arki/dataset/tests.h"
#include "arki/dataset.h"
#include "arki/dataset/time.h"
#include "arki/dataset/query.h"
#include "arki/dataset/session.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/types/reftime.h"
#include "arki/utils/files.h"
#include "arki/utils/accounting.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/exceptions.h"
#include "arki/segment/dir.h"
#include <sys/fcntl.h>

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;
using namespace arki::core;

namespace {

struct ForceDirMockDataSession : public arki::dataset::Session
{
public:
    std::shared_ptr<arki::segment::Writer> segment_writer(const segment::WriterConfig& writer_config, const std::string& format, const std::string& root, const std::string& relpath) override
    {
        std::string abspath = str::joinpath(root, relpath);
        return std::shared_ptr<arki::segment::Writer>(new arki::segment::dir::HoleWriter(writer_config, format, root, relpath, abspath));
    }
};

template<class Data>
struct FixtureWriter : public DatasetTest
{
    using DatasetTest::DatasetTest;

    Data td;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            step = daily
            unique = product, area, reftime
        )");
    }

    bool smallfiles() const
    {
        return cfg->value_bool("smallfiles") || (td.format == "vm2" && cfg->value("type") == "simple");
    }
};

class Tests : public FixtureTestCase<DatasetTest>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests test_writer_ondisk2("arki_dataset_writer_ondisk2", "type=ondisk2\n");
Tests test_writer_simple_plain("arki_dataset_writer_simple_plain", "type=simple\nindex_type=plain\n");
Tests test_writer_simple_sqlite("arki_dataset_writer_simple_sqlite", "type=simple\nindex_type=sqlite");
Tests test_writer_iseg("arki_dataset_writer_iseg", "type=iseg\nformat=grib");

template<class Data>
class TestsWriter : public FixtureTestCase<FixtureWriter<Data>>
{
    using FixtureTestCase<FixtureWriter<Data>>::FixtureTestCase;

    void register_tests() override;
};

TestsWriter<GRIBData> test_writer_grib_ondisk2("arki_dataset_writer_grib_ondisk2", "type=ondisk2\nformat=grib\n");
TestsWriter<GRIBData> test_writer_grib_simple_plain("arki_dataset_writer_grib_simple_plain", "type=simple\nindex_type=plain\nformat=grib\n");
TestsWriter<GRIBData> test_writer_grib_simple_sqlite("arki_dataset_writer_grib_simple_sqlite", "type=simple\nindex_type=sqlite\nformat=grib\n");
TestsWriter<GRIBData> test_writer_grib_iseg("arki_dataset_writer_grib_iseg", "type=iseg\nformat=grib\n");
TestsWriter<BUFRData> test_writer_bufr_ondisk2("arki_dataset_writer_bufr_ondisk2", "type=ondisk2\nformat=bufr\n");
TestsWriter<BUFRData> test_writer_bufr_simple_plain("arki_dataset_writer_bufr_simple_plain", "type=simple\nindex_type=plain\nformat=bufr\n");
TestsWriter<BUFRData> test_writer_bufr_simple_sqlite("arki_dataset_writer_bufr_simple_sqlite", "type=simple\nindex_type=sqlite\nformat=bufr\n");
TestsWriter<BUFRData> test_writer_bufr_iseg("arki_dataset_writer_bufr_iseg", "type=iseg\nformat=bufr\n");
TestsWriter<VM2Data> test_writer_vm2_ondisk2("arki_dataset_writer_vm2_ondisk2", "type=ondisk2\nformat=vm2\n");
TestsWriter<VM2Data> test_writer_vm2_simple_plain("arki_dataset_writer_vm2_simple_plain", "type=simple\nindex_type=plain\nformat=vm2\n");
TestsWriter<VM2Data> test_writer_vm2_simple_sqlite("arki_dataset_writer_vm2_simple_sqlite", "type=simple\nindex_type=sqlite\nformat=vm2\n");
TestsWriter<VM2Data> test_writer_vm2_iseg("arki_dataset_writer_vm2_iseg", "type=iseg\nformat=vm2\n");
TestsWriter<ODIMData> test_writer_odim_ondisk2("arki_dataset_writer_odim_ondisk2", "type=ondisk2\nformat=odimh5\n");
TestsWriter<ODIMData> test_writer_odim_simple_plain("arki_dataset_writer_odim_simple_plain", "type=simple\nindex_type=plain\nformat=odimh5\n");
TestsWriter<ODIMData> test_writer_odim_simple_sqlite("arki_dataset_writer_odim_simple_sqlite", "type=simple\nindex_type=sqlite\nformat=odimh5\n");
TestsWriter<ODIMData> test_writer_odim_iseg("arki_dataset_writer_odim_iseg", "type=iseg\nformat=odimh5\n");

void Tests::register_tests() {

// Test a dataset with very large mock files in it
add_method("import_largefile", [](Fixture& f) {
    skip_unless_filesystem_has_holes(".");
    // A dataset with hole files
    f.cfg->set("step", "daily");
    f.set_session(std::make_shared<ForceDirMockDataSession>());


    {
        // Import 24*30*10Mb=7.2Gb of data
        auto writer = f.config().create_writer();
        for (unsigned day = 1; day <= 30; ++day)
        {
            for (unsigned hour = 0; hour < 24; ++hour)
            {
                Metadata md = make_large_mock("grib", 10*1024*1024, 12, day, hour);
                wassert(actual(*writer).import(md));
            }
        }
        writer->flush();
    }

    auto checker = f.config().create_checker();
    wassert(actual(*checker).check_clean());

    // Query it, without data
    auto reader = f.config().create_reader();
    metadata::Collection mdc(*reader, "");
    wassert(actual(mdc.size()) == 720u);

    // Query it, streaming its data to /dev/null
    sys::File out("/dev/null", O_WRONLY);
    dataset::ByteQuery bq;
    bq.setData(Matcher());
    wassert(reader->query_bytes(bq, out));
});

add_method("import_batch_replace_usn", [](Fixture& f) {
    f.cfg->set("format", "bufr");
    f.cfg->set("step", "daily");
    metadata::TestCollection mdc("inbound/synop-gts.bufr");
    metadata::TestCollection mdc_upd("inbound/synop-gts-usn2.bufr");

    auto ds = f.config().create_writer();

    dataset::WriterBatch batch;
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mdc[0]));
    wassert(ds->acquire_batch(batch, dataset::REPLACE_HIGHER_USN));
    wassert(actual(batch[0]->result) == dataset::ACQ_OK);
    wassert(actual(batch[0]->dataset_name) == "testds");
    wassert(actual_file(str::joinpath(f.ds_root, "2009/12-04.bufr")).exists());
    wassert(actual_type(mdc[0].source()).is_source_blob("bufr", f.ds_root, "2009/12-04.bufr"));

    // Acquire again: it works, since USNs the same as the existing ones do overwrite
    wassert(ds->acquire_batch(batch, dataset::REPLACE_HIGHER_USN));
    wassert(actual(batch[0]->result) == dataset::ACQ_OK);
    wassert(actual(batch[0]->dataset_name) == "testds");

    // Acquire with a newer USN: it works
    batch.clear();
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mdc_upd[0]));
    wassert(ds->acquire_batch(batch, dataset::REPLACE_HIGHER_USN));
    wassert(actual(batch[0]->result) == dataset::ACQ_OK);
    wassert(actual(batch[0]->dataset_name) == "testds");

    // Acquire with the lower USN: it fails
    batch.clear();
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mdc[0]));
    wassert(ds->acquire_batch(batch, dataset::REPLACE_HIGHER_USN));
    if (ds->type() == "simple")
    {
        wassert(actual(batch[0]->result) == dataset::ACQ_OK);
        wassert(actual(batch[0]->dataset_name) == "testds");
    } else {
        wassert(actual(batch[0]->result) == dataset::ACQ_ERROR_DUPLICATE);
        wassert(actual(batch[0]->dataset_name) == "");
    }
});

add_method("issue237", [](Fixture& f) {
    f.cfg->set("format", "vm2");
    f.cfg->set("step", "daily");
    f.cfg->set("smallfiles", "yes");
    metadata::TestCollection mdc("inbound/issue237.vm2", true);
    wassert(actual_type(mdc[0].source()).is_source_blob("vm2", sys::abspath("."), "inbound/issue237.vm2", 0, 36));

    // Acquire value
    {
        auto ds = f.config().create_writer();
        wassert(actual(ds->acquire(mdc[0], dataset::REPLACE_NEVER)) == dataset::ACQ_OK);
        wassert(actual_type(mdc[0].source()).is_source_blob("vm2", f.ds_root, "2020/10-31.vm2", 0, 36));
    }

    // Read it back
    {
        metadata::Collection mdc1(*f.config().create_reader(), Matcher());
        wassert(actual(mdc1.size()) == 1u);
        wassert(actual_type(mdc1[0].source()).is_source_blob("vm2", f.ds_root, "2020/10-31.vm2", 0, 36));
        auto data = mdc1[0].get_data().read();
        wassert(actual(std::string((const char*)data.data(), data.size())) == "202010312300,12865,158,9.409990,,,");
    }

    wassert(actual_file(str::joinpath(f.ds_root, "2020/10-31.vm2")).contents_equal("20201031230000,12865,158,9.409990,,,\n"));

    auto state = f.scan_state();
    wassert(actual(state.size()) == 1u);
    wassert(actual(state.get("testds:2020/10-31.vm2").state) == segment::SEGMENT_OK);
});

}


template<class Data>
void TestsWriter<Data>::register_tests() {

typedef FixtureWriter<Data> Fixture;

this->add_method("import", [](Fixture& f) {
    auto ds = f.config().create_writer();

    for (unsigned i = 0; i < 3; ++i)
    {
        Metadata md = f.td.mds[i];
        wassert(actual(*ds).import(md));
        wassert(actual_file(str::joinpath(f.ds_root, f.destfile(f.td.mds[i]))).exists());
        wassert(actual_type(md.source()).is_source_blob(f.td.format, f.ds_root, f.destfile(f.td.mds[i])));
    }
});

this->add_method("import_error", [](Fixture& f) {
    std::string format = f.cfg->value("format");
    Metadata md;
    fill(md);
    md.set("reftime", "2018-01-01T00:00:00");
    md.set_source_inline(format, metadata::DataManager::get().to_unreadable_data(1));

    auto ds = f.config().create_writer();
    wassert(actual(ds->acquire(md, dataset::REPLACE_NEVER)) == dataset::ACQ_ERROR);

    auto state = f.scan_state();
    wassert(actual(state.size()) == 1u);
    wassert(actual(state.get("testds:2018/01-01." + format).state) == segment::SEGMENT_DELETED);
    wassert(f.query_results({}));
});

this->add_method("import_batch_replace_never", [](Fixture& f) {
    auto ds = f.config().create_writer();

    dataset::WriterBatch batch;
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(f.td.mds[0]));
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(f.td.mds[1]));
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(f.td.mds[2]));
    wassert(ds->acquire_batch(batch, dataset::REPLACE_NEVER));
    for (unsigned i = 0; i < 3; ++i)
    {
        wassert(actual(batch[i]->result) == dataset::ACQ_OK);
        wassert(actual(batch[i]->dataset_name) == "testds");
        wassert(actual_file(str::joinpath(f.ds_root, f.destfile(f.td.mds[i]))).exists());
        wassert(actual_type(f.td.mds[i].source()).is_source_blob(f.td.format, f.ds_root, f.destfile(f.td.mds[i])));
    }

    Metadata mds[3] = { f.td.mds[0], f.td.mds[1], f.td.mds[2] };
    batch.clear();
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mds[0]));
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mds[1]));
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mds[2]));
    wassert(ds->acquire_batch(batch, dataset::REPLACE_NEVER));
    for (unsigned i = 0; i < 3; ++i)
    {
        if (ds->type() == "simple")
        {
            wassert(actual(batch[i]->result) == dataset::ACQ_OK);
            wassert(actual(batch[i]->dataset_name) == "testds");
            wassert(actual(mds[i].sourceBlob().absolutePathname()) == f.td.mds[i].sourceBlob().absolutePathname());
            wassert(actual(mds[i].sourceBlob().offset) > f.td.mds[i].sourceBlob().offset);
            wassert(actual(mds[i].sourceBlob().size) == f.td.mds[i].sourceBlob().size);
        } else {
            wassert(actual(batch[i]->result) == dataset::ACQ_ERROR_DUPLICATE);
            wassert(actual(batch[i]->dataset_name) == "");
        }
    }
});

this->add_method("import_batch_replace_always", [](Fixture& f) {
    auto ds = f.config().create_writer();

    dataset::WriterBatch batch;
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(f.td.mds[0]));
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(f.td.mds[1]));
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(f.td.mds[2]));
    wassert(ds->acquire_batch(batch, dataset::REPLACE_ALWAYS));

    for (unsigned i = 0; i < 3; ++i)
    {
        wassert(actual(batch[i]->result) == dataset::ACQ_OK);
        wassert(actual(batch[i]->dataset_name) == "testds");
        wassert(actual_file(str::joinpath(f.ds_root, f.destfile(f.td.mds[i]))).exists());
        wassert(actual_type(f.td.mds[i].source()).is_source_blob(f.td.format, f.ds_root, f.destfile(f.td.mds[i])));
    }

    Metadata mds[3] = { f.td.mds[0], f.td.mds[1], f.td.mds[2] };
    batch.clear();
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mds[0]));
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mds[1]));
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mds[2]));
    wassert(ds->acquire_batch(batch, dataset::REPLACE_ALWAYS));
    for (unsigned i = 0; i < 3; ++i)
    {
        wassert(actual(batch[i]->result) == dataset::ACQ_OK);
        wassert(actual(batch[i]->dataset_name) == "testds");
        wassert(actual(mds[i].sourceBlob().absolutePathname()) == f.td.mds[i].sourceBlob().absolutePathname());
        wassert(actual(mds[i].sourceBlob().offset) > f.td.mds[i].sourceBlob().offset);
        wassert(actual(mds[i].sourceBlob().size) == f.td.mds[i].sourceBlob().size);
    }
});

this->add_method("import_before_archive_age", [](Fixture& f) {
    auto o = dataset::SessionTime::local_override(1483225200); // date +%s --date="2017-01-01"
    f.cfg->set("archive age", "1");
    auto ds = f.config().create_writer();

    for (unsigned i = 0; i < 3; ++i)
    {
        Metadata md = f.td.mds[i];
        wassert(actual(ds->acquire(md)) == dataset::ACQ_ERROR);
        wassert(actual(md.notes().back().content).contains("is older than archive age"));
    }

    metadata::Collection mdc(*f.config().create_reader(), Matcher());
    wassert(actual(mdc.size()) == 0u);
});

this->add_method("import_before_delete_age", [](Fixture& f) {
    auto o = dataset::SessionTime::local_override(1483225200); // date +%s --date="2017-01-01"
    f.cfg->set("delete age", "1");
    auto ds = f.config().create_writer();

    for (unsigned i = 0; i < 3; ++i)
    {
        Metadata md = f.td.mds[i];
        wassert(actual(*ds).import(md));
        wassert(actual(md.notes().back().content).contains("is older than delete age"));
    }

    metadata::Collection mdc(*f.config().create_reader(), Matcher());
    wassert(actual(mdc.size()) == 0u);
});

this->add_method("second_resolution", [](Fixture& f) {
    Metadata md(f.td.mds[1]);
    md.set(types::Reftime::createPosition(Time(2007, 7, 7, 0, 0, 0)));

    // Import a first metadata to create a segment to repack
    {
        auto writer = f.config().create_writer();
        wassert(actual(*writer).import(md));
    }

    md.set(types::Reftime::createPosition(Time(2007, 7, 7, 0, 0, 1)));
    {
        auto writer = f.config().create_writer();
        wassert(actual(*writer).import(md));
    }

    wassert(f.ensure_localds_clean(1, 2));
});

auto test_same_segment_fail = [](Fixture& f, unsigned fail_idx, dataset::ReplaceStrategy strategy) {
    sys::rmtree_ifexists("testds");
    Metadata md;
    fill(md);
    std::string format = f.cfg->value("format");

    // Make a batch that ends up all in the same segment
    metadata::Collection mds;
    for (unsigned idx = 0; idx < 3; ++idx)
    {
        md.set(types::Reftime::createPosition(Time(2018, 1, 1, idx, 0, 0)));
        if (idx == fail_idx)
            md.set_source_inline(format, metadata::DataManager::get().to_unreadable_data(1));
        else
            md.set_source_inline(format, metadata::DataManager::get().to_data(format, std::vector<uint8_t>{(unsigned char)idx}));
        mds.push_back(md);
    }

    auto ds = f.config().create_writer();
    dataset::WriterBatch batch = mds.make_import_batch();
    wassert(ds->acquire_batch(batch, strategy));
    wassert(actual(batch[0]->result) == dataset::ACQ_ERROR);
    wassert(actual(batch[1]->result) == dataset::ACQ_ERROR);
    wassert(actual(batch[2]->result) == dataset::ACQ_ERROR);

    auto state = f.scan_state();
    wassert(actual(state.size()) == 1u);
    wassert(actual(state.get("testds:2018/01-01." + format).state) == segment::SEGMENT_DELETED);
    wassert(f.query_results({}));
};

this->add_method("transaction_same_segment_fail_first", [=](Fixture& f) {
    wassert(test_same_segment_fail(f, 0, dataset::REPLACE_NEVER));
    wassert(test_same_segment_fail(f, 0, dataset::REPLACE_ALWAYS));
    wassert(test_same_segment_fail(f, 0, dataset::REPLACE_HIGHER_USN));
});

this->add_method("transaction_same_segment_fail_middle", [=](Fixture& f) {
    wassert(test_same_segment_fail(f, 1, dataset::REPLACE_NEVER));
    wassert(test_same_segment_fail(f, 1, dataset::REPLACE_ALWAYS));
    wassert(test_same_segment_fail(f, 1, dataset::REPLACE_HIGHER_USN));
});

this->add_method("transaction_same_segment_fail_last", [=](Fixture& f) {
    wassert(test_same_segment_fail(f, 2, dataset::REPLACE_NEVER));
    wassert(test_same_segment_fail(f, 2, dataset::REPLACE_ALWAYS));
    wassert(test_same_segment_fail(f, 2, dataset::REPLACE_HIGHER_USN));
});

auto test_different_segment_fail = [](Fixture& f, unsigned fail_idx, dataset::ReplaceStrategy strategy) {
    sys::rmtree_ifexists("testds");
    Metadata md;
    fill(md);
    std::string format = f.cfg->value("format");

    // Make a batch that ends up all in the same segment
    metadata::Collection mds;
    for (unsigned idx = 0; idx < 3; ++idx)
    {
        md.set(types::Reftime::createPosition(Time(2018, idx + 1, 1, 0, 0, 0)));
        if (idx == fail_idx)
            md.set_source_inline(format, metadata::DataManager::get().to_unreadable_data(1));
        else
            md.set_source_inline(format, metadata::DataManager::get().to_data(format, std::vector<uint8_t>{(unsigned char)idx}));
        mds.push_back(md);
    }

    auto ds = f.config().create_writer();
    dataset::WriterBatch batch = mds.make_import_batch();
    wassert(ds->acquire_batch(batch, strategy));
    wassert(actual(batch[0]->result) == (fail_idx == 0 ? dataset::ACQ_ERROR : dataset::ACQ_OK));
    wassert(actual(batch[1]->result) == (fail_idx == 1 ? dataset::ACQ_ERROR : dataset::ACQ_OK));
    wassert(actual(batch[2]->result) == (fail_idx == 2 ? dataset::ACQ_ERROR : dataset::ACQ_OK));
    ds->flush();

    auto state = f.scan_state();
    wassert(actual(state.size()) == 3u);
    wassert(actual(state.get("testds:2018/01-01." + format).state) == (fail_idx == 0 ? segment::SEGMENT_DELETED : segment::SEGMENT_OK));
    wassert(actual(state.get("testds:2018/02-01." + format).state) == (fail_idx == 1 ? segment::SEGMENT_DELETED : segment::SEGMENT_OK));
    wassert(actual(state.get("testds:2018/03-01." + format).state) == (fail_idx == 2 ? segment::SEGMENT_DELETED : segment::SEGMENT_OK));

    metadata::Collection res = f.query(dataset::DataQuery());
    wassert(actual(res.size()) == 2u);
};

this->add_method("transaction_different_segment_fail_first", [=](Fixture& f) {
    wassert(test_different_segment_fail(f, 0, dataset::REPLACE_NEVER));
    wassert(test_different_segment_fail(f, 0, dataset::REPLACE_ALWAYS));
    wassert(test_different_segment_fail(f, 0, dataset::REPLACE_HIGHER_USN));
});

this->add_method("transaction_different_segment_fail_middle", [=](Fixture& f) {
    wassert(test_different_segment_fail(f, 1, dataset::REPLACE_NEVER));
    wassert(test_different_segment_fail(f, 1, dataset::REPLACE_ALWAYS));
    wassert(test_different_segment_fail(f, 1, dataset::REPLACE_HIGHER_USN));
});

this->add_method("transaction_different_segment_fail_last", [=](Fixture& f) {
    wassert(test_different_segment_fail(f, 2, dataset::REPLACE_NEVER));
    wassert(test_different_segment_fail(f, 2, dataset::REPLACE_ALWAYS));
    wassert(test_different_segment_fail(f, 2, dataset::REPLACE_HIGHER_USN));
});

this->add_method("test_acquire", [](Fixture& f) {
    // TODO: add tests for test_acquire
});

this->add_method("import_eatmydata", [](Fixture& f) {
    f.cfg->set("eatmydata", "yes");

    {
        auto ds = f.config().create_writer();
        for (auto& md: f.td.mds)
            wassert(actual(*ds).import(*md));

        dataset::WriterBatch batch;
        for (auto& md: f.td.mds)
            batch.emplace_back(make_shared<dataset::WriterBatchElement>(*md));
        wassert(ds->acquire_batch(batch, dataset::REPLACE_ALWAYS));
    }

    metadata::Collection mdc(*f.config().create_reader(), "");
    if (f.cfg->value("type") == "simple")
        wassert(actual(mdc.size()) == 6u);
    else
        wassert(actual(mdc.size()) == 3u);
});


}
}
