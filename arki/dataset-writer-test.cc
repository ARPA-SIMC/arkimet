#include "arki/dataset/tests.h"
#include "arki/dataset.h"
#include "arki/dataset/time.h"
#include "arki/query.h"
#include "arki/dataset/session.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/types/reftime.h"
#include "arki/types/note.h"
#include "arki/utils/files.h"
#include "arki/utils/accounting.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/stream.h"
#include "arki/exceptions.h"
#include "arki/segment/data/dir.h"
#include <sys/fcntl.h>

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;
using namespace arki::core;

namespace {

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
        return cfg->value_bool("smallfiles") || (td.format == DataFormat::VM2 && cfg->value("type") == "simple");
    }
};

class Tests : public FixtureTestCase<DatasetTest>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests test_writer_simple("arki_dataset_writer_simple", "type=simple");
Tests test_writer_iseg("arki_dataset_writer_iseg", "type=iseg\nformat=grib");

template<class Data>
class TestsWriter : public FixtureTestCase<FixtureWriter<Data>>
{
    using FixtureTestCase<FixtureWriter<Data>>::FixtureTestCase;

    void register_tests() override;
};

TestsWriter<GRIBData> test_writer_grib_simple("arki_dataset_writer_grib_simple", "type=simple\nformat=grib\n");
TestsWriter<GRIBData> test_writer_grib_iseg("arki_dataset_writer_grib_iseg", "type=iseg\nformat=grib\n");
TestsWriter<BUFRData> test_writer_bufr_simple("arki_dataset_writer_bufr_simple", "type=simple\nformat=bufr\n");
TestsWriter<BUFRData> test_writer_bufr_iseg("arki_dataset_writer_bufr_iseg", "type=iseg\nformat=bufr\n");
TestsWriter<VM2Data> test_writer_vm2_simple("arki_dataset_writer_vm2_simple", "type=simple\nformat=vm2\n");
TestsWriter<VM2Data> test_writer_vm2_iseg("arki_dataset_writer_vm2_iseg", "type=iseg\nformat=vm2\n");
TestsWriter<ODIMData> test_writer_odim_simple("arki_dataset_writer_odim_simple", "type=simple\nformat=odimh5\n");
TestsWriter<ODIMData> test_writer_odim_iseg("arki_dataset_writer_odim_iseg", "type=iseg\nformat=odimh5\n");
TestsWriter<NCData> test_writer_nc_simple("arki_dataset_writer_nc_simple", "type=simple\nformat=nc\n");
TestsWriter<NCData> test_writer_nc_iseg("arki_dataset_writer_nc_iseg", "type=iseg\nformat=nc\n");
TestsWriter<JPEGData> test_writer_jpeg_simple("arki_dataset_writer_jpeg_simple", "type=simple\nformat=nc\n");
TestsWriter<JPEGData> test_writer_jpeg_iseg("arki_dataset_writer_jpeg_iseg", "type=iseg\nformat=jpeg\n");

void Tests::register_tests() {

// Test a dataset with very large mock files in it
add_method("import_largefile", [](Fixture& f) {
    skip_unless_filesystem_has_holes(".");
    // A dataset with hole files
    f.cfg->set("step", "daily");
    f.segment_session()->default_file_segment = segment::DefaultFileSegment::SEGMENT_DIR;
    f.segment_session()->mock_data = true;


    {
        // Import 24*30*10Mb=7.2Gb of data
        auto writer = f.config().create_writer();
        for (unsigned day = 1; day <= 30; ++day)
        {
            for (unsigned hour = 0; hour < 24; ++hour)
            {
                auto md = make_large_mock(DataFormat::GRIB, 10*1024*1024, 12, day, hour);
                wassert(actual(*writer).import(*md));
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
    auto out = StreamOutput::create_discard();
    query::Bytes bq;
    bq.setData(Matcher());
    wassert(reader->query_bytes(bq, *out));
});

add_method("import_batch_replace_usn", [](Fixture& f) {
    f.cfg->set("format", "bufr");
    f.cfg->set("step", "daily");
    metadata::TestCollection mdc("inbound/synop-gts.bufr");
    metadata::TestCollection mdc_upd("inbound/synop-gts-usn2.bufr");

    auto ds = f.config().create_writer();

    metadata::InboundBatch batch;
    batch.emplace_back(make_shared<metadata::Inbound>(mdc.get(0)));
    wassert(ds->acquire_batch(batch, dataset::REPLACE_HIGHER_USN));
    wassert(actual(batch[0]->result) == metadata::Inbound::Result::OK);
    wassert(actual(batch[0]->destination) == "testds");
    wassert(actual_file(f.ds_root / "2009/12-04.bufr").exists());
    wassert(actual_type(mdc[0].source()).is_source_blob(DataFormat::BUFR, f.ds_root, "2009/12-04.bufr"));

    // Acquire again: it works, since USNs the same as the existing ones do overwrite
    wassert(ds->acquire_batch(batch, dataset::REPLACE_HIGHER_USN));
    wassert(actual(batch[0]->result) == metadata::Inbound::Result::OK);
    wassert(actual(batch[0]->destination) == "testds");

    // Acquire with a newer USN: it works
    batch.clear();
    batch.emplace_back(make_shared<metadata::Inbound>(mdc_upd.get(0)));
    wassert(ds->acquire_batch(batch, dataset::REPLACE_HIGHER_USN));
    wassert(actual(batch[0]->result) == metadata::Inbound::Result::OK);
    wassert(actual(batch[0]->destination) == "testds");

    // Acquire with the lower USN: it fails
    batch.clear();
    batch.emplace_back(make_shared<metadata::Inbound>(mdc.get(0)));
    wassert(ds->acquire_batch(batch, dataset::REPLACE_HIGHER_USN));
    if (ds->type() == "simple")
    {
        wassert(actual(batch[0]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[0]->destination) == "testds");
    } else {
        wassert(actual(batch[0]->result) == metadata::Inbound::Result::DUPLICATE);
        wassert(actual(batch[0]->destination) == "");
    }
});

add_method("issue237", [](Fixture& f) {
    skip_unless_vm2();
    f.cfg->set("format", "vm2");
    f.cfg->set("step", "daily");
    f.cfg->set("smallfiles", "yes");
    metadata::TestCollection mdc("inbound/issue237.vm2", true);
    wassert(actual_type(mdc[0].source()).is_source_blob(DataFormat::VM2, std::filesystem::current_path(), "inbound/issue237.vm2", 0, 36));

    // Acquire value
    {
        auto ds = f.config().create_writer();
        wassert(actual(ds->acquire(mdc[0], dataset::REPLACE_NEVER)) == metadata::Inbound::Result::OK);
        wassert(actual_type(mdc[0].source()).is_source_blob(DataFormat::VM2, f.ds_root, "2020/10-31.vm2", 0, 36));
    }

    // Read it back
    {
        metadata::Collection mdc1(*f.config().create_reader(), Matcher());
        wassert(actual(mdc1.size()) == 1u);
        wassert(actual_type(mdc1[0].source()).is_source_blob(DataFormat::VM2, f.ds_root, "2020/10-31.vm2", 0, 36));
        auto data = mdc1[0].get_data().read();
        wassert(actual(std::string((const char*)data.data(), data.size())) == "202010312300,12865,158,9.409990,,,");
    }

    wassert(actual_file(f.ds_root / "2020/10-31.vm2").contents_equal("20201031230000,12865,158,9.409990,,,\n"));

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
        std::shared_ptr<Metadata> md(f.td.mds[i].clone());
        wassert(actual(*ds).import(*md));
        wassert(actual_file(f.ds_root / f.destfile(f.td.mds[i])).exists());
        wassert(actual_type(md->source()).is_source_blob(f.td.format, f.ds_root, f.destfile(f.td.mds[i])));
    }
});

this->add_method("import_error", [](Fixture& f) {
    auto format = format_from_string(f.cfg->value("format"));
    Metadata md;
    fill(md);
    md.test_set("reftime", "2018-01-01T00:00:00");
    md.set_source_inline(format, metadata::DataManager::get().to_unreadable_data(1));

    auto ds = f.config().create_writer();
    wassert(actual(ds->acquire(md, dataset::REPLACE_NEVER)) == metadata::Inbound::Result::ERROR);

    auto state = f.scan_state();
    wassert(actual(state.size()) == 1u);
    wassert(actual(state.get("testds:2018/01-01." + format_name(format)).state) == segment::SEGMENT_DELETED);
    wassert(f.query_results({}));
});

this->add_method("import_batch_replace_never", [](Fixture& f) {
    auto ds = f.config().create_writer();

    metadata::InboundBatch batch = f.td.mds.make_batch();
    wassert(ds->acquire_batch(batch, dataset::REPLACE_NEVER));
    for (unsigned i = 0; i < 3; ++i)
    {
        wassert(actual(batch[i]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[i]->destination) == "testds");
        wassert(actual_file(f.ds_root / f.destfile(f.td.mds[i])).exists());
        wassert(actual_type(f.td.mds[i].source()).is_source_blob(f.td.format, f.ds_root, f.destfile(f.td.mds[i])));
    }

    metadata::Collection mds = f.td.mds.clone();
    batch = mds.make_batch();
    wassert(ds->acquire_batch(batch, dataset::REPLACE_NEVER));
    for (unsigned i = 0; i < 3; ++i)
    {
        if (ds->type() == "simple")
        {
            wassert(actual(batch[i]->result) == metadata::Inbound::Result::OK);
            wassert(actual(batch[i]->destination) == "testds");
            wassert(actual(mds[i].sourceBlob().absolutePathname()) == f.td.mds[i].sourceBlob().absolutePathname());
            wassert(actual(mds[i].sourceBlob().offset) > f.td.mds[i].sourceBlob().offset);
            wassert(actual(mds[i].sourceBlob().size) == f.td.mds[i].sourceBlob().size);
        } else {
            wassert(actual(batch[i]->result) == metadata::Inbound::Result::DUPLICATE);
            wassert(actual(batch[i]->destination) == "");
        }
    }
});

this->add_method("import_batch_replace_always", [](Fixture& f) {
    auto ds = f.config().create_writer();

    metadata::InboundBatch batch = f.td.mds.make_batch();
    wassert(ds->acquire_batch(batch, dataset::REPLACE_ALWAYS));

    for (unsigned i = 0; i < 3; ++i)
    {
        wassert(actual(batch[i]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[i]->destination) == "testds");
        wassert(actual_file(f.ds_root / f.destfile(f.td.mds[i])).exists());
        wassert(actual_type(f.td.mds[i].source()).is_source_blob(f.td.format, f.ds_root, f.destfile(f.td.mds[i])));
    }

    auto mds = f.td.mds.clone();
    batch = mds.make_batch();
    wassert(ds->acquire_batch(batch, dataset::REPLACE_ALWAYS));
    for (unsigned i = 0; i < 3; ++i)
    {
        wassert(actual(batch[i]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[i]->destination) == "testds");
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
        std::shared_ptr<Metadata> md(f.td.mds[i].clone());
        wassert(actual(ds->acquire(*md)) == metadata::Inbound::Result::ERROR);
        core::Time time;
        std::string content;
        md->get_last_note().get(time, content);
        wassert(actual(content).contains("is older than archive age"));
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
        std::shared_ptr<Metadata> md(f.td.mds[i].clone());
        wassert(actual(*ds).import(*md));
        core::Time time;
        std::string content;
        md->get_last_note().get(time, content);
        wassert(actual(content).contains("is older than delete age"));
    }

    metadata::Collection mdc(*f.config().create_reader(), Matcher());
    wassert(actual(mdc.size()) == 0u);
});

this->add_method("second_resolution", [](Fixture& f) {
    std::shared_ptr<Metadata> md(f.td.mds[1].clone());
    md->test_set(types::Reftime::createPosition(Time(2007, 7, 7, 0, 0, 0)));

    // Import a first metadata to create a segment to repack
    {
        auto writer = f.config().create_writer();
        wassert(actual(*writer).import(*md));
    }

    md->test_set(types::Reftime::createPosition(Time(2007, 7, 7, 0, 0, 1)));
    {
        auto writer = f.config().create_writer();
        wassert(actual(*writer).import(*md));
    }

    wassert(f.ensure_localds_clean(1, 2));
});

auto test_same_segment_fail = [](Fixture& f, unsigned fail_idx, dataset::ReplaceStrategy strategy) {
    sys::rmtree_ifexists("testds");
    Metadata md;
    fill(md);
    auto format = format_from_string(f.cfg->value("format"));

    // Make a batch that ends up all in the same segment
    metadata::Collection mds;
    for (unsigned idx = 0; idx < 3; ++idx)
    {
        md.test_set(types::Reftime::createPosition(Time(2018, 1, 1, idx, 0, 0)));
        if (idx == fail_idx)
            md.set_source_inline(format, metadata::DataManager::get().to_unreadable_data(1));
        else
            md.set_source_inline(format, metadata::DataManager::get().to_data(format, std::vector<uint8_t>{(unsigned char)idx}));
        mds.push_back(md);
    }

    auto ds = f.config().create_writer();
    metadata::InboundBatch batch = mds.make_batch();
    wassert(ds->acquire_batch(batch, strategy));
    wassert(actual(batch[0]->result) == metadata::Inbound::Result::ERROR);
    wassert(actual(batch[1]->result) == metadata::Inbound::Result::ERROR);
    wassert(actual(batch[2]->result) == metadata::Inbound::Result::ERROR);

    auto state = f.scan_state();
    wassert(actual(state.size()) == 1u);
    wassert(actual(state.get("testds:2018/01-01." + format_name(format)).state) == segment::SEGMENT_DELETED);
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
    auto format = format_from_string(f.cfg->value("format"));

    // Make a batch that ends up all in the same segment
    metadata::Collection mds;
    for (unsigned idx = 0; idx < 3; ++idx)
    {
        md.test_set(types::Reftime::createPosition(Time(2018, idx + 1, 1, 0, 0, 0)));
        if (idx == fail_idx)
            md.set_source_inline(format, metadata::DataManager::get().to_unreadable_data(1));
        else
            md.set_source_inline(format, metadata::DataManager::get().to_data(format, std::vector<uint8_t>{(unsigned char)idx}));
        mds.push_back(md);
    }

    auto ds = f.config().create_writer();
    metadata::InboundBatch batch = mds.make_batch();
    wassert(ds->acquire_batch(batch, strategy));
    wassert(actual(batch[0]->result) == (fail_idx == 0 ? metadata::Inbound::Result::ERROR : metadata::Inbound::Result::OK));
    wassert(actual(batch[1]->result) == (fail_idx == 1 ? metadata::Inbound::Result::ERROR : metadata::Inbound::Result::OK));
    wassert(actual(batch[2]->result) == (fail_idx == 2 ? metadata::Inbound::Result::ERROR : metadata::Inbound::Result::OK));
    ds->flush();

    auto state = f.scan_state();
    wassert(actual(state.size()) == 3u);
    wassert(actual(state.get("testds:2018/01-01." + format_name(format)).state) == (fail_idx == 0 ? segment::SEGMENT_DELETED : segment::SEGMENT_OK));
    wassert(actual(state.get("testds:2018/02-01." + format_name(format)).state) == (fail_idx == 1 ? segment::SEGMENT_DELETED : segment::SEGMENT_OK));
    wassert(actual(state.get("testds:2018/03-01." + format_name(format)).state) == (fail_idx == 2 ? segment::SEGMENT_DELETED : segment::SEGMENT_OK));

    metadata::Collection res = f.query(query::Data());
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

this->add_method("test_acquire", [](Fixture& f) noexcept {
    // TODO: add tests for test_acquire
});

this->add_method("import_eatmydata", [](Fixture& f) {
    f.cfg->set("eatmydata", "yes");

    {
        auto ds = f.config().create_writer();
        for (auto& md: f.td.mds)
            wassert(actual(*ds).import(*md));

        metadata::InboundBatch batch = f.td.mds.make_batch();
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
