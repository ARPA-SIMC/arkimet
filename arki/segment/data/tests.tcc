#ifndef ARKI_SEGMENT_TESTS_TCC
#define ARKI_SEGMENT_TESTS_TCC

#include "arki/segment/data/tests.h"
#include "arki/core/file.h"
#include "arki/stream.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/types/source/blob.h"
#include "arki/metadata/data.h"
#include <cstring>

namespace arki {
namespace tests {

template<class Data, class FixtureData>
void SegmentFixture<Data, FixtureData>::test_setup()
{
    using namespace arki::utils;
    sys::rmtree_ifexists("testseg");
    std::filesystem::create_directory("testseg");
    seg_mds = td.mds.clone();
    segment = std::make_shared<Segment>(td.format, std::filesystem::current_path(), "testseg/test." + format_name(td.format));
}

template<class Data, class FixtureData>
std::shared_ptr<segment::data::Checker> SegmentFixture<Data, FixtureData>::create()
{
    return create(seg_mds);
}

template<class Data, class FixtureData>
std::shared_ptr<segment::data::Checker> SegmentFixture<Data, FixtureData>::create(metadata::Collection mds)
{
    return Data::create(*segment, mds, repack_config);
}

template<class Data, class FixtureData>
std::shared_ptr<segment::data::Checker> SegmentFixture<Data, FixtureData>::create(const segment::data::RepackConfig& cfg)
{
    return Data::create(*segment, seg_mds, cfg);
}

template<class Data, class FixtureData>
void SegmentTests<Data, FixtureData>::register_tests()
{
using namespace arki::utils;

this->add_method("create", [](Fixture& f) {
    wassert_true(Data::can_store(f.td.format));
    std::shared_ptr<segment::data::Checker> checker = f.create();
    wassert_true(checker->exists_on_disk());
});

this->add_method("scan", [](Fixture& f) {
    auto checker = f.create();
    auto reader = checker->data().reader(std::make_shared<arki::core::lock::Null>());
    if (strcmp(reader->data().type(), "tar") == 0)
        throw TestSkipped("scanning .tar segments is not yet supported");
    metadata::Collection mds;
    reader->scan(mds.inserter_func());
    wassert(actual(mds.size()) == f.seg_mds.size());
    wassert(actual(mds[0]).is_similar(f.seg_mds[0]));
    wassert(actual(mds[1]).is_similar(f.seg_mds[1]));
    wassert(actual(mds[2]).is_similar(f.seg_mds[2]));
    wassert(actual(mds.size()) == 3u);
});

this->add_method("read", [](Fixture& f) {
    wassert_true(Data::can_store(f.td.format));
    auto checker = f.create();
    auto reader = checker->data().reader(std::make_shared<arki::core::lock::Null>());
    size_t pad_size = f.td.format == DataFormat::VM2 ? 1 : 0;
    for (auto& md: f.seg_mds)
    {
        std::vector<uint8_t> buf = wcallchecked(reader->read(md->sourceBlob()));
        wassert(actual(buf.size()) == md->sourceBlob().size);

        {
            auto stream = StreamOutput::create(std::make_shared<sys::File>("stream.out", O_WRONLY | O_CREAT | O_TRUNC));
            wassert(actual(reader->stream(md->sourceBlob(), *stream)) == stream::SendResult());
            size_t size = sys::size("stream.out");
            wassert(actual(size) == md->sourceBlob().size + pad_size);
        }

        std::string str0(buf.begin(), buf.end());
        if (f.td.format == DataFormat::VM2) str0 += "\n";
        std::string str1 = sys::read_file("stream.out");
        wassert(actual(str1) == str0);
        //wassert_true(std::equal(buf.begin(), buf.end(), buf1.begin(), buf1.end()));
    }
});

this->add_method("repack", [](Fixture& f) {
    auto checker = f.create();
    auto reader = checker->data().reader(std::make_shared<core::lock::Null>());
    for (auto& md: f.seg_mds)
        md->sourceBlob().lock(reader);
    auto p = wcallchecked(checker->repack(f.segment->root, f.seg_mds));
    wassert(p.commit());
    auto rep = [](const std::string& msg) noexcept {
        // fprintf(stderr, "POST REPACK %s\n", msg.c_str());
    };
    wassert(actual(checker->check(rep, f.seg_mds)) == segment::SEGMENT_OK);
});

this->add_method("check", [](Fixture& f) {
    auto checker = f.create();

    segment::State state;

    auto rep = [](const std::string& msg) noexcept {
        // fprintf(stderr, "CHECK %s\n", msg.c_str());
    };

    // A simple segment freshly imported is ok
    wassert(actual(checker->check(rep, f.seg_mds)) == segment::SEGMENT_OK);
    wassert(actual(checker->check(rep, f.seg_mds, true)) == segment::SEGMENT_OK);

    // Simulate one element being deleted
    {
        metadata::Collection mdc1;
        mdc1.push_back(f.seg_mds[1]);
        mdc1.push_back(f.seg_mds[2]);
        wassert(actual(checker->check(rep, mdc1)) == segment::SEGMENT_DIRTY);
    }
    {
        metadata::Collection mdc1;
        mdc1.push_back(f.seg_mds[0]);
        mdc1.push_back(f.seg_mds[2]);
        wassert(actual(checker->check(rep, mdc1)) == segment::SEGMENT_DIRTY);
    }

    {
        metadata::Collection mdc1;
        mdc1.push_back(f.seg_mds[0]);
        mdc1.push_back(f.seg_mds[1]);
        wassert(actual(checker->check(rep, mdc1)) == segment::SEGMENT_DIRTY);
    }

    // Simulate elements out of order
    {
        metadata::Collection mdc1;
        mdc1.push_back(f.seg_mds[0]);
        mdc1.push_back(f.seg_mds[2]);
        mdc1.push_back(f.seg_mds[1]);
        wassert(actual(checker->check(rep, mdc1)) == segment::SEGMENT_DIRTY);
    }

    // Simulate all elements deleted
    {
        metadata::Collection mdc1;
        wassert(actual(checker->check(rep, mdc1)) == segment::SEGMENT_DIRTY);
    }

    // Simulate corrupted file
    {
        metadata::Collection mdc1;
        mdc1.push_back(f.seg_mds[0]);
        mdc1.push_back(f.seg_mds[1]);
        mdc1.push_back(f.seg_mds[2]);
        std::unique_ptr<types::source::Blob> src(f.seg_mds[0].sourceBlob().clone());
        src->offset += 1;
        mdc1[0].set_source(std::unique_ptr<types::Source>(src.release()));
        wassert(actual(checker->check(rep, mdc1, true)) == segment::SEGMENT_CORRUPTED);
        wassert(actual(checker->check(rep, mdc1, false)) == segment::SEGMENT_CORRUPTED);
    }

    {
        metadata::Collection mdc1;
        mdc1.push_back(f.seg_mds[0]);
        mdc1.push_back(f.seg_mds[1]);
        mdc1.push_back(f.seg_mds[2]);
        std::unique_ptr<types::source::Blob> src(f.seg_mds[2].sourceBlob().clone());
        src->offset += 1;
        mdc1[2].set_source(std::unique_ptr<types::Source>(src.release()));
        wassert(actual(checker->check(rep, mdc1, true)) == segment::SEGMENT_CORRUPTED);
        wassert(actual(checker->check(rep, mdc1, false)) == segment::SEGMENT_CORRUPTED);
    }
});

this->add_method("remove", [](Fixture& f) {
    auto checker = f.create();
    size_t size = wcallchecked(checker->size());

    wassert(actual(checker->exists_on_disk()).istrue());
    wassert(actual(checker->remove()) == size);
    wassert(actual(checker->exists_on_disk()).isfalse());
});

this->add_method("is_empty", [](Fixture& f) {
    auto checker = f.create();
    wassert(actual(checker->is_empty()).isfalse());
    checker->test_truncate(f.seg_mds[0].sourceBlob().offset);
    wassert(actual(checker->is_empty()).istrue());
});

this->add_method("issue244", [](Fixture& f) {
    auto md = std::make_shared<Metadata>();
    md->test_set("reftime", "2020-12-01 00:00:00");
    // 65536 is bigger than TransferBuffer's size in utils/sys.cc
    std::vector<uint8_t> buf(65536);
    auto data = arki::metadata::DataManager::get().to_data(f.td.format, std::move(buf));

    md->set_source_inline(f.td.format, data);

    metadata::Collection mds;
    mds.push_back(md);
    auto checker = f.create(mds);
    auto reader = checker->data().reader(std::make_shared<arki::core::lock::Null>());

    // Writing normally uses sendfile
    {
        auto stream = StreamOutput::create(std::make_shared<sys::File>("stream.out", O_WRONLY | O_CREAT | O_TRUNC));
        wassert(actual(reader->stream(md->sourceBlob(), *stream)) == stream::SendResult());
        size_t pad_size = f.td.format == DataFormat::VM2 ? 1 : 0;
        size_t size = sys::size("stream.out");
        wassert(actual(size) == md->sourceBlob().size + pad_size);
    }

    // Opening for append makes sendfile fail and falls back on normal
    // read/write
    {
        auto stream = StreamOutput::create(std::make_shared<sys::File>("stream.out", O_WRONLY | O_TRUNC | O_APPEND));
        wassert(actual(reader->stream(md->sourceBlob(), *stream)) == stream::SendResult());
        size_t pad_size = f.td.format == DataFormat::VM2 ? 1 : 0;
        size_t size = sys::size("stream.out");
        wassert(actual(size) == md->sourceBlob().size + pad_size);
    }
});

}

}
}
#endif
