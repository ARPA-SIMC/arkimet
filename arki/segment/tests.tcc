#ifndef ARKI_SEGMENT_TESTS_TCC
#define ARKI_SEGMENT_TESTS_TCC

#include "arki/segment/tests.h"
#include "arki/core/file.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/types/source/blob.h"
#include <cstring>

namespace arki {
namespace tests {

template<class Segment, class Data>
void SegmentFixture<Segment, Data>::test_setup()
{
    using namespace arki::utils;
    seg_mds = td.mds;
    root = sys::getcwd();
    sys::rmtree_ifexists("testseg");
    sys::mkdir_ifmissing("testseg");
    relpath = "testseg/test." + td.format;
    abspath = sys::abspath(relpath);
}

template<class Segment, class Data>
std::shared_ptr<segment::Checker> SegmentFixture<Segment, Data>::create()
{
    return Segment::create(td.format, root, relpath, abspath, seg_mds, repack_config);
}

template<class Segment, class Data>
std::shared_ptr<segment::Checker> SegmentFixture<Segment, Data>::create(const segment::RepackConfig& cfg)
{
    return Segment::create(td.format, root, relpath, abspath, seg_mds, cfg);
}

template<class Segment, class Data>
void SegmentTests<Segment, Data>::register_tests()
{
using namespace arki::utils;

this->add_method("create", [](Fixture& f) {
    wassert_true(Segment::can_store(f.td.format));
    std::shared_ptr<segment::Checker> checker = f.create();
    wassert_true(checker->exists_on_disk());
});

this->add_method("scan", [](Fixture& f) {
    auto checker = f.create();
    auto reader = checker->segment().reader(std::make_shared<arki::core::lock::Null>());
    if (strcmp(reader->segment().type(), "tar") == 0)
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
    wassert_true(Segment::can_store(f.td.format));
    auto checker = f.create();
    auto reader = checker->segment().reader(std::make_shared<arki::core::lock::Null>());
    size_t pad_size = f.td.format == "vm2" ? 1 : 0;
    for (auto& md: f.seg_mds)
    {
        std::vector<uint8_t> buf = wcallchecked(reader->read(md->sourceBlob()));
        wassert(actual(buf.size()) == md->sourceBlob().size);

        sys::File out("stream.out", O_WRONLY | O_CREAT | O_TRUNC);
        size_t size = wcallchecked(reader->stream(md->sourceBlob(), out));
        wassert(actual(size) == md->sourceBlob().size + pad_size);
        out.close();

        std::string str0(buf.begin(), buf.end());
        if (f.td.format == "vm2") str0 += "\n";
        std::string str1 = sys::read_file("stream.out");
        wassert(actual(str1) == str0);
        //wassert_true(std::equal(buf.begin(), buf.end(), buf1.begin(), buf1.end()));
    }
});

this->add_method("repack", [](Fixture& f) {
    auto checker = f.create();
    auto reader = checker->segment().reader(std::make_shared<core::lock::Null>());
    for (auto& md: f.seg_mds)
        md->sourceBlob().lock(reader);
    Pending p = wcallchecked(checker->repack(f.root, f.seg_mds));
    wassert(p.commit());
    auto rep = [](const std::string& msg) {
        // fprintf(stderr, "POST REPACK %s\n", msg.c_str());
    };
    wassert(actual(checker->check(rep, f.seg_mds)) == segment::SEGMENT_OK);
});

this->add_method("check", [](Fixture& f) {
    auto checker = f.create();

    segment::State state;

    auto rep = [](const std::string& msg) {
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
    checker->test_truncate(0);
    wassert(actual(checker->is_empty()).istrue());
});

}

}
}
#endif
