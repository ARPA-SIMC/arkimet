#include "arki/dataset/tests.h"
#include "arki/runtime/tests.h"
#include "arki/types/source/blob.h"
#include "arki/utils/accounting.h"
#include "arki-scan.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

struct Fixture : public DatasetTest
{
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type=ondisk2
            step=daily
            postprocess=cat,echo,say,checkfiles,error,outthenerr
            filter=origin:GRIB1
        )");

        write_dispatch_config();
    }

    void write_dispatch_config()
    {
        core::cfg::Sections dispatch_cfg;
        dispatch_cfg.emplace("testds", cfg);

        auto& section = dispatch_cfg.obtain("error");
        section.set("type", "error");
        section.set("step", "daily");
        section.set("path", "error");

        sys::File out("test-dispatch", O_WRONLY | O_CREAT | O_TRUNC);
        stringstream ss;
        dispatch_cfg.write(ss, "memory");
        out.write_all_or_throw(ss.str());
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase<Fixture>::FixtureTestCase;

    void register_tests() override;
} test("arki_runtime_arkiscan");

void Tests::register_tests() {

add_method("dispatch_plain", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    acct::acquire_single_count.reset();
    acct::acquire_batch_count.reset();

    metadata::Collection mds;
    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiScan>({
            "arki-scan",
            "--dispatch=test-dispatch",
            "inbound/test.grib1",
        });
        wassert(co.check_success(res));
        mds.read_from_file(co.file_stdout.name());
    }

    wassert(actual(mds.size()) == 3u);
    wassert(actual(mds[0].sourceBlob().filename) == sys::abspath("testds/2007/07-08.grib"));
    wassert(actual(mds[1].sourceBlob().filename) == sys::abspath("testds/2007/07-07.grib"));
    wassert(actual(mds[2].sourceBlob().filename) == sys::abspath("testds/2007/10-09.grib"));
    wassert(actual(acct::acquire_single_count.val()) == 0u);
    wassert(actual(acct::acquire_batch_count.val()) == 1u);
});

add_method("dispatch_flush_threshold", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    acct::acquire_single_count.reset();
    acct::acquire_batch_count.reset();

    metadata::Collection mds;
    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiScan>({
            "arki-scan",
            "--dispatch=test-dispatch",
            "--flush-threshold=8k",
            "inbound/test.grib1",
        });
        wassert(co.check_success(res));
        mds.read_from_file(co.file_stdout.name());
    }

    wassert(actual(mds.size()) == 3u);
    wassert(actual(mds[0].sourceBlob().filename) == sys::abspath("testds/2007/07-08.grib"));
    wassert(actual(mds[1].sourceBlob().filename) == sys::abspath("testds/2007/07-07.grib"));
    wassert(actual(mds[2].sourceBlob().filename) == sys::abspath("testds/2007/10-09.grib"));
    wassert(actual(acct::acquire_single_count.val()) == 0u);
    wassert(actual(acct::acquire_batch_count.val()) == 2u);
});

add_method("dispatch_copyok_copyko", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    f.cfg.set("filter", "origin:GRIB1,200 or GRIB1,80");
    f.test_reread_config();
    f.write_dispatch_config();

    sys::rmtree_ifexists("copyok");
    sys::makedirs("copyok/copyok");
    sys::makedirs("copyok/copyko");

    metadata::Collection mds;
    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiScan>({
            "arki-scan",
            "--copyok=copyok/copyok",
            "--copyko=copyok/copyko",
            "--dispatch=test-dispatch",
            "inbound/test.grib1",
        });
        wassert(actual(res) == 2);
        mds.read_from_file(co.file_stdout.name());
    }

    wassert(actual(mds.size()) == 3u);
    wassert(actual(mds[0].sourceBlob().filename) == sys::abspath("testds/2007/07-08.grib"));
    wassert(actual(mds[1].sourceBlob().filename) == sys::abspath("testds/2007/07-07.grib"));
    wassert(actual(mds[2].sourceBlob().filename) == sys::abspath("error/2007/10-09.grib"));

    wassert(actual_file("copyok/copyok/test.grib1").exists());
    wassert(actual(sys::size("copyok/copyok/test.grib1")) == 42178u);
    wassert(actual_file("copyok/copyko/test.grib1").exists());
    wassert(actual(sys::size("copyok/copyko/test.grib1")) == 2234u);
});

add_method("dispatch_copyok", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    f.cfg.set("filter", "origin:GRIB1");
    f.test_reread_config();
    f.write_dispatch_config();

    sys::rmtree_ifexists("copyok");
    sys::makedirs("copyok/copyok");
    sys::makedirs("copyok/copyko");

    metadata::Collection mds;
    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiScan>({
            "arki-scan",
            "--copyok=copyok/copyok",
            "--copyko=copyok/copyko",
            "--dispatch=test-dispatch",
            "inbound/test.grib1",
        });
        wassert(actual(res) == 0);
        mds.read_from_file(co.file_stdout.name());
    }

    wassert(actual(mds.size()) == 3u);
    wassert(actual(mds[0].sourceBlob().filename) == sys::abspath("testds/2007/07-08.grib"));
    wassert(actual(mds[1].sourceBlob().filename) == sys::abspath("testds/2007/07-07.grib"));
    wassert(actual(mds[2].sourceBlob().filename) == sys::abspath("testds/2007/10-09.grib"));

    wassert(actual_file("copyok/copyok/test.grib1").exists());
    wassert(actual(sys::size("copyok/copyok/test.grib1")) == 44412u);
    wassert(actual_file("copyok/copyko/test.grib1").not_exists());
});

add_method("dispatch_copyko", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    f.cfg.set("filter", "origin:GRIB2");
    f.test_reread_config();
    f.write_dispatch_config();

    sys::rmtree_ifexists("copyok");
    sys::makedirs("copyok/copyok");
    sys::makedirs("copyok/copyko");

    metadata::Collection mds;
    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiScan>({
            "arki-scan",
            "--copyok=copyok/copyok",
            "--copyko=copyok/copyko",
            "--dispatch=test-dispatch",
            "inbound/test.grib1",
        });
        wassert(actual(res) == 2);
        mds.read_from_file(co.file_stdout.name());
    }

    wassert(actual(mds.size()) == 3u);
    wassert(actual(mds[0].sourceBlob().filename) == sys::abspath("error/2007/07-08.grib"));
    wassert(actual(mds[1].sourceBlob().filename) == sys::abspath("error/2007/07-07.grib"));
    wassert(actual(mds[2].sourceBlob().filename) == sys::abspath("error/2007/10-09.grib"));

    wassert(actual_file("copyok/copyok/test.grib1").not_exists());
    wassert(actual_file("copyok/copyko/test.grib1").exists());
    wassert(actual(sys::size("copyok/copyko/test.grib1")) == 44412u);
});

add_method("dispatch_issue68", [](Fixture& f) {
    using runtime::tests::run_cmdline;
    skip_unless_vm2();

    f.cfg.set("filter", "area:VM2,1");
    f.test_reread_config();
    f.write_dispatch_config();

    sys::rmtree_ifexists("copyok");
    sys::makedirs("copyok/copyok");
    sys::makedirs("copyok/copyko");

    metadata::Collection mds;
    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiScan>({
            "arki-scan",
            "--copyok=copyok/copyok",
            "--copyko=copyok/copyko",
            "--dispatch=test-dispatch",
            "inbound/issue68.vm2",
        });
        wassert(actual(res) == 2);
        mds.read_from_file(co.file_stdout.name());
    }

    wassert(actual_file("copyok/copyok/issue68.vm2").exists());
    wassert(actual_file("copyok/copyko/issue68.vm2").exists());

    wassert(actual_file("copyok/copyok/issue68.vm2").contents_equal({
        "198710310000,1,227,1.2,,,000000000",
        "19871031000030,1,228,.5,,,000000000",
    }));
    wassert(actual_file("copyok/copyko/issue68.vm2").contents_equal({
        "201101010000,12,1,800,,,000000000",
        "201101010100,12,2,50,,,000000000",
        "201101010300,12,2,50,,,000000000",
    }));
});

add_method("dispatch_issue154", [](Fixture& f) {
    using runtime::tests::run_cmdline;
    skip_unless_vm2();

    f.cfg.set("filter", "area:VM2,42");
    f.test_reread_config();
    f.write_dispatch_config();

    sys::rmtree_ifexists("copyok");
    sys::makedirs("copyok/copyok");
    sys::makedirs("copyok/copyko");

    metadata::Collection mds;
    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiScan>({
            "arki-scan",
            "--copyok=copyok/copyok",
            "--copyko=copyok/copyko",
            "--dispatch=test-dispatch",
            "inbound/issue68.vm2",
        });
        wassert(actual(res) == 2);
        wassert(actual_file(co.file_stderr.name()).contents_equal({
        }));
        mds.read_from_file(co.file_stdout.name());
    }

    wassert(actual_file("copyok/copyok/issue68.vm2").not_exists());
    wassert(actual_file("copyok/copyko/issue68.vm2").exists());
    wassert(actual_file("copyok/copyko/issue68.vm2").contents_equal({
        "198710310000,1,227,1.2,,,000000000",
        "19871031000030,1,228,.5,,,000000000",
        "201101010000,12,1,800,,,000000000",
        "201101010100,12,2,50,,,000000000",
        "201101010300,12,2,50,,,000000000",
    }));
});




}

}
