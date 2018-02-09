#include "arki/dataset/tests.h"
#include "arki/runtime/tests.h"
#include "arki/exceptions.h"
#include "arki/types/source/blob.h"
#include "arki-check.h"

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
            step=daily
        )");
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase<Fixture>::FixtureTestCase;

    void register_tests() override;
};

Tests test1("arki_runtime_arkicheck_ondisk2", "type=ondisk2");
Tests test2("arki_runtime_arkicheck_simple_plain", "type=simple\nindex_type=plain");
Tests test3("arki_runtime_arkicheck_simple_sqlite", "type=simple\nindex_type=sqlite");
Tests test4("arki_runtime_arkicheck_iseg", "type=iseg\nformat=grib");

void Tests::register_tests() {

add_method("clean", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    f.clean_and_import("inbound/fixture.grib1");

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
        wassert(actual(sys::read_file(co.file_stderr.name())) == "testds: check 3 files ok\n");
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--fix" });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
        wassert(actual(sys::read_file(co.file_stderr.name())) == "testds: check 3 files ok\n");
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--repack" });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
        wassert(actual(sys::read_file(co.file_stderr.name())) == "testds: repack 3 files ok\n");
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--repack", "--fix" });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
        wassert(actual(sys::read_file(co.file_stderr.name())).matches(
                "(testds: repack: running VACUUM ANALYZE on the dataset index(, if applicable)?\n)?"
                "(testds: repack: rebuilding the summary cache\n)?"
                "testds: repack 3 files ok\n"));
    }
});

add_method("clean_filtered", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    f.cfg.setValue("type", "iseg");
    f.cfg.setValue("format", "grib");
    f.clean_and_import("inbound/fixture.grib1");

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--filter=reftime:>=2007-07-08" });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
        wassert(actual(sys::read_file(co.file_stderr.name())) == "testds: check 2 files ok\n");
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--fix", "--filter=reftime:>=2007-07-08" });
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
        wassert(actual(sys::read_file(co.file_stderr.name())) == "testds: check 2 files ok\n");
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--repack", "--filter=reftime:>=2007-07-08" });
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
        wassert(actual(sys::read_file(co.file_stderr.name())) == "testds: repack 2 files ok\n");
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--repack", "--fix", "--filter=reftime:>=2007-07-08" });
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
        wassert(actual(sys::read_file(co.file_stderr.name())) == 
                "testds: repack 2 files ok\n");
        wassert(actual(res) == 0);
    }
});

add_method("remove_all", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    f.clean_and_import("inbound/fixture.grib1");

    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/2007/07-07.grib").exists());
    wassert(actual_file("testds/2007/10-09.grib").exists());

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--remove-all" });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds:2007/07-07.grib: should be deleted\n"
            "testds:2007/07-08.grib: should be deleted\n"
            "testds:2007/10-09.grib: should be deleted\n");
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
    }

    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/2007/07-07.grib").exists());
    wassert(actual_file("testds/2007/10-09.grib").exists());

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--remove-all", "-f" });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stdout.name())).matches(
            "testds:2007/07-07.grib: deleted \\([0-9]+ freed\\)\n"
            "testds:2007/07-08.grib: deleted \\([0-9]+ freed\\)\n"
            "testds:2007/10-09.grib: deleted \\([0-9]+ freed\\)\n"));
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
    }

    wassert(actual_file("testds/2007/07-08.grib").not_exists());
    wassert(actual_file("testds/2007/07-07.grib").not_exists());
    wassert(actual_file("testds/2007/10-09.grib").not_exists());
});

add_method("remove_all_filtered", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    f.clean_and_import("inbound/fixture.grib1");

    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/2007/07-07.grib").exists());
    wassert(actual_file("testds/2007/10-09.grib").exists());

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--remove-all", "--filter=reftime:=2007-07-08" });
        wassert(actual(sys::read_file(co.file_stdout.name())) == "testds:2007/07-08.grib: should be deleted\n");
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(res) == 0);
    }

    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/2007/07-07.grib").exists());
    wassert(actual_file("testds/2007/10-09.grib").exists());

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--remove-all", "--filter=reftime:=2007-07-08", "-f" });
        wassert(actual(sys::read_file(co.file_stdout.name())).matches("testds:2007/07-08.grib: deleted \\([0-9]+ freed\\)\n"));
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(res) == 0);
    }

    wassert(actual_file("testds/2007/07-08.grib").not_exists());
    wassert(actual_file("testds/2007/07-07.grib").exists());
    wassert(actual_file("testds/2007/10-09.grib").exists());
});

add_method("archive", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    f.clean_and_import("inbound/fixture.grib1");

    f.cfg.setValue("archive age", "1");
    f.test_reread_config();

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
        wassert(actual(sys::read_file(co.file_stderr.name())) == 
            "testds:2007/07-07.grib: segment old enough to be archived\n"
            "testds:2007/07-08.grib: segment old enough to be archived\n"
            "testds:2007/10-09.grib: segment old enough to be archived\n"
            "testds: check 0 files ok\n"
        );
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--fix" });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
        wassert(actual(sys::read_file(co.file_stderr.name())) == 
            "testds:2007/07-07.grib: segment old enough to be archived\n"
            "testds:2007/07-08.grib: segment old enough to be archived\n"
            "testds:2007/10-09.grib: segment old enough to be archived\n"
            "testds: check 0 files ok\n"
        );
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--repack" });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
        wassert(actual(sys::read_file(co.file_stderr.name())) == 
            "testds:2007/07-07.grib: segment old enough to be archived\n"
            "testds:2007/07-07.grib: should be archived\n"
            "testds:2007/07-08.grib: segment old enough to be archived\n"
            "testds:2007/07-08.grib: should be archived\n"
            "testds:2007/10-09.grib: segment old enough to be archived\n"
            "testds:2007/10-09.grib: should be archived\n"
            "testds: repack 0 files ok, 3 files should be archived\n"
        );
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--repack", "--fix" });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
        wassert(actual(sys::read_file(co.file_stderr.name())).matches(
            "testds:2007/07-07.grib: segment old enough to be archived\n"
            "testds:2007/07-07.grib: archived\n"
            "testds:2007/07-08.grib: segment old enough to be archived\n"
            "testds:2007/07-08.grib: archived\n"
            "testds:2007/10-09.grib: segment old enough to be archived\n"
            "testds:2007/10-09.grib: archived\n"
            "(testds: repack: running VACUUM ANALYZE on the dataset index(, if applicable)?\n)?"
            "(testds: repack: rebuilding the summary cache\n)?"
            "testds: repack 0 files ok, 3 files archived\n"
            "testds.archives.last: repack: running VACUUM ANALYZE on the dataset index, if applicable\n"
            "testds.archives.last: repack 3 files ok\n")
        );
    }

    wassert(actual_file("testds/.archive/last/2007/07-08.grib").exists());
    wassert(actual_file("testds/2007/07-08.grib").not_exists());
    metadata::Collection mdc1(*f.config().create_reader(), "reftime:=2007-07-08");
    wassert(actual(mdc1.size()) == 1u);
    wassert(actual(mdc1[0].sourceBlob().basedir).endswith("/.archive/last"));
    wassert(actual(mdc1[0].sourceBlob().filename) == "2007/07-08.grib");

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--unarchive=2007/07-08.grib" });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
    }

    wassert(actual_file("testds/.archive/last/2007/07-08.grib").not_exists());
    wassert(actual_file("testds/2007/07-08.grib").exists());
    metadata::Collection mdc2(*f.config().create_reader(), "reftime:=2007-07-08");
    wassert(actual(mdc2.size()) == 1u);
    wassert(actual(mdc2[0].sourceBlob().basedir).endswith("/testds"));
    wassert(actual(mdc2[0].sourceBlob().filename) == "2007/07-08.grib");
});

add_method("issue57", [](Fixture& f) {
#ifndef HAVE_VM2
    throw TestSkipped();
#endif
    using runtime::tests::run_cmdline;

    if (f.cfg.value("type") == "simple")
        throw TestSkipped();

    f.cfg.setValue("format", "vm2");
    f.cfg.setValue("unique", "reftime, area, product");
    f.test_reread_config();

    sys::mkdir_ifmissing("testds/2016");
    sys::write_file("testds/2016/10-05.vm2", "201610050000,12626,139,70,,,000000000");

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "-f", "testds" });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
        wassert(actual(sys::read_file(co.file_stderr.name())) ==
            "testds:2016/10-05.vm2: segment found on disk with no associated index data\n"
            "testds:2016/10-05.vm2: rescanned\n"
            "testds: check 0 files ok, 1 file rescanned\n");
    }

    //arki-query '' issue57 > issue57/todelete.md
    metadata::Collection to_delete(*f.config().create_reader(), "");
    //runtest "test -s issue57/todelete.md"
    wassert(actual(to_delete.size()) == 1u);
    to_delete.writeAtomically("testds/todelete.md");

    //runtest "arki-check --remove=issue57/todelete.md issue57"
    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--remove=testds/todelete.md" });
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(res) == 0);
    }

    metadata::Collection post_delete(*f.config().create_reader(), "");
    wassert(actual(post_delete.size()) == 0u);
});

}

}
