#include "arki/dataset/tests.h"
#include "arki/segment/tests.h"
#include "arki/dataset/time.h"
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
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) == "testds: check 3 files ok\n");
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--fix" });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) == "testds: check 3 files ok\n");
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--repack" });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) == "testds: repack 3 files ok\n");
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--repack", "--fix" });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())).matches(
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
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) == "testds: check 2 files ok\n");
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--fix", "--filter=reftime:>=2007-07-08" });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) == "testds: check 2 files ok\n");
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--repack", "--filter=reftime:>=2007-07-08" });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) == "testds: repack 2 files ok\n");
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--repack", "--fix", "--filter=reftime:>=2007-07-08" });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
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

    wassert(f.ensure_localds_clean(0, 0));
    wassert(f.query_results({}));
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

    wassert(f.ensure_localds_clean(2, 2));
    wassert(f.query_results({1, 2}));
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
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
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
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
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
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
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
        int res = run_cmdline(runtime::arki_check, { "arki-check", "testds", "--repack", "--fix", "--online", "--offline" });
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())).matches(
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
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
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
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds:2016/10-05.vm2: segment found on disk with no associated index data\n"
            "testds:2016/10-05.vm2: rescanned\n"
            "testds: check 0 files ok, 1 file rescanned\n");
        wassert(actual(res) == 0);
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
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
        wassert(actual(res) == 0);
    }

    metadata::Collection post_delete(*f.config().create_reader(), "");
    wassert(actual(post_delete.size()) == 0u);
});

add_method("tar", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    f.cfg.setValue("format", "odimh5");
    f.test_reread_config();
    f.clean_and_import("inbound/fixture.h5/00.h5");
    f.import("inbound/fixture.h5/01.h5");
    f.import("inbound/fixture.h5/02.h5");

    auto o = dataset::SessionTime::local_override(1184018400); // date +%s --date="2007-07-10"
    f.cfg.setValue("archive age", "1");
    f.test_reread_config();
    f.repack();

    wassert(actual_file("testds/.archive/last/2007/07-07.odimh5").exists());
    wassert(actual_file("testds/2007/07-08.odimh5").exists());
    wassert(actual_file("testds/2007/10-09.odimh5").exists());

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--tar", "testds", });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds.archives.last:2007/07-07.odimh5: should be tarred\n"
        );
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--tar", "--offline", "testds", });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds.archives.last:2007/07-07.odimh5: should be tarred\n"
        );
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--tar", "--online", "testds", });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds:2007/07-08.odimh5: should be tarred\n"
            "testds:2007/10-09.odimh5: should be tarred\n"
        );
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--tar", "testds", "-f" });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds.archives.last:2007/07-07.odimh5: tarred\n"
        );
        wassert(actual(res) == 0);
    }

    wassert(actual_file("testds/.archive/last/2007/07-07.odimh5").not_exists());
    wassert(actual_file("testds/.archive/last/2007/07-07.odimh5.tar").exists());
    wassert(actual_file("testds/2007/07-08.odimh5").exists());
    wassert(actual_file("testds/2007/10-09.odimh5").exists());

    wassert(f.ensure_localds_clean(3, 3));
    wassert(f.ensure_localds_clean(3, 3, false));
    wassert(f.query_results({1, 0, 2}));
});

add_method("zip", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    f.cfg.setValue("format", "odimh5");
    f.test_reread_config();
    f.clean_and_import("inbound/fixture.h5/00.h5");
    f.import("inbound/fixture.h5/01.h5");
    f.import("inbound/fixture.h5/02.h5");

    auto o = dataset::SessionTime::local_override(1184018400); // date +%s --date="2007-07-10"
    f.cfg.setValue("archive age", "1");
    f.test_reread_config();
    f.repack();

    wassert(actual_file("testds/.archive/last/2007/07-07.odimh5").exists());
    wassert(actual_file("testds/2007/07-08.odimh5").exists());
    wassert(actual_file("testds/2007/10-09.odimh5").exists());

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--zip", "testds", });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds.archives.last:2007/07-07.odimh5: should be zipped\n"
        );
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--zip", "--offline", "testds", });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds.archives.last:2007/07-07.odimh5: should be zipped\n"
        );
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--zip", "--online", "testds", });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds:2007/07-08.odimh5: should be zipped\n"
            "testds:2007/10-09.odimh5: should be zipped\n"
        );
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--zip", "testds", "-f" });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds.archives.last:2007/07-07.odimh5: zipped\n"
        );
        wassert(actual(res) == 0);
    }

    wassert(f.archived_segment_exists("last/2007/07-07.odimh5", {".zip"}));
    wassert(f.online_segment_exists("2007/07-08.odimh5", {""}));
    wassert(f.online_segment_exists("2007/10-09.odimh5", {""}));

    wassert(f.ensure_localds_clean(3, 3));
    wassert(f.ensure_localds_clean(3, 3, false));
    wassert(f.query_results({1, 0, 2}));
});

add_method("compress", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    f.clean_and_import("inbound/fixture.grib1");
    auto o = dataset::SessionTime::local_override(1184018400); // date +%s --date="2007-07-10"
    f.cfg.setValue("archive age", "1");
    f.test_reread_config();
    f.repack();

    wassert(actual_file("testds/.archive/last/2007/07-07.grib").exists());
    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/2007/10-09.grib").exists());

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--compress", "testds", });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds.archives.last:2007/07-07.grib: should be compressed\n"
        );
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--compress", "--offline", "testds", });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds.archives.last:2007/07-07.grib: should be compressed\n"
        );
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--compress", "--online", "testds", });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds:2007/07-08.grib: should be compressed\n"
            "testds:2007/10-09.grib: should be compressed\n"
        );
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--compress", "testds", "-f" });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())).matches(
            "testds.archives.last:2007/07-07.grib: compressed \\([0-9]+ freed\\)\n"
        ));
        wassert(actual(res) == 0);
    }

    wassert(actual_file("testds/.archive/last/2007/07-07.grib").not_exists());
    wassert(actual_file("testds/.archive/last/2007/07-07.grib.gz").exists());
    wassert(actual_file("testds/.archive/last/2007/07-07.grib.gz.idx").exists());
    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/2007/10-09.grib").exists());

    wassert(f.ensure_localds_clean(3, 3));
    wassert(f.ensure_localds_clean(3, 3, false));
    wassert(f.query_results({1, 0, 2}));
});

add_method("scan", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    f.cfg.setValue("format", "odimh5");
    f.test_reread_config();
    f.clean_and_import("inbound/fixture.h5/00.h5");
    f.import("inbound/fixture.h5/01.h5");
    f.import("inbound/fixture.h5/02.h5");

    auto o = dataset::SessionTime::local_override(1184018400); // date +%s --date="2007-07-10"
    f.cfg.setValue("archive age", "1");
    f.test_reread_config();
    f.repack();

    wassert(actual_file("testds/.archive/last/2007/07-07.odimh5").exists());
    wassert(actual_file("testds/2007/07-08.odimh5").exists());
    wassert(actual_file("testds/2007/10-09.odimh5").exists());

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--state", "testds", });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds:2007/07-08.odimh5: OK 2007-07-08 00:00:00Z to 2007-07-08 23:59:59Z\n"
            "testds:2007/10-09.odimh5: OK 2007-10-09 00:00:00Z to 2007-10-09 23:59:59Z\n"
            "testds.archives.last:2007/07-07.odimh5: OK 2007-07-01 00:00:00Z to 2007-07-31 23:59:59Z\n"
        );
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--state", "--offline", "testds", });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds.archives.last:2007/07-07.odimh5: OK 2007-07-01 00:00:00Z to 2007-07-31 23:59:59Z\n"
        );
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--state", "--online", "testds", });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds:2007/07-08.odimh5: OK 2007-07-08 00:00:00Z to 2007-07-08 23:59:59Z\n"
            "testds:2007/10-09.odimh5: OK 2007-10-09 00:00:00Z to 2007-10-09 23:59:59Z\n"
        );
        wassert(actual(res) == 0);
    }
});

add_method("remove_old", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    f.cfg.setValue("format", "odimh5");
    f.test_reread_config();
    f.clean_and_import("inbound/fixture.h5/00.h5");
    f.import("inbound/fixture.h5/01.h5");
    f.import("inbound/fixture.h5/02.h5");

    auto o = dataset::SessionTime::local_override(1184104800); // date +%s --date="2007-07-11"
    f.cfg.setValue("archive age", "2");
    f.test_reread_config();
    f.repack();

    f.cfg.setValue("delete age", "1");
    f.test_reread_config();

    wassert(actual_file("testds/.archive/last/2007/07-07.odimh5").exists());
    wassert(actual_file("testds/2007/07-08.odimh5").exists());
    wassert(actual_file("testds/2007/10-09.odimh5").exists());

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--remove-old", "testds", });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds:2007/07-08.odimh5: segment old enough to be deleted\n"
            "testds:2007/07-08.odimh5: should be deleted\n"
        );
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--remove-old", "--offline", "testds", });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            // "testds.archives.last:2007/07-07.odimh5: segment old enough to be deleted\n"
            "testds.archives.last:2007/07-07.odimh5: should be deleted\n"
        );
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--remove-old", "--online", "testds", });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds:2007/07-08.odimh5: segment old enough to be deleted\n"
            "testds:2007/07-08.odimh5: should be deleted\n"
        );
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--remove-old", "--offline", "--online", "testds" });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())) ==
            "testds:2007/07-08.odimh5: segment old enough to be deleted\n"
            "testds:2007/07-08.odimh5: should be deleted\n"
            "testds.archives.last:2007/07-07.odimh5: should be deleted\n"
        );
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_check, { "arki-check", "--remove-old", "testds", "-f" });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(sys::read_file(co.file_stdout.name())).matches(
            "testds:2007/07-08.odimh5: segment old enough to be deleted\n"
            "testds:2007/07-08.odimh5: deleted \\([0-9]+ freed\\)\n"
        ));
        wassert(actual(res) == 0);
    }

    wassert(f.archived_segment_exists("last/2007/07-07.odimh5", {""}));
    wassert(actual_segment("testds/2007/07-08.odimh5").not_exists());
    wassert(f.online_segment_exists("2007/10-09.odimh5", {""}));

    wassert(f.ensure_localds_clean(2, 2));
    wassert(f.query_results({1, 2}));
});

}

}
