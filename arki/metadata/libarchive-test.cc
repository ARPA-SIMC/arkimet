#include "arki/tests/tests.h"
#include "libarchive.h"
#include "arki/utils/sys.h"
#if 0
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source.h"
#include "arki/types/origin.h"
#include "arki/types/product.h"
#include "arki/types/level.h"
#include "arki/types/timerange.h"
#include "arki/types/reftime.h"
#include "arki/types/area.h"
#include "arki/types/proddef.h"
#include "arki/types/assigneddataset.h"
#include <iostream>
#endif

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

#if 0
/**
 * Creates a tempfile, runs body and returns the contents of the temp file after that.
 *
 * The temp file is created in the current directory with a fixed name; this is
 * ok for tests that run on a temp dir, and is not to be used outside tests.
 */
std::string tempfile_to_string(std::function<void(arki::utils::sys::NamedFileDescriptor& out)> body)
{
    sys::File wr("tempfile", O_WRONLY | O_CREAT | O_TRUNC, 0666);
    body(wr);
    wr.close();
    string res = sys::read_file("tempfile");
    sys::unlink("tempfile");
    return res;
}

void fill(Metadata& md)
{
    using namespace arki::types;

    ValueBag testValues;
    testValues.set("foo", Value::createInteger(5));
    testValues.set("bar", Value::createInteger(5000));
    testValues.set("baz", Value::createInteger(-200));
    testValues.set("moo", Value::createInteger(0x5ffffff));
    testValues.set("antani", Value::createInteger(-1));
    testValues.set("blinda", Value::createInteger(0));
    testValues.set("supercazzola", Value::createInteger(-1234567));
    testValues.set("pippo", Value::createString("pippo"));
    testValues.set("pluto", Value::createString("12"));
    testValues.set("cippo", Value::createString(""));

    md.set(Reftime::createPosition(core::Time(2006, 5, 4, 3, 2, 1)));
    md.set(origin::GRIB1::create(1, 2, 3));
    md.set(product::GRIB1::create(1, 2, 3));
    md.set(level::GRIB1::create(114, 12, 34));
    md.set(timerange::GRIB1::create(1, 1, 2, 3));
    md.set(area::GRIB::create(testValues));
    md.set(proddef::GRIB::create(testValues));
    md.add_note("test note");
    md.set(AssignedDataset::create("dsname", "dsid"));
}

inline bool cmpmd(Metadata& md1, Metadata& md2)
{
    if (md1 != md2)
    {
        cerr << "----- The two metadata differ.  First one:" << endl;
        md1.write_yaml(cerr);
        if (md1.source().style() == types::Source::INLINE)
        {
            const auto& buf = md1.getData();
            cerr << "-- Inline data:" << string((const char*)buf.data(), buf.size()) << endl;
        }
        cerr << "----- Second one:" << endl;
        md2.write_yaml(cerr);
        if (md2.source().style() == types::Source::INLINE)
        {
            const auto& buf = md2.getData();
            cerr << "-- Inline data:" << string((const char*)buf.data(), buf.size()) << endl;
        }
        return false;
    }
    return true;
}
#endif

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_metadata_libarchive");

void Tests::register_tests() {

// Test compression
add_method("targz", [] {
#ifndef HAVE_LIBARCHIVE
    throw TestSkipped();
#endif
    metadata::TestCollection mds("inbound/test.grib1");
    sys::File out("test.tar.gz", O_WRONLY | O_CREAT | O_TRUNC);
    metadata::LibarchiveOutput arc_out("tar.gz", out);
    wassert(arc_out.append(mds[0]));
    wassert(arc_out.append(mds[1]));
    wassert(arc_out.append(mds[2]));
    wassert(arc_out.flush());
    out.close();

    wassert(actual(system("tar ztf test.tar.gz > test.out")) == 0);
    wassert(actual(sys::read_file("test.out")) == 
        "000001.grib\n"
        "000002.grib\n"
        "000003.grib\n"
        "metadata.md\n"
    );
});

add_method("tarxz", [] {
#ifndef HAVE_LIBARCHIVE
    throw TestSkipped();
#endif
    metadata::TestCollection mds("inbound/test.grib1");
    sys::File out("test.tar.xz", O_WRONLY | O_CREAT | O_TRUNC);
    metadata::LibarchiveOutput arc_out("tar.xz", out);
    wassert(arc_out.append(mds[0]));
    wassert(arc_out.append(mds[1]));
    wassert(arc_out.append(mds[2]));
    wassert(arc_out.flush());
    out.close();

    wassert(actual(system("tar Jtf test.tar.xz > test.out")) == 0);
    wassert(actual(sys::read_file("test.out")) == 
        "000001.grib\n"
        "000002.grib\n"
        "000003.grib\n"
        "metadata.md\n"
    );
});

add_method("zip", [] {
#ifndef HAVE_LIBARCHIVE
    throw TestSkipped();
#endif
    metadata::TestCollection mds("inbound/test.grib1");
    sys::File out("test.zip", O_WRONLY | O_CREAT | O_TRUNC);
    metadata::LibarchiveOutput arc_out("zip", out);
    wassert(arc_out.append(mds[0]));
    wassert(arc_out.append(mds[1]));
    wassert(arc_out.append(mds[2]));
    wassert(arc_out.flush());
    out.close();

    wassert(actual(system("unzip -l test.zip > test.out")) == 0);
    wassert(actual(sys::read_file("test.out")) == 
R"(Archive:  test.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
     7218  1980-01-01 00:00   000001.grib
    34960  1980-01-01 00:00   000002.grib
     2234  1980-01-01 00:00   000003.grib
      612  1980-01-01 00:00   metadata.md
---------                     -------
    45024                     4 files
)"
    );
});

#if 0
// Send data split in less chunks than we have metadata
add_method("split", [] {
    // Create test metadata
    Metadata md;
    md.set_source(types::Source::createURL("grib", "http://www.example.org"));
    fill(md);

    // Encode it in a buffer 3 times
    std::string str = tempfile_to_string([&](sys::NamedFileDescriptor& out) {
        md.write(out);
        md.write(out);
        md.write(out);
    });

    // Where we collect the decoded metadata
    metadata::Collection results;

    // Stream for the decoding
    metadata::Stream mdstream(results.inserter_func(), "test stream");

    // Send the data in two halves
    mdstream.readData(str.data(), str.size() / 2);
    ensure_equals(results.size(), 1u);
    mdstream.readData(str.data() + str.size() / 2, str.size() - (str.size() / 2));

    // No bytes must be left to decode
    ensure_equals(mdstream.countBytesUnprocessed(), 0u);

    // See that we've got what we expect
    ensure_equals(results.size(), 3u);
    ensure(cmpmd(md, results[0]));
    ensure(cmpmd(md, results[1]));
    ensure(cmpmd(md, results[2]));
});
#endif

}
}
