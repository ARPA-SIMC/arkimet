#include "config.h"
#include "arki/dataset/tests.h"
#include "arki/dataset/index/manifest.h"
#include "arki/dataset/maintenance.h"
#include "arki/metadata.h"
#include "arki/metadata/consumer.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/scan/any.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::tests;
using namespace arki::utils;
using namespace arki::dataset;
using namespace arki::dataset::index;

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_dataset_index_manifest");

void clean()
{
    if (sys::exists("testds"))
        sys::rmtree("testds");
    sys::makedirs("testds/.archive/last");
}

std::string idxfname()
{
    return Manifest::get_force_sqlite() ? "index.sqlite" : "MANIFEST";
}

struct IndexingCollector : public MaintenanceCollector
{
	Manifest& m;
	const Summary& s;
	time_t mtime;

	IndexingCollector(Manifest& m, const Summary& s, time_t mtime) : m(m), s(s), mtime(mtime) {}

    virtual void operator()(const std::string& file, dataset::segment::State state)
    {
        MaintenanceCollector::operator()(file, state);
        int n = atoi(file.c_str());

        char fname[32];
        if (n > 10)
        {
            snprintf(fname, 32, "%02d.grib1", n - 10);
            m.acquire(fname, mtime, s);
        }
        if (n < 50)
        {
            snprintf(fname, 32, "%02d.grib1", n + 10);
            m.acquire(fname, mtime, s);
        }
    }
};

void Tests::register_tests() {

add_method("exists", [] {
    clean();

    // Empty dirs do not show up as having a manifest
    ensure(!Manifest::exists("testds/.archive/last"));

    // An empty MANIFEST file counts as an empty manifest
    system("touch testds/.archive/last/MANIFEST");
    ensure(Manifest::exists("testds/.archive/last"));
    sys::unlink_ifexists("testds/.archive/last/MANIFEST");

    // Same if there is a sqlite manifest
    system("touch testds/.archive/last/index.sqlite");
    ensure(Manifest::exists("testds/.archive/last"));
    sys::unlink_ifexists("testds/.archive/last/index.sqlite");
});

// Test accessing empty manifests
add_method("empty", [] {
    clean();

	// Opening a missing manifest read only fails
	{
		ensure(!Manifest::exists("testds/.archive/last/" + idxfname()));
		std::unique_ptr<Manifest> m = Manifest::create("testds/.archive/last");
		try {
			m->openRO();
			ensure(false);
		} catch (std::exception& e) {
			ensure(string(e.what()).find("does not exist") != string::npos);
		}
	}

	// But an empty dataset
	{
		std::unique_ptr<Manifest> m = Manifest::create("testds/.archive/last");
		m->openRW();
	}

	std::unique_ptr<Manifest> m = Manifest::create("testds/.archive/last");
	m->openRO();

    vector<string> files = m->file_list(Matcher());
    ensure(files.empty());
});

// Test creating a new manifest
add_method("create", [] {
    clean();

    // Opening a missing manifest read-write creates a new one
    ensure(!sys::exists("testds/.archive/last/" + idxfname()));
    std::unique_ptr<Manifest> m = Manifest::create("testds/.archive/last");
    m->openRW();
    m->flush();
    ensure(sys::exists("testds/.archive/last/" + idxfname()));

    size_t count = 0;
    m->list_segments([&](const std::string&) { ++count; });
    wassert(actual(count) == 0u);

    m->vacuum();
});

// Test adding and removing files
add_method("add_remove", [] {
    clean();

	system("cp inbound/test.grib1 testds/.archive/last/a.grib1");
	system("mkdir testds/.archive/last/foo");
	system("cp inbound/test.grib1 testds/.archive/last/foo/b.grib1");

	std::unique_ptr<Manifest> m = Manifest::create("testds/.archive/last");
	m->openRW();

    Summary s;
    scan::scan("inbound/test.grib1", [&](unique_ptr<Metadata> md) { s.add(*md); return true; });

	m->acquire("a.grib1", 1000010, s);
	m->acquire("foo/b.grib1", 1000011, s);

    vector<string> files = m->file_list(Matcher());
    ensure_equals(files.size(), 2u);
    ensure_equals(files[0], "a.grib1");
    ensure_equals(files[1], "foo/b.grib1");

    m->remove("a.grib1");
    files = m->file_list(Matcher());
    ensure_equals(files.size(), 1u);
    ensure_equals(files[0], "foo/b.grib1");
});

// TODO: Retest with sqlite

// Test modifying index during scanning/listing of segments
add_method("modify_while_scanning", [] {
    clean();

#warning TODO: move to index-test once acquire is part of the index interface
    // Index data about a sample file
    time_t mtime = sys::timestamp("inbound/test-sorted.grib1");

    // Generate their metadata and summary files
    metadata::Collection mdc("inbound/test-sorted.grib1");
    Summary s;
    for (const auto& md: mdc) s.add(*md);

    // Build index
    {
        std::unique_ptr<Manifest> m = Manifest::create("testds/.archive/last");
        m->openRW();
        m->acquire("10.grib1", mtime, s);
        m->acquire("20.grib1", mtime, s);
        m->acquire("30.grib1", mtime, s);
    }

    // Enumerate with list_segments while adding files
    {
        std::unique_ptr<Manifest> m = Manifest::create("testds/.archive/last");
        m->openRW();
        size_t count = 0;
        m->list_segments([&](const std::string&) {
            ++count;
            m->acquire("40.grib1", mtime, s);
        });
        // The enumeration should return only the files previously known
        wassert(actual(count) == 3u);
    }

    // Check again, we should have all that we added so far
    {
        std::unique_ptr<Manifest> m = Manifest::create("testds/.archive/last");
        m->openRW();
        size_t count = 0;
        m->list_segments([&](const std::string&) { ++count; });
        wassert(actual(count) == 4u);
    }
});

}

}
