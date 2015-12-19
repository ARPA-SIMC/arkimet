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

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::tests;
using namespace arki::utils;
using namespace arki::dataset::index;

struct arki_dataset_index_manifest_shar : public DatasetTest {
	arki_dataset_index_manifest_shar()
	{
		system("rm -rf testds");
		system("mkdir testds");
		system("mkdir testds/.archive");
		system("mkdir testds/.archive/last");
	}

	std::string idxfname() const
	{
		return Manifest::get_force_sqlite() ? "index.sqlite" : "MANIFEST";
	}
};
TESTGRP(arki_dataset_index_manifest);

// Acquire and query
def_test(1)
{
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
}


// Test accessing empty manifests
def_test(2)
{
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

	vector<string> files;
	m->fileList(Matcher(), files);
	ensure(files.empty());
}

// Retest with sqlite

// Test creating a new manifest
def_test(4)
{
    // Opening a missing manifest read-write creates a new one
    ensure(!sys::exists("testds/.archive/last/" + idxfname()));
    std::unique_ptr<Manifest> m = Manifest::create("testds/.archive/last");
    m->openRW();
    m->flush();
    ensure(sys::exists("testds/.archive/last/" + idxfname()));

    MaintenanceCollector c;
    unique_ptr<dataset::data::SegmentManager> sm(dataset::data::SegmentManager::get("testds/.archive/last"));
    m->check(*sm, c);
	ensure_equals(c.fileStates.size(), 0u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());

	m->vacuum();
}

// Retest with sqlite


// Test adding and removing files
def_test(6)
{
	system("cp inbound/test.grib1 testds/.archive/last/a.grib1");
	system("mkdir testds/.archive/last/foo");
	system("cp inbound/test.grib1 testds/.archive/last/foo/b.grib1");

	std::unique_ptr<Manifest> m = Manifest::create("testds/.archive/last");
	m->openRW();

    Summary s;
    scan::scan("inbound/test.grib1", [&](unique_ptr<Metadata> md) { s.add(*md); return true; });

	m->acquire("a.grib1", 1000010, s);
	m->acquire("foo/b.grib1", 1000011, s);

	vector<string> files;
	m->fileList(Matcher(), files);
	ensure_equals(files.size(), 2u);
	ensure_equals(files[0], "a.grib1");
	ensure_equals(files[1], "foo/b.grib1");

	m->remove("a.grib1");
	files.clear();
	m->fileList(Matcher(), files);
	ensure_equals(files.size(), 1u);
	ensure_equals(files[0], "foo/b.grib1");
}

// Retest with sqlite

namespace {
struct IndexingCollector : public MaintenanceCollector
{
	Manifest& m;
	const Summary& s;
	time_t mtime;

	IndexingCollector(Manifest& m, const Summary& s, time_t mtime) : m(m), s(s), mtime(mtime) {}

    virtual void operator()(const std::string& file, dataset::data::FileState state)
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
}

// Test modifying index during maintenance
def_test(8)
{
    // Start with 4 data files
    system("cp -a inbound/test-sorted.grib1 testds/.archive/last/10.grib1");
    system("cp -a inbound/test-sorted.grib1 testds/.archive/last/20.grib1");
    system("cp -a inbound/test-sorted.grib1 testds/.archive/last/30.grib1");
    system("cp -a inbound/test-sorted.grib1 testds/.archive/last/40.grib1");
    system("cp -a inbound/test-sorted.grib1 testds/.archive/last/50.grib1");
    time_t mtime = sys::timestamp("inbound/test-sorted.grib1");

    // Generate their metadata and summary files
    metadata::Collection mdc("inbound/test-sorted.grib1");
    Summary s;
    for (const auto& md: mdc) s.add(*md);
    for (int i = 10; i <= 50; i += 10)
    {
        char buf[128];
        snprintf(buf, 128, "testds/.archive/last/%02d.grib1.metadata", i);
        mdc.writeAtomically(buf);
        snprintf(buf, 128, "testds/.archive/last/%02d.grib1.summary", i);
        s.writeAtomically(buf);
    }

	// Build index
	{
		std::unique_ptr<Manifest> m = Manifest::create("testds/.archive/last");
		m->openRW();
		m->acquire("10.grib1", mtime, s);
		//m->acquire("20.grib1", mtime, s);
		m->acquire("30.grib1", mtime, s);
		//m->acquire("40.grib1", mtime, s);
		m->acquire("50.grib1", mtime, s);
	}

	// Check and messily fix
	{
		std::unique_ptr<Manifest> m = Manifest::create("testds/.archive/last");
		IndexingCollector c(*m, s, mtime);
		m->openRW();
        unique_ptr<dataset::data::SegmentManager> sm(dataset::data::SegmentManager::get("testds/.archive/last"));
        m->check(*sm, c);
		ensure_equals(c.fileStates.size(), 5u);
		ensure_equals(c.count(COUNTED_TO_INDEX), 2u);
		ensure_equals(c.count(COUNTED_OK), 3u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	// Check again, everything should be fine
	{
		std::unique_ptr<Manifest> m = Manifest::create("testds/.archive/last");
		MaintenanceCollector c;
		m->openRO();
        unique_ptr<dataset::data::SegmentManager> sm(dataset::data::SegmentManager::get("testds/.archive/last"));
        m->check(*sm, c);
		ensure_equals(c.fileStates.size(), 5u);
		ensure_equals(c.count(COUNTED_OK), 5u);
		ensure_equals(c.remaining(), string());
		ensure(c.isClean());
	}
}

// Retest with sqlite

}
