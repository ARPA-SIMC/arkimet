#ifndef ARKI_DATASET_TESTUTILS_H
#define ARKI_DATASET_TESTUTILS_H

#include <arki/metadata/tests.h>
#include <arki/configfile.h>
#include <arki/types.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/dataset/maintenance.h>
#include <arki/dataset/data.h>
#include <arki/sort.h>
#include <arki/scan/any.h>
#include <arki/utils/string.h>
#include <vector>
#include <string>
#include <sstream>

namespace arki {
struct Metadata;
struct Dispatcher;
struct ReadonlyDataset;
struct WritableDataset;

namespace dataset {
struct Local;
struct WritableLocal;

namespace ondisk2 {
struct Reader;
struct Writer;
}

namespace simple {
struct Reader;
struct Writer;
}
}

namespace testdata {
struct Fixture;
}

namespace tests {
#define ensure_dispatches(x, y, z) arki::tests::impl_ensure_dispatches(wibble::tests::Location(__FILE__, __LINE__, #x ", " #y), (x), (y), (z))
void impl_ensure_dispatches(const wibble::tests::Location& loc, Dispatcher& dispatcher, std::unique_ptr<Metadata> md, metadata::Eater& mdc);

struct OutputChecker : public std::stringstream
{
	std::vector<std::string> lines;
	bool split;

	// Split the output into lines if it has not been done yet
	void splitIfNeeded();

	// Join the split and marked lines
	std::string join() const;
	
	OutputChecker();

	void ignore_line_containing(const std::string& needle);

#define ensure_line_contains(x) impl_ensure_line_contains(wibble::tests::Location(__FILE__, __LINE__, "look for " #x), (x))
#define inner_ensure_line_contains(x) impl_ensure_line_contains(wibble::tests::Location(loc, __FILE__, __LINE__, "look for " #x), (x))
	void impl_ensure_line_contains(const wibble::tests::Location& loc, const std::string& needle);

#define ensure_all_lines_seen() impl_ensure_all_lines_seen(wibble::tests::Location(__FILE__, __LINE__, "all lines seen"))
#define inner_ensure_all_lines_seen() impl_ensure_all_lines_seen(wibble::tests::Location(loc, __FILE__, __LINE__, "all lines seen"))
	void impl_ensure_all_lines_seen(const wibble::tests::Location& loc);
};

struct LineChecker
{
    std::vector<std::string> ignore_regexps;
    std::vector<std::string> require_contains;
    std::vector<std::string> require_contains_re;

    void ignore_regexp(const std::string& regexp);
    void require_line_contains(const std::string& needle);
    void require_line_contains_re(const std::string& needle);
    void check(WIBBLE_TEST_LOCPRM, const std::string& s) const;
};

struct ForceSqlite
{
	bool old;

	ForceSqlite(bool val = true);
	~ForceSqlite();
};

// Return the number of days passed from the given date until today
int days_since(int year, int month, int day);

// Base class for dataset tests
struct DatasetTest
{
    enum Counted {
        COUNTED_OK,
        COUNTED_ARC_OK,
        COUNTED_TO_ARCHIVE,
        COUNTED_TO_DELETE,
        COUNTED_TO_PACK,
        COUNTED_TO_INDEX,
        COUNTED_TO_RESCAN,
        COUNTED_TO_DEINDEX,
        COUNTED_ARC_TO_INDEX,
        COUNTED_ARC_TO_RESCAN,
        COUNTED_ARC_TO_DEINDEX,
        COUNTED_MAX,
    };

	// Default dataset configuration (to be filled by subclasser)
	ConfigFile cfg;
    dataset::data::SegmentManager* segment_manager;
    Metadata import_results[3];

    DatasetTest();
    ~DatasetTest();

    dataset::data::SegmentManager& segments();

	// Return the file name of the index of the current dataset
	std::string idxfname(const ConfigFile* wcfg = 0) const;
	// Return the file name of the archive index of the current dataset
	std::string arcidxfname() const;

	ReadonlyDataset* makeReader(const ConfigFile* wcfg = 0);
	WritableDataset* makeWriter(const ConfigFile* wcfg = 0);
	dataset::Local* makeLocalReader(const ConfigFile* wcfg = 0);
	dataset::WritableLocal* makeLocalWriter(const ConfigFile* wcfg = 0);
	dataset::ondisk2::Reader* makeOndisk2Reader(const ConfigFile* wcfg = 0);
	dataset::ondisk2::Writer* makeOndisk2Writer(const ConfigFile* wcfg = 0);
	dataset::simple::Reader* makeSimpleReader(const ConfigFile* wcfg = 0);
	dataset::simple::Writer* makeSimpleWriter(const ConfigFile* wcfg = 0);

	// Clean the dataset directory
	void clean(const ConfigFile* wcfg = 0);

	// Import a file
	void import(const ConfigFile* wcfg = 0, const std::string& testfile = "inbound/test.grib1");

	// Recreate the dataset importing data into it
	void clean_and_import(const ConfigFile* wcfg = 0, const std::string& testfile = "inbound/test.grib1");

#define ensure_maint_clean(...) impl_ensure_maint_clean(wibble::tests::Location(__FILE__, __LINE__, #__VA_ARGS__), ##__VA_ARGS__)
	void impl_ensure_maint_clean(wibble::tests::Location, size_t filecount, const ConfigFile* wcfg = 0);

#define ensure_localds_clean(...) impl_ensure_localds_clean(wibble::tests::Location(__FILE__, __LINE__, #__VA_ARGS__), ##__VA_ARGS__)
	void impl_ensure_localds_clean(const wibble::tests::Location& loc, size_t filecount, size_t resultcount, const ConfigFile* wcfg = 0);

    void import_all(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture);
    void import_all_packed(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture);
};

struct DatasetTestDefaultConfig
{
    DatasetTestDefaultConfig(const ConfigFile& cfg);
    ~DatasetTestDefaultConfig();
};

template<typename T>
struct dataset_tg : public tut::test_group<T>
{
    ConfigFile default_config;

    dataset_tg(const char* name, const std::string& config_test)
        : tut::test_group<T>(name)
    {
        std::stringstream in(config_test);
        default_config.parse(in, "(memory)");
    }

    tut::test_result run_next()
    {
        DatasetTestDefaultConfig dtdc(default_config);
        return tut::test_group<T>::run_next();
    }
    tut::test_result run_test(int n)
    {
        DatasetTestDefaultConfig dtdc(default_config);
        return tut::test_group<T>::run_test(n);
    }
};

std::unique_ptr<dataset::WritableLocal> make_dataset_writer(const std::string& cfg, bool empty=true);
std::unique_ptr<ReadonlyDataset> make_dataset_reader(const std::string& cfg);

}

struct MaintenanceCollector : public dataset::maintenance::MaintFileVisitor
{
    typedef tests::DatasetTest::Counted Counted;

    std::map <std::string, dataset::data::FileState> fileStates;
    size_t counts[tests::DatasetTest::COUNTED_MAX];
    static const char* names[];
    std::set<Counted> checked;

    MaintenanceCollector();

    void clear();
    bool isClean() const;
    virtual void operator()(const std::string& file, dataset::data::FileState state);
    void dump(std::ostream& out) const;
    size_t count(tests::DatasetTest::Counted state);
    std::string remaining() const;
};

struct OrderCheck : public metadata::Eater
{
    std::shared_ptr<sort::Compare> order;
    Metadata old;
    bool first;

    OrderCheck(const std::string& order);
    virtual ~OrderCheck();
    bool eat(std::unique_ptr<Metadata>&& md) override;
};

namespace testdata {

struct Element
{
    Metadata md;
    types::Time time;
    std::string destfile;
    Matcher matcher;

    Element() : time(0, 0, 0) {}

    void set(const Metadata& md, const std::string& matcher)
    {
        const types::reftime::Position* rt = md.get<types::reftime::Position>();
        this->md = md;
        this->time = rt->time;
        char buf[32];
        snprintf(buf, 32, "%04d/%02d-%02d.%s", time.vals[0], time.vals[1], time.vals[2], md.source().format.c_str());
        this->destfile = buf;
        this->matcher = Matcher::parse(matcher);
    }
};

struct Fixture
{
    std::string format;
    // Maximum aggregation period that still generates more than one file
    std::string max_selective_aggregation;
    Element test_data[3];
    /// Date that falls somewhere inbetween files in the dataset
    int selective_cutoff[6];
    std::set<std::string> fnames;
    std::set<std::string> fnames_before_cutoff;
    std::set<std::string> fnames_after_cutoff;

    unsigned count_dataset_files() const;
    // Value for "archive age" or "delete age" that would work on part of the
    // dataset, but not all of it
    unsigned selective_days_since() const;
    void finalise_init();
};

struct GRIBData : Fixture
{
    GRIBData()
    {
        metadata::Collection mdc;
        scan::scan("inbound/test.grib1", mdc);
        format = "grib";
        max_selective_aggregation = "monthly";
        test_data[0].set(mdc[0], "reftime:=2007-07-08");
        test_data[1].set(mdc[1], "reftime:=2007-07-07");
        test_data[2].set(mdc[2], "reftime:=2007-10-09");
        finalise_init();
    }
};

struct BUFRData : Fixture
{
    BUFRData()
    {
        metadata::Collection mdc;
        scan::scan("inbound/test.bufr", mdc);
        format = "bufr";
        max_selective_aggregation = "yearly";
        test_data[0].set(mdc[0], "reftime:=2005-12-01");
        test_data[1].set(mdc[1], "reftime:=2004-11-30; proddef:GRIB:blo=60");
        test_data[2].set(mdc[2], "reftime:=2004-11-30; proddef:GRIB:blo=6");
        finalise_init();
    }
};

struct VM2Data : Fixture
{
    VM2Data()
    {
        metadata::Collection mdc;
        scan::scan("inbound/test.vm2", mdc);
        format = "vm2";
        max_selective_aggregation = "yearly";
        test_data[0].set(mdc[0], "reftime:=1987-10-31; product:VM2,227");
        test_data[1].set(mdc[1], "reftime:=1987-10-31; product:VM2,228");
        test_data[2].set(mdc[2], "reftime:=2011-01-01; product:VM2,1");
        finalise_init();
    }
};

struct ODIMData : Fixture
{
    ODIMData()
    {
        metadata::Collection mdc;
        format = "odim";
        max_selective_aggregation = "yearly";
        scan::scan("inbound/odimh5/COMP_CAPPI_v20.h5", mdc);
        scan::scan("inbound/odimh5/PVOL_v20.h5", mdc);
        scan::scan("inbound/odimh5/XSEC_v21.h5", mdc);
        test_data[0].set(mdc[0], "reftime:=2013-03-18");
        test_data[1].set(mdc[1], "reftime:=2000-01-02");
        test_data[2].set(mdc[2], "reftime:=2013-11-04");
        finalise_init();
    }
};

Metadata make_large_mock(const std::string& format, size_t size, unsigned month, unsigned day, unsigned hour=0);

}


namespace tests {



struct MaintenanceResults
{
    /// 0: dataset is unclean, 1: dataset is clean, -1: don't check
    int is_clean;
    /// Number of files seen during maintenance (-1 == don't check)
    int files_seen;
    /// Number of files expected for each maintenance outcome (-1 == don't check)
    int by_type[tests::DatasetTest::COUNTED_MAX];

    void reset_by_type()
    {
        for (unsigned i = 0; i < tests::DatasetTest::COUNTED_MAX; ++i)
            by_type[i] = -1;
    }

    MaintenanceResults()
        : is_clean(-1), files_seen(-1)
    {
        reset_by_type();
    }

    MaintenanceResults(bool is_clean)
        : is_clean(is_clean), files_seen(-1)
    {
        reset_by_type();
    }

    MaintenanceResults(bool is_clean, unsigned files_seen)
        : is_clean(is_clean), files_seen(files_seen)
    {
        reset_by_type();
    }
};

struct TestMaintenance
{
    dataset::WritableLocal& dataset;
    MaintenanceResults expected;
    bool quick;

    TestMaintenance(dataset::WritableLocal& dataset, const MaintenanceResults& expected, bool quick=true)
        : dataset(dataset), expected(expected), quick(quick) {}

    void check(WIBBLE_TEST_LOCPRM) const;
};

struct TestRepack
{
    dataset::WritableLocal& dataset;
    LineChecker expected;
    bool write;

    TestRepack(dataset::WritableLocal& dataset, const LineChecker& expected, bool write=false)
        : dataset(dataset), expected(expected), write(write) {}

    void check(WIBBLE_TEST_LOCPRM) const;
};

struct TestCheck
{
    dataset::WritableLocal& dataset;
    LineChecker expected;
    bool write;
    bool quick;

    TestCheck(dataset::WritableLocal& dataset, const LineChecker& expected, bool write=false, bool quick=true)
        : dataset(dataset), expected(expected), write(write), quick(quick) {}

    void check(WIBBLE_TEST_LOCPRM) const;
};

struct ActualWritableLocal : public wibble::tests::Actual<dataset::WritableLocal*>
{
    ActualWritableLocal(dataset::WritableLocal* s) : Actual<dataset::WritableLocal*>(s) {}

    /// Run maintenance and see that the results are as expected
    TestMaintenance maintenance(const MaintenanceResults& expected, bool quick=true)
    {
        return TestMaintenance(*actual, expected, quick);
    }
    TestMaintenance maintenance_clean(unsigned data_count, bool quick=true)
    {
        MaintenanceResults expected(true, data_count);
        expected.by_type[tests::DatasetTest::COUNTED_OK] = data_count;
        return TestMaintenance(*actual, expected, quick);
    }
    TestRepack repack(const LineChecker& expected, bool write=false)
    {
        return TestRepack(*actual, expected, write);
    }
    TestRepack repack_clean(bool write=false)
    {
        LineChecker expected;
        expected.ignore_regexp("total bytes freed.");
        return TestRepack(*actual, expected, write);
    }
    TestCheck check(const LineChecker& expected, bool write=false, bool quick=true)
    {
        return TestCheck(*actual, expected, write, quick);
    }
    TestCheck check_clean(bool write=false)
    {
        LineChecker expected;
        return TestCheck(*actual, expected, write);
    }
};

/// Corrupt a datafile by overwriting the first 4 bytes of its first data
/// element with zeros
void corrupt_datafile(const std::string& absname);

void test_append_transaction_ok(WIBBLE_TEST_LOCPRM, dataset::data::Segment* dw, Metadata& md, int append_amount_adjust=0);
void test_append_transaction_rollback(WIBBLE_TEST_LOCPRM, dataset::data::Segment* dw, Metadata& md);

}

}

namespace wibble {
namespace tests {

inline arki::tests::ActualWritableLocal actual(arki::dataset::WritableLocal* actual) { return arki::tests::ActualWritableLocal(actual); }

}
}

#endif
