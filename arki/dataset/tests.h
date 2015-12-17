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
struct Reader;
struct Writer;

namespace dataset {
struct LocalReader;
struct LocalWriter;

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
#define ensure_dispatches(x, y, z) wassert(impl_ensure_dispatches((x), (y), (z)))
void impl_ensure_dispatches(Dispatcher& dispatcher, std::unique_ptr<Metadata> md, metadata_dest_func mdc);

unsigned count_results(Reader& ds, const dataset::DataQuery& dq);

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
    void ensure_line_contains(const std::string& needle);
    void ensure_all_lines_seen();
};

struct LineChecker
{
    std::vector<std::string> ignore_regexps;
    std::vector<std::string> require_contains;
    std::vector<std::string> require_contains_re;

    void ignore_regexp(const std::string& regexp);
    void require_line_contains(const std::string& needle);
    void require_line_contains_re(const std::string& needle);
    void check(const std::string& s) const;
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
struct DatasetTest : public Fixture
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
    dataset::data::SegmentManager* segment_manager = nullptr;
    Metadata import_results[3];

    DatasetTest();
    ~DatasetTest();

    dataset::data::SegmentManager& segments();

	// Return the file name of the index of the current dataset
	std::string idxfname(const ConfigFile* wcfg = 0) const;
	// Return the file name of the archive index of the current dataset
	std::string arcidxfname() const;

    Reader* makeReader(const ConfigFile* wcfg = 0);
    Writer* makeWriter(const ConfigFile* wcfg = 0);
    dataset::LocalReader* makeLocalReader(const ConfigFile* wcfg = 0);
    dataset::SegmentedWriter* makeLocalWriter(const ConfigFile* wcfg = 0);
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

    void ensure_maint_clean(size_t filecount, const ConfigFile* wcfg = 0);
    void ensure_localds_clean(size_t filecount, size_t resultcount, const ConfigFile* wcfg = 0);

    void import_all(const testdata::Fixture& fixture);
    void import_all_packed(const testdata::Fixture& fixture);
};

#if 0
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
#endif

std::unique_ptr<dataset::LocalWriter> make_dataset_writer(const std::string& cfg, bool empty=true);
std::unique_ptr<Reader> make_dataset_reader(const std::string& cfg);

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
        scan::scan("inbound/test.grib1", mdc.inserter_func());
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
        scan::scan("inbound/test.bufr", mdc.inserter_func());
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
        scan::scan("inbound/test.vm2", mdc.inserter_func());
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
        scan::scan("inbound/odimh5/COMP_CAPPI_v20.h5", mdc.inserter_func());
        scan::scan("inbound/odimh5/PVOL_v20.h5", mdc.inserter_func());
        scan::scan("inbound/odimh5/XSEC_v21.h5", mdc.inserter_func());
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

template<typename DatasetWriter>
struct ActualLocalWriter : public arki::utils::tests::Actual<DatasetWriter*>
{
    ActualLocalWriter(DatasetWriter* s) : Actual<DatasetWriter*>(s) {}

    void repack(const LineChecker& expected, bool write=false);
    void repack_clean(bool write=false);
    void check(const LineChecker& expected, bool write=false, bool quick=true);
    void check_clean(bool write=false);
};

struct ActualSegmentedWriter : public ActualLocalWriter<dataset::SegmentedWriter>
{
    ActualSegmentedWriter(dataset::SegmentedWriter* s) : ActualLocalWriter<dataset::SegmentedWriter>(s) {}

    /// Run maintenance and see that the results are as expected
    void maintenance(const MaintenanceResults& expected, bool quick=true);
    void maintenance_clean(unsigned data_count, bool quick=true);
};

/// Corrupt a datafile by overwriting the first 4 bytes of its first data
/// element with zeros
void corrupt_datafile(const std::string& absname);

void test_append_transaction_ok(dataset::data::Segment* dw, Metadata& md, int append_amount_adjust=0);
void test_append_transaction_rollback(dataset::data::Segment* dw, Metadata& md);

inline arki::tests::ActualLocalWriter<dataset::LocalWriter> actual(arki::dataset::LocalWriter* actual)
{
    return arki::tests::ActualLocalWriter<dataset::LocalWriter>(actual);
}
inline arki::tests::ActualSegmentedWriter actual(arki::dataset::SegmentedWriter* actual) { return arki::tests::ActualSegmentedWriter(actual); }

}
}

#endif
