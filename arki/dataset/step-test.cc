#include "arki/dataset/step.h"
#include "arki/matcher.h"
#include "arki/matcher/parser.h"
#include "arki/metadata.h"
#include "arki/tests/tests.h"
#include "arki/types/reftime.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include <algorithm>
#include <memory>
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::dataset;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::tests;

namespace {

struct Fixture : public arki::utils::tests::Fixture
{
    core::Time time;

    Fixture() : time(2007, 6, 5, 4, 3, 2) {}

    // Called before each test
    void test_setup() { std::filesystem::create_directory("test_step"); }

    // Called after each test
    void test_teardown() { sys::rmtree("test_step"); }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} tests("arki_dataset_step");

inline const matcher::OR& mimpl(const Matcher& m)
{
    return *m.get(TYPE_REFTIME);
}

void Tests::register_tests()
{

    add_method("single", [](Fixture& f) {
        matcher::Parser parser;
        auto step = Step::create("single");

        wassert(actual((*step)(f.time)) == "all");
        wassert_true(step->pathMatches("all.test",
                                       mimpl(parser.parse("reftime:>2006"))));
        wassert_true(step->pathMatches("all.test",
                                       mimpl(parser.parse("reftime:<=2008"))));

        std::filesystem::create_directory("test_step/");
        files::createFlagfile("test_step/all.grib");
        files::createFlagfile("test_step/all.bufr");

        vector<string> res;
        step->list_segments(
            step::SegmentQuery("test_step", DataFormat::GRIB,
                               parser.parse("reftime:<2002")),
            [&](std::string&& s) { res.emplace_back(move(s)); });
        std::sort(res.begin(), res.end());
        wassert(actual(res.size()) == 1u);
        wassert(actual(res[0]) == "all.grib");

        res.clear();
        step->list_segments(
            step::SegmentQuery("test_step", DataFormat::GRIB),
            [&](std::string&& s) { res.emplace_back(move(s)); });
        wassert(actual(res.size()) == 1u);

        res.clear();
        step->list_segments(
            step::SegmentQuery("test_step", DataFormat::GRIB,
                               parser.parse("origin:GRIB1,98")),
            [&](std::string&& s) { res.emplace_back(move(s)); });
        wassert(actual(res.size()) == 1u);

        core::Interval interval;
        step->time_extremes(step::SegmentQuery("test_step", DataFormat::GRIB),
                            interval);
        wassert_true(interval.begin.is_set());
        wassert_true(interval.end.is_set());
        wassert(actual(interval.begin) == core::Time(1000, 1, 1));
        wassert(actual(interval.end) == core::Time(100000, 1, 1));
    });

    add_method("yearly", [](Fixture& f) {
        matcher::Parser parser;
        auto step = Step::create("yearly");

        wassert(actual((*step)(f.time)) == "20/2007");
        wassert_true(step->pathMatches("20/2007.test",
                                       mimpl(parser.parse("reftime:>2006"))));
        wassert_true(step->pathMatches("20/2007.test",
                                       mimpl(parser.parse("reftime:<=2008"))));
        wassert_false(step->pathMatches("20/2007.test",
                                        mimpl(parser.parse("reftime:>2007"))));
        wassert_false(step->pathMatches("20/2007.test",
                                        mimpl(parser.parse("reftime:<2007"))));

        std::filesystem::create_directory("test_step/19");
        files::createFlagfile("test_step/19/1998.grib");
        files::createFlagfile("test_step/19/1998.bufr");
        std::filesystem::create_directory("test_step/20");
        files::createFlagfile("test_step/20/2001.grib");
        files::createFlagfile("test_step/20/2002.grib");

        vector<string> res;
        step->list_segments(
            step::SegmentQuery("test_step", DataFormat::GRIB,
                               parser.parse("reftime:<2002")),
            [&](std::string&& s) { res.emplace_back(move(s)); });
        std::sort(res.begin(), res.end());
        wassert(actual(res.size()) == 2u);
        wassert(actual(res[0]) == "19/1998.grib");
        wassert(actual(res[1]) == "20/2001.grib");

        res.clear();
        step->list_segments(
            step::SegmentQuery("test_step", DataFormat::GRIB,
                               parser.parse("reftime:>=2002")),
            [&](std::string&& s) { res.emplace_back(move(s)); });
        wassert(actual(res.size()) == 1u);
        wassert(actual(res[0]) == "20/2002.grib");

        res.clear();
        step->list_segments(
            step::SegmentQuery("test_step", DataFormat::GRIB),
            [&](std::string&& s) { res.emplace_back(move(s)); });
        wassert(actual(res.size()) == 3u);

        res.clear();
        step->list_segments(
            step::SegmentQuery("test_step", DataFormat::GRIB,
                               parser.parse("origin:GRIB1,98")),
            [&](std::string&& s) { res.emplace_back(move(s)); });
        wassert(actual(res.size()) == 3u);

        core::Interval interval;
        step->time_extremes(step::SegmentQuery("test_step", DataFormat::GRIB),
                            interval);
        wassert_true(interval.begin.is_set());
        wassert_true(interval.end.is_set());
        wassert(actual(interval.begin) == core::Time(1998, 1, 1));
        wassert(actual(interval.end) == core::Time(2003, 1, 1));
    });

    add_method("monthly", [](Fixture& f) {
        matcher::Parser parser;
        auto step = Step::create("monthly");

        wassert(actual((*step)(f.time)) == "2007/06");

        std::filesystem::create_directory("test_step/2007");
        files::createFlagfile("test_step/2007/01.grib");
        files::createFlagfile("test_step/2007/01.bufr");
        std::filesystem::create_directory("test_step/2008");
        files::createFlagfile("test_step/2008/06.grib");
        std::filesystem::create_directory("test_step/2008/07.grib");
        std::filesystem::create_directory("test_step/2009");
        files::createFlagfile("test_step/2009/11.grib");
        files::createFlagfile("test_step/2009/12.grib");

        vector<string> res;
        step->list_segments(
            step::SegmentQuery("test_step", DataFormat::GRIB,
                               parser.parse("reftime:<2009-11-15")),
            [&](std::string&& s) { res.emplace_back(move(s)); });
        std::sort(res.begin(), res.end());
        wassert(actual(res.size()) == 4u);
        wassert(actual(res[0]) == "2007/01.grib");
        wassert(actual(res[1]) == "2008/06.grib");
        wassert(actual(res[2]) == "2008/07.grib");
        wassert(actual(res[3]) == "2009/11.grib");

        res.clear();
        step->list_segments(
            step::SegmentQuery("test_step", DataFormat::GRIB,
                               parser.parse("reftime:>=2009-12-01")),
            [&](std::string&& s) { res.emplace_back(move(s)); });
        wassert(actual(res.size()) == 1u);
        wassert(actual(res[0]) == "2009/12.grib");

        res.clear();
        step->list_segments(
            step::SegmentQuery("test_step", DataFormat::GRIB),
            [&](std::string&& s) { res.emplace_back(move(s)); });
        wassert(actual(res.size()) == 5u);

        res.clear();
        step->list_segments(
            step::SegmentQuery("test_step", DataFormat::GRIB,
                               parser.parse("origin:GRIB1,98")),
            [&](std::string&& s) { res.emplace_back(move(s)); });
        wassert(actual(res.size()) == 5u);

        core::Interval interval;
        step->time_extremes(step::SegmentQuery("test_step", DataFormat::GRIB),
                            interval);
        wassert_true(interval.begin.is_set());
        wassert_true(interval.end.is_set());
        wassert(actual(interval.begin) == core::Time(2007, 1, 1));
        wassert(actual(interval.end) == core::Time(2010, 1, 1));
    });

    add_method("biweekly", [](Fixture& f) {
        auto step = Step::create("biweekly");

        wassert(actual((*step)(f.time)) == "2007/06-1");
    });

    add_method("weekly", [](Fixture& f) {
        auto step = Step::create("weekly");

        wassert(actual((*step)(f.time)) == "2007/06-1");
    });

    add_method("daily", [](Fixture& f) {
        matcher::Parser parser;
        auto step = Step::create("daily");

        wassert(actual((*step)(f.time)) == "2007/06-05");

        std::filesystem::create_directory("test_step/2007");
        files::createFlagfile("test_step/2007/01-01.grib");
        files::createFlagfile("test_step/2007/01-01.bufr");
        std::filesystem::create_directory("test_step/2008");
        files::createFlagfile("test_step/2008/06-01.grib");
        std::filesystem::create_directory("test_step/2008/06-05.grib");
        std::filesystem::create_directory("test_step/2009");
        files::createFlagfile("test_step/2009/12-29.grib");
        files::createFlagfile("test_step/2009/12-30.grib");

        vector<string> res;
        step->list_segments(
            step::SegmentQuery("test_step", DataFormat::GRIB,
                               parser.parse("reftime:<2009-12-30")),
            [&](std::string&& s) { res.emplace_back(move(s)); });
        std::sort(res.begin(), res.end());
        wassert(actual(res.size()) == 4u);
        wassert(actual(res[0]) == "2007/01-01.grib");
        wassert(actual(res[1]) == "2008/06-01.grib");
        wassert(actual(res[2]) == "2008/06-05.grib");
        wassert(actual(res[3]) == "2009/12-29.grib");

        res.clear();
        step->list_segments(
            step::SegmentQuery("test_step", DataFormat::GRIB,
                               parser.parse("reftime:>=2009-12-30")),
            [&](std::string&& s) { res.emplace_back(move(s)); });
        wassert(actual(res.size()) == 1u);
        wassert(actual(res[0]) == "2009/12-30.grib");

        res.clear();
        step->list_segments(
            step::SegmentQuery("test_step", DataFormat::GRIB),
            [&](std::string&& s) { res.emplace_back(move(s)); });
        wassert(actual(res.size()) == 5u);

        res.clear();
        step->list_segments(
            step::SegmentQuery("test_step", DataFormat::GRIB,
                               parser.parse("origin:GRIB1,98")),
            [&](std::string&& s) { res.emplace_back(move(s)); });
        wassert(actual(res.size()) == 5u);

        core::Interval interval;
        step->time_extremes(step::SegmentQuery("test_step", DataFormat::GRIB),
                            interval);
        wassert_true(interval.begin.is_set());
        wassert_true(interval.end.is_set());
        wassert(actual(interval.begin) == core::Time(2007, 1, 1));
        wassert(actual(interval.end) == core::Time(2009, 12, 31));
    });
}

} // namespace
