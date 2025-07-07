#include "tests.h"
#include "timerange.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;

class Tests : public TypeTestCase<types::Timerange>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_timerange");

void Tests::register_tests()
{

    // Check GRIB1 with seconds
    add_generic_test("grib1_seconds",
                     {
                         "GRIB1(2, 2s, 2s)",
                     },
                     "GRIB1(2, 2s, 3s)",
                     {
                         "GRIB1(2, 2s, 4s)",
                         "GRIB1(2, 3s, 3s)",
                         "GRIB1(2, 2h, 2h)",
                         "GRIB1(2, 2y, 2y)",
                     },
                     "GRIB1, 002, 002s, 003s");

    // Check GRIB1 with hours
    add_generic_test("grib1_hours",
                     {
                         "GRIB1(2, 3s, 4s)",
                     },
                     "GRIB1(2, 2h, 3h)",
                     {
                         "GRIB1(2, 3h, 4h)",
                         "GRIB1(2, 2y, 3y)",
                     },
                     "GRIB1, 002, 002h, 003h");

    // Check GRIB1 with years
    add_generic_test("grib1_years",
                     {
                         "GRIB1(2, 2s, 3s)",
                         "GRIB1(2, 2h, 3h)",
                         "GRIB1(2, 2y, 2y)",
                     },
                     "GRIB1(2, 2y, 3y)",
                     {
                         "GRIB1(2, 2y, 4y)",
                         "GRIB1(2, 3y, 3y)",
                     },
                     "GRIB1, 002, 002y, 003y");

    // Check GRIB1 with unknown values
    add_generic_test("grib1_unknown",
                     {
                         "GRIB1(250, 125s, 126s)",
                         "GRIB1(250, 123h, 127h)",
                         "GRIB1(250, 124h, 126h)",
                     },
                     "GRIB1(250, 124h, 127h)",
                     {
                         "GRIB1(250, 124y, 127y)",
                     },
                     "GRIB1, 250, 124h, 127h");

    // Check GRIB2 with seconds
    add_generic_test("grib2_seconds",
                     {
                         "GRIB1(2, 2y, 3y)",
                         "GRIB1(2, 2h, 3h)",
                         "GRIB2(2, 254, 1s, 3s)",
                         "GRIB2(2, 254, 2s, 2s)",
                     },
                     "GRIB2(2, 254, 2s, 3s)",
                     {
                         "GRIB2(2, 254, 3s, 4s)",
                     },
                     "GRIB2,2,254,2,3");

    // Check GRIB2 with hours
    add_generic_test("grib2_hours",
                     {
                         "GRIB1(2, 1s, 3s)",
                         "GRIB1(2, 2s, 4s)",
                         "GRIB2(2, 1, 1h, 3h)",
                         "GRIB2(2, 1, 2h, 2h)",
                     },
                     "GRIB2(2, 1, 2h, 3h)",
                     {
                         "GRIB2(2, 254, 3s, 3s)",
                         "GRIB2(2, 254, 2s, 4s)",
                         "GRIB2(2, 1, 2h, 4h)",
                         "GRIB2(2, 1, 3h, 3h)",
                         "GRIB2(2, 4, 2y, 3y)",
                     },
                     "GRIB2,2,1,2,3");

    // Check GRIB2 with years
    add_generic_test("grib2_years",
                     {
                         "GRIB1(2, 5y, 5y)",
                         "GRIB2(2, 1, 1h, 3h)",
                         "GRIB2(2, 1, 2h, 2h)",
                     },
                     "GRIB2(2, 4, 2y, 3y)",
                     {
                         "GRIB2(2, 4, 2y, 4y)",
                         "GRIB2(2, 4, 3y, 3y)",
                         "GRIB2(2, 254, 2s, 3s)",
                     },
                     "GRIB2,2,4,2,3");

    // Check GRIB2 with negative values
    add_generic_test("grib2_negative",
                     {
                         "GRIB1(2, 2y, 3y)",
                         "GRIB2(2, 1, -3h, -3h)",
                         "GRIB2(2, 1, -2h, -4h)",
                     },
                     "GRIB2(2, 1, -2h, -3h)",
                     {
                         "GRIB2(2, 1, -1h, -3h)",
                         "GRIB2(2, 1, -2h, -2h)",
                         "GRIB2(2, 4, -2y, -3y)",
                         "GRIB2(2, 4, -2y, -3y)",
                         "GRIB2(2, 254, -2s, -3s)",
                         "GRIB2(2, 254, -3s, -3s)",
                     },
                     "GRIB2,2,1,-2,-3");

    // Check GRIB2 with unknown values
    add_generic_test("grib2_unknown",
                     {
                         "GRIB1(250, -2y, -3y)",
                         "GRIB2(250, 1, -3h, -3h)",
                         "GRIB2(250, 1, -2h, -4h)",
                     },
                     "GRIB2(250, 1, -2h, -3h)",
                     {
                         "GRIB2(250, 1, 2h, 3h)",
                         "GRIB2(250, 254, -2s, -3s)",
                     },
                     "GRIB2,250,1,-2,-3");

    // Check Timedef with step and statistical processing
    add_generic_test(
        "timedef_step_stat",
        {
            "Timedef(5h,2,60m)",
            "Timedef(6h,2)",
            "GRIB1(2, 2h, 3h)",
            "GRIB1(2, -2s, -3s)",
            "GRIB2(2, 254, -2s, -3s)",
        },
        {
            "Timedef(6h,2,60m)",
            "Timedef(360m, 2, 1h)",
            "Timedef(21600s,2,3600s)",
        },
        // FIXME Undefined statistical analysis type currently sorts higher than
        // everything else: fix it when it is safe to do so
        {
            "Timedef(6h)",
            "Timedef(6h,3,60m)",
            "Timedef(6h,2,61m)",
            "Timedef(6mo,2,60m)",
        },
        "Timedef,6h,2,60m");

    // Check Timedef with step and statistical processing, in months
    add_generic_test(
        "timedef_step_stat_months",
        {
            "GRIB1(2, 2h, 3h)",
            "GRIB2(2, 254, -2s, -3s)",
            "Timedef(6h,2)",
            "Timedef(6h,2,61m)",
            "Timedef(6mo,2,60m)",
            "Timedef(1y,2)",
        },
        "Timedef(1y,2,3mo)",
        {
            "Timedef(1y,3)",
            // FIXME Undefined statistical analysis type currently sorts higher
            // than everything else: fix it when it is safe to do so
            "Timedef(1y)",
            "Timedef(1y,3,3mo)",
            "Timedef(1y,2,4mo)",
            "Timedef(2y,2,3mo)",
        },
        "Timedef,1y,2,3mo");

    // Check Timedef with step only
    add_generic_test(
        "timedef_step",
        {
            "GRIB1(2, 2h, 3h)",
            "GRIB2(2, 254, -2s, -3s)",
            "Timedef(6h,2)",
            // FIXME Undefined statistical analysis type currently sorts higher
            // than everything else: fix it when it is safe to do so
            "Timedef(1d,0)",
            "Timedef(1d,0,0s)",
            "Timedef(1m)",
        },
        {
            "Timedef(1d)",
            "Timedef(24h)",
            "Timedef(1440m)",
            "Timedef(86400s)",
        },
        {
            "Timedef(2d)",
            "Timedef(1mo)",
        },
        "Timedef,1d,-");

    // Check Timedef with step only, in months
    add_generic_test(
        "timedef_step_months",
        {
            "GRIB1(2, 2h, 3h)",
            "GRIB2(2, 254, -2s, -3s)",
            "Timedef(6h,2)",
            "Timedef(1ce)",
            "Timedef(2d)",
            // FIXME Undefined statistical analysis type currently sorts higher
            // than everything else: fix it when it is safe to do so
            "Timedef(2ce,0)",
            "Timedef(2ce,0,0s)",
        },
        {
            "Timedef(2ce)",
            "Timedef(20de)",
            "Timedef(200y)",
            "Timedef(2400mo)",
        },
        {
            "Timedef(3ce)",
        },
        "Timedef,2ce,-");

    // Check Timedef2 with step, and only statistical process type
    add_generic_test(
        "timedef_step_ptype",
        {"GRIB1(2, 2h, 3h)", "GRIB2(2, 254, -2s, -3s)", "Timedef(5h,2)"},
        {"Timedef(6h,2)", "Timedef(360m, 2)", "Timedef(21600s,2)"},
        // FIXME Undefined statistical analysis type currently sorts higher than
        // everything else: fix it when it is safe to do so
        {
            "Timedef(6h)",
            "Timedef(6h,3)",
            "Timedef(6h,2,60m)",
            "Timedef(2d)",
        },
        "Timedef,6h,2,-");

    // Check Timedef with step in months, and only statistical process type
    add_generic_test(
        "timedef_step_months_ptype",
        {
            "GRIB1(2, 2h, 3h)",
            "GRIB2(2, 254, -2s, -3s)",
            "Timedef(5h,2)",
            "Timedef(6h)",
            "Timedef(5no)",
            "Timedef(5no,2)",
        },
        {
            "Timedef(6no,2)",
            "Timedef(180y, 2)",
            "Timedef(18de, 2)",
            "Timedef(2160mo, 2)",
        },
        // FIXME Undefined statistical analysis type currently sorts higher than
        // everything else: fix it when it is safe to do so
        {
            "Timedef(6no)",
            "Timedef(6no,3)",
            "Timedef(6no,2,60d)",
        },
        "Timedef,6no,2,-");

    // Check Timedef with step and statistical processing, one in seconds and
    // one in months
    add_generic_test(
        "timedef_step_stats_secs_months",
        {"GRIB1(2, 2h, 3h)", "GRIB2(2, 254, -2s, -3s)", "Timedef(5h,2)",
         "Timedef(6h)", "Timedef(1y,2)"},
        vector<string>({
            "Timedef(1y,2,3d)",
            "Timedef(12mo,2,72h)",
        }),
        // FIXME Undefined statistical analysis type currently sorts higher than
        // everything else: fix it when it is safe to do so
        {"Timedef(1y)", "Timedef(1y,2,4d)", "Timedef(1y,3,3d)",
         "Timedef(2y,2,3d)", "Timedef(6no,3)", "Timedef(6no,2,60d)"},
        "Timedef,1y,2,3d");

    // Check BUFR
    add_generic_test(
        "bufr",
        {"GRIB1(2, 2h, 3h)", "GRIB2(2, 254, -2s, -3s)", "BUFR(5h)", "BUFR(6s)"},
        "BUFR(6h)",
        {"BUFR(7h)", "BUFR(6mo)", "Timedef(5h,2)", "Timedef(6no,3)"},
        "BUFR,6h");

    add_method("grib1_details", [] {
        unique_ptr<Timerange> o = Timerange::createGRIB1(2, 254, 2, 3);
        wassert(actual(o->style()) == Timerange::Style::GRIB1);
        const timerange::GRIB1* v = dynamic_cast<timerange::GRIB1*>(o.get());
        {
            unsigned type, unit, p1, p2;
            v->get_GRIB1(type, unit, p1, p2);

            wassert(actual(type) == 2u);
            wassert(actual(unit) == 254u);
            wassert(actual(p1) == 2u);
            wassert(actual(p2) == 3u);
        }

        timerange::GRIB1Unit u;
        int t, p1, p2;
        bool use_p1, use_p2;
        v->get_GRIB1_normalised(t, u, p1, p2, use_p1, use_p2);
        wassert(actual(t) == 2);
        wassert(actual(u) == timerange::SECOND);
        wassert(actual(p1) == 2);
        wassert(actual(p2) == 3);

        o = Timerange::createGRIB1(2, 1, 2, 3);
        wassert(actual(o->style()) == Timerange::Style::GRIB1);
        v = dynamic_cast<timerange::GRIB1*>(o.get());
        {
            unsigned type, unit, p1, p2;
            v->get_GRIB1(type, unit, p1, p2);
            wassert(actual(type) == 2u);
            wassert(actual(unit) == 1u);
            wassert(actual(p1) == 2u);
            wassert(actual(p2) == 3u);
        }

        v->get_GRIB1_normalised(t, u, p1, p2, use_p1, use_p2);
        wassert(actual(t) == 2);
        wassert(actual(u) == timerange::SECOND);
        wassert(actual(p1) == 2 * 3600);
        wassert(actual(p2) == 3 * 3600);

        o = Timerange::createGRIB1(2, 4, 2, 3);
        wassert(actual(o->style()) == Timerange::Style::GRIB1);
        v = dynamic_cast<timerange::GRIB1*>(o.get());
        {
            unsigned type, unit, p1, p2;
            v->get_GRIB1(type, unit, p1, p2);
            wassert(actual(type) == 2u);
            wassert(actual(unit) == 4u);
            wassert(actual(p1) == 2u);
            wassert(actual(p2) == 3u);
        }

        v->get_GRIB1_normalised(t, u, p1, p2, use_p1, use_p2);
        wassert(actual(t) == 2);
        wassert(actual(u) == timerange::MONTH);
        wassert(actual(p1) == 2 * 12);
        wassert(actual(p2) == 3 * 12);

        o = Timerange::createGRIB1(250, 1, 124, 127);
        wassert(actual(o->style()) == Timerange::Style::GRIB1);
        v = dynamic_cast<timerange::GRIB1*>(o.get());
        {
            unsigned type, unit, p1, p2;
            v->get_GRIB1(type, unit, p1, p2);
            wassert(actual(type) == 250u);
            wassert(actual(unit) == 1u);
            wassert(actual(p1) == 124u);
            wassert(actual(p2) == 127u);
        }

        v->get_GRIB1_normalised(t, u, p1, p2, use_p1, use_p2);
        wassert(actual(t) == 250);
        wassert(actual(u) == timerange::SECOND);
        wassert(actual(p1) == 124 * 3600);
        wassert(actual(p2) == 127 * 3600);
    });

    add_method("grib2_details", [] {
        unique_ptr<Timerange> o = Timerange::createGRIB2(2, 254, 2, 3);
        wassert(actual(o->style()) == Timerange::Style::GRIB2);
        const timerange::GRIB2* v = dynamic_cast<timerange::GRIB2*>(o.get());
        unsigned type, unit;
        signed long p1, p2;
        v->get_GRIB2(type, unit, p1, p2);
        wassert(actual(type) == 2u);
        wassert(actual(unit) == 254u);
        wassert(actual(p1) == 2);
        wassert(actual(p2) == 3);

        o = Timerange::createGRIB2(2, 1, 2, 3);
        wassert(actual(o->style()) == Timerange::Style::GRIB2);
        v = dynamic_cast<timerange::GRIB2*>(o.get());
        v->get_GRIB2(type, unit, p1, p2);
        wassert(actual(type) == 2u);
        wassert(actual(unit) == 1u);
        wassert(actual(p1) == 2);
        wassert(actual(p2) == 3);

        o = Timerange::createGRIB2(2, 1, 2, 3);
        wassert(actual(o->style()) == Timerange::Style::GRIB2);
        v = dynamic_cast<timerange::GRIB2*>(o.get());
        v->get_GRIB2(type, unit, p1, p2);
        wassert(actual(type) == 2u);
        wassert(actual(unit) == 1u);
        wassert(actual(p1) == 2);
        wassert(actual(p2) == 3);

        o = Timerange::createGRIB2(2, 1, -2, -3);
        wassert(actual(o->style()) == Timerange::Style::GRIB2);
        v = dynamic_cast<timerange::GRIB2*>(o.get());
        v->get_GRIB2(type, unit, p1, p2);
        wassert(actual(type) == 2u);
        wassert(actual(unit) == 1u);
        wassert(actual(p1) == -2);
        wassert(actual(p2) == -3);

        o = Timerange::createGRIB2(250, 1, -2, -3);
        wassert(actual(o->style()) == Timerange::Style::GRIB2);
        v = dynamic_cast<timerange::GRIB2*>(o.get());
        v->get_GRIB2(type, unit, p1, p2);
        wassert(actual(type) == 250u);
        wassert(actual(unit) == 1u);
        wassert(actual(p1) == -2);
        wassert(actual(p2) == -3);

        // Check GRIB2 with some values that used to fail
        unique_ptr<Timerange> o1   = Timerange::createGRIB2(11, 1, 3, 3);
        unique_ptr<Timerange> o2   = Timerange::createGRIB2(11, 1, 3, 6);
        const timerange::GRIB2* v1 = dynamic_cast<timerange::GRIB2*>(o1.get());
        const timerange::GRIB2* v2 = dynamic_cast<timerange::GRIB2*>(o2.get());
        v1->get_GRIB2(type, unit, p1, p2);
        wassert(actual(type) == 11u);
        wassert(actual(unit) == 1u);
        wassert(actual(p1) == 3);
        wassert(actual(p2) == 3);
        v2->get_GRIB2(type, unit, p1, p2);
        wassert(actual(type) == 11u);
        wassert(actual(unit) == 1u);
        wassert(actual(p1) == 3);
        wassert(actual(p2) == 6);

        wassert(actual(o1) != o2);

        wassert(actual(o1->encodeBinary() != o2->encodeBinary()));
        // ensure(o1 < o2);
        // ensure(o2 > o1);
    });

    add_method("timedef_details", [] {
        auto v = Timerange::decodeString("Timedef(6h,2,60m)");
        timerange::TimedefUnit step_unit, stat_unit;
        unsigned step_len, stat_type, stat_len;
        v->get_Timedef(step_unit, step_len, stat_type, stat_unit, stat_len);
        wassert(actual(v->style()) == Timerange::Style::TIMEDEF);
        wassert(actual(step_unit) == timerange::UNIT_HOUR);
        wassert(actual(step_len) == 6u);
        wassert(actual(stat_type) == 2u);
        wassert(actual(stat_unit) == timerange::UNIT_MINUTE);
        wassert(actual(stat_len) == 60u);

        wassert(actual(v) == Timerange::createTimedef(6, timerange::UNIT_HOUR,
                                                      2, 60,
                                                      timerange::UNIT_MINUTE));

        v = Timerange::decodeString("Timedef(1y,2,3mo)");
        v->get_Timedef(step_unit, step_len, stat_type, stat_unit, stat_len);
        wassert(actual(v->style()) == Timerange::Style::TIMEDEF);
        wassert(actual(step_unit) == timerange::UNIT_YEAR);
        wassert(actual(step_len) == 1u);
        wassert(actual(stat_type) == 2u);
        wassert(actual(stat_unit) == timerange::UNIT_MONTH);
        wassert(actual(stat_len) == 3u);

        wassert(actual(v) == Timerange::createTimedef(1, timerange::UNIT_YEAR,
                                                      2, 3,
                                                      timerange::UNIT_MONTH));
        wassert(actual(v) == Timerange::decodeString("Timedef(12mo, 2, 3mo)"));

        v = Timerange::decodeString("Timedef(1d)");
        v->get_Timedef(step_unit, step_len, stat_type, stat_unit, stat_len);
        wassert(actual(v->style()) == Timerange::Style::TIMEDEF);
        wassert(actual(step_unit) == timerange::UNIT_DAY);
        wassert(actual(step_len) == 1u);
        wassert(actual(stat_type) == 255u);
        wassert(actual(stat_unit) == timerange::UNIT_MISSING);
        wassert(actual(stat_len) == 0u);

        wassert(actual(v) == Timerange::createTimedef(1, timerange::UNIT_DAY));

        v = Timerange::decodeString("Timedef(2ce)");
        v->get_Timedef(step_unit, step_len, stat_type, stat_unit, stat_len);
        wassert(actual(v->style()) == Timerange::Style::TIMEDEF);
        wassert(actual(step_unit) == timerange::UNIT_CENTURY);
        wassert(actual(step_len) == 2u);
        wassert(actual(stat_type) == 255u);
        wassert(actual(stat_unit) == timerange::UNIT_MISSING);
        wassert(actual(stat_len) == 0u);

        wassert(actual(v) ==
                Timerange::createTimedef(2, timerange::UNIT_CENTURY));

        v = Timerange::decodeString("Timedef(6h,2)");
        v->get_Timedef(step_unit, step_len, stat_type, stat_unit, stat_len);
        wassert(actual(v->style()) == Timerange::Style::TIMEDEF);
        wassert(actual(step_unit) == timerange::UNIT_HOUR);
        wassert(actual(step_len) == 6u);
        wassert(actual(stat_type) == 2u);
        wassert(actual(stat_unit) == timerange::UNIT_MISSING);
        wassert(actual(stat_len) == 0u);

        wassert(actual(v) == Timerange::createTimedef(6, timerange::UNIT_HOUR,
                                                      2, 0,
                                                      timerange::UNIT_MISSING));

        v = Timerange::decodeString("Timedef(6no,2)");
        v->get_Timedef(step_unit, step_len, stat_type, stat_unit, stat_len);
        wassert(actual(v->style()) == Timerange::Style::TIMEDEF);
        wassert(actual(step_unit) == timerange::UNIT_NORMAL);
        wassert(actual(step_len) == 6u);
        wassert(actual(stat_type) == 2u);
        wassert(actual(stat_unit) == timerange::UNIT_MISSING);
        wassert(actual(stat_len) == 0u);

        wassert(actual(v) == Timerange::createTimedef(6, timerange::UNIT_NORMAL,
                                                      2, 0,
                                                      timerange::UNIT_MISSING));

        v = Timerange::decodeString("Timedef(1y,2,3d)");
        v->get_Timedef(step_unit, step_len, stat_type, stat_unit, stat_len);
        wassert(actual(v->style()) == Timerange::Style::TIMEDEF);
        wassert(actual(step_unit) == timerange::UNIT_YEAR);
        wassert(actual(step_len) == 1u);
        wassert(actual(stat_type) == 2u);
        wassert(actual(stat_unit) == timerange::UNIT_DAY);
        wassert(actual(stat_len) == 3u);

        wassert(actual(v) == Timerange::createTimedef(1, timerange::UNIT_YEAR,
                                                      2, 3,
                                                      timerange::UNIT_DAY));
    });

    add_method("timedef_details", [] {
        unique_ptr<Timerange> o  = Timerange::createBUFR(6, 1);
        const timerange::BUFR* v = dynamic_cast<timerange::BUFR*>(o.get());
        unsigned unit, value;
        v->get_BUFR(unit, value);
        wassert(actual(v->style()) == Timerange::Style::BUFR);
        wassert(actual(unit) == 1u);
        wassert(actual(value) == 6u);
    });

    // Test computing timedef information
    add_method("timedef_info", [] {
        int val;
        bool issec;

        {
            // GRIB1, forecast at +60min
            unique_ptr<Timerange> tr(Timerange::createGRIB1(0, 0, 60, 0));

            wassert_true(tr->get_forecast_step(val, issec));
            wassert(actual(val) == 3600);
            wassert(actual(issec) == true);

            wassert(actual(tr->get_proc_type()) == 254);

            wassert_true(tr->get_proc_duration(val, issec));
            wassert(actual(val) == 0);
            wassert(actual(issec) == true);
        }

        {
            // GRIB1, average between rt+1h and rt+3h
            unique_ptr<Timerange> tr(Timerange::createGRIB1(3, 1, 1, 3));

            wassert_true(tr->get_forecast_step(val, issec));
            wassert(actual(val) == 3 * 3600);
            wassert(actual(issec) == true);

            wassert(actual(tr->get_proc_type()) == 0);

            wassert_true(tr->get_proc_duration(val, issec));
            wassert(actual(val) == 2 * 3600);
            wassert(actual(issec) == true);
        }
    });

    // Test Timedef's validity_time_to_emission_time
    add_method("timedef_validity_time_to_emission_time", [] {
        using namespace reftime;
        auto v = Timerange::decodeString("Timedef(6h)");
        auto p =
            downcast<Position>(Reftime::decodeString("2009-02-13 12:00:00"));
        auto p1 = dynamic_cast<const timerange::Timedef*>(v.get())
                      ->validity_time_to_emission_time(*p);
        wassert(actual(p1->get_Position()).is(2009, 2, 13, 6));
    });

    add_method("issue256", [] {
        std::unique_ptr<Timerange> tr(Timerange::createGRIB1(0, 1, 228, 0));

        unsigned t, u, p1, p2;
        tr->get_GRIB1(t, u, p1, p2);
        wassert(actual(t) == 0u);
        wassert(actual(u) == 1u);
        wassert(actual(p1) == 228u);
        wassert(actual(p2) == 0u);

        wassert(actual(tr->to_string()) == "GRIB1(000, 228h)");
    });
}

} // namespace
