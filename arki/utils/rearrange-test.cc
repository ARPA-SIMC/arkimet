#include "arki/tests/tests.h"
#include "arki/utils/sys.h"
#include "rearrange.h"
#include <filesystem>

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;
using arki::utils::rearrange::Span;
using arki::utils::rearrange::Plan;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_utils_rearrange");

void Tests::register_tests() {

add_method("add", [] {
    Plan spans;
    spans.add({0, 10, 5});
    spans.add({5, 0, 5});
    spans.add({10, 11, 1});
    spans.add(11, 12, 1);

    wassert(actual(spans.size()) == 3u);
    wassert(actual(spans[0]) == Span{0, 10, 5});
    wassert(actual(spans[1]) == Span{5, 0, 5});
    wassert(actual(spans[2]) == Span{10, 11, 2});
});

add_method("execute", [] {
    std::filesystem::path abspath_in("testfile1");
    std::filesystem::path abspath_out("testfile2");
    std::filesystem::remove(abspath_in);
    std::filesystem::remove(abspath_out);

    sys::write_file(abspath_in, "122333444455555666666");

    Plan plan;
    {
        size_t src_offset = 0;
        for (unsigned i = 1; i <= 6; ++i)
        {
            plan.add(src_offset, 21 - src_offset - i, i);
            src_offset += i;
        }
    }

    wassert(actual(plan.size()) == 6u);
    wassert(actual(plan[0]) == Span{ 0, 20, 1});
    wassert(actual(plan[1]) == Span{ 1, 18, 2});
    wassert(actual(plan[2]) == Span{ 3, 15, 3});
    wassert(actual(plan[3]) == Span{ 6, 11, 4});
    wassert(actual(plan[4]) == Span{10,  6, 5});
    wassert(actual(plan[5]) == Span{15,  0, 6});

    {
        core::File in(abspath_in, O_RDONLY);
        core::File out(abspath_out, O_WRONLY | O_CREAT | O_EXCL, 0644);
        plan.execute(in, out);
    }

    wassert(actual_file(abspath_out).contents_equal("666666555554444333221"));
});

}

}

