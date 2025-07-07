#include "arki/core/file.h"
#include "arki/utils/sys.h"
#include "stream.h"
#include "stream/tests.h"
#include <sys/ioctl.h>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
};

Tests test("arki_stream");

void Tests::register_tests()
{

    add_method("empty", []() noexcept {});
}

} // namespace
