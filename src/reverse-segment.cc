#include "arki/core/lock.h"
#include "arki/metadata/collection.h"
#include "arki/runtime.h"
#include "arki/segment.h"
#include "arki/segment/scan.h"
#include "arki/segment/session.h"
#include <algorithm>
#include <filesystem>

int main(int argc, const char* argv[])
{
    arki::init();
    for (int i = 1; i < argc; ++i)
    {
        std::filesystem::path abspath(argv[i]);
        auto session = std::make_shared<arki::segment::scan::Session>(
            abspath.parent_path());
        auto segment = session->segment_from_relpath(abspath.filename());
        auto checker = segment->checker(
            std::make_shared<arki::core::lock::NullCheckLock>());
        auto mds = checker->scan();
        std::reverse(mds.begin(), mds.end());
        auto fixer = checker->fixer();
        fixer->reorder(mds, arki::segment::RepackConfig());
    }
    return 0;
}
