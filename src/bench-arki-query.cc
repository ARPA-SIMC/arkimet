#include "arki/runtime.h"
#include "arki/dataset.h"
#include "arki/dataset/session.h"
#include "arki/dataset/query.h"
#ifdef USE_GPERFTOOLS
#include <gperftools/profiler.h>
#endif

int main(int argc, const char* argv[])
{
    arki::init();
    auto session = std::make_shared<arki::dataset::Session>();
    auto match = session->matcher(argv[1]);
    auto cfg = arki::dataset::Session::read_config(argv[2]);
    auto ds = session->dataset(*cfg);
    auto reader = ds->create_reader();
    size_t count = 0;
    arki::dataset::DataQuery query(match);
#ifdef USE_GPERFTOOLS
    ProfilerStart("bench-arki-query.log");
#endif
    reader->query_data(query, [&](std::shared_ptr<arki::Metadata> md) { ++count; return true; });
#ifdef USE_GPERFTOOLS
    ProfilerStop();
#endif
    printf("%zd elements matched.\n", count);
    return 0;
}
