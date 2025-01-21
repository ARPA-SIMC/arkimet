#include "arki/runtime.h"
#include "arki/dataset.h"
#include "arki/dataset/session.h"
#include "arki/query.h"
#include "arki/stream.h"
#include "arki/utils/sys.h"
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
    arki::query::Bytes query;
    query.setData(match);
    auto outfd = std::make_shared<arki::core::NamedFileDescriptor>(1, "stdout");
    auto output = arki::StreamOutput::create(outfd);
#ifdef USE_GPERFTOOLS
    ProfilerStart("bench-arki-query-data.log");
#endif
    reader->query_bytes(query, *output);
#ifdef USE_GPERFTOOLS
    ProfilerStop();
#endif
    printf("%zd elements matched.\n", count);
    return 0;
}
