#include "pool.h"
#include "arki/utils/string.h"
#include "arki/core/cfg.h"
#include "arki/dataset.h"
#include "arki/dataset/local.h"
#include "arki/dataset/session.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

WriterPool::WriterPool(std::shared_ptr<Session> session)
    : session(session)
{
}

WriterPool::~WriterPool()
{
}

std::shared_ptr<dataset::Writer> WriterPool::get(const std::string& name)
{
    auto ci = cache.find(name);
    if (ci == cache.end())
    {
        auto ds = session->dataset(name);
        auto writer(ds->create_writer());
        cache.insert(make_pair(name, writer));
        return writer;
    } else {
        return ci->second;
    }
}

std::shared_ptr<dataset::Writer> WriterPool::get_error()
{
    return get("error");
}

std::shared_ptr<dataset::Writer> WriterPool::get_duplicates()
{
    if (session->has_dataset("duplicates"))
        return get("duplicates");
    else
        return get_error();
}


void WriterPool::flush()
{
    for (auto& i: cache)
        i.second->flush();
}

}
}
