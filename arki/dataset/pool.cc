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

Datasets::Datasets(std::shared_ptr<Session> session, const core::cfg::Sections& cfg)
    : session(session)
{
    for (const auto& si: cfg)
        session->add_dataset(si.second);
}

std::shared_ptr<dataset::Dataset> Datasets::get(const std::string& name) const
{
    return session->dataset(name);
}

bool Datasets::has(const std::string& name) const
{
    return session->has_dataset(name);
}

std::shared_ptr<dataset::Dataset> Datasets::locate_metadata(Metadata& md)
{
    return session->locate_metadata(md);
}


WriterPool::WriterPool(const Datasets& datasets)
    : datasets(datasets)
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
        auto config = datasets.get(name);
        auto writer(config->create_writer());
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
    if (datasets.has("duplicates"))
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
