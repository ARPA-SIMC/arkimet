#include "fromfunction.h"
#include "arki/metadata.h"
#include <ostream>

using namespace std;

namespace arki {
namespace dataset {
namespace fromfunction {

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : dataset::Dataset(session, cfg)
{
}

std::unique_ptr<dataset::Reader> Dataset::create_reader() const { return std::unique_ptr<dataset::Reader>(new Reader(shared_from_this())); }


Reader::Reader(std::shared_ptr<const dataset::Dataset> config) : m_config(config) {}
Reader::~Reader() {}

bool Reader::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    return generator([&](std::shared_ptr<Metadata> md) {
        if (!q.matcher(*md))
            return true;
        return dest(md);
    });
}

}
}
}
