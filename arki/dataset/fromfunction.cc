#include "fromfunction.h"
#include "arki/metadata.h"
#include "arki/dataset/query.h"
#include <ostream>

using namespace std;

namespace arki {
namespace dataset {
namespace fromfunction {

std::shared_ptr<dataset::Reader> Dataset::create_reader() { return std::make_shared<Reader>(shared_from_this()); }


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
