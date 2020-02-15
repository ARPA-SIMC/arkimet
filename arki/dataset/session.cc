#include "session.h"

namespace arki {
namespace dataset {


Session::~Session()
{
}


std::shared_ptr<segment::Reader> Session::reader_from_pool(const std::string& abspath)
{
    auto res = reader_pool.find(abspath);
    if (res == reader_pool.end())
        return std::shared_ptr<segment::Reader>();
    if (res->second.expired())
    {
        reader_pool.erase(res);
        return std::shared_ptr<segment::Reader>();
    }
    return res->second.lock();
}


}
}
