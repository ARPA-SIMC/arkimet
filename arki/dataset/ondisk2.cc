#include "ondisk2.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace ondisk2 {

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : dataset::Dataset(session, cfg)
{
}

std::shared_ptr<dataset::Reader> Dataset::create_reader()
{
    throw std::runtime_error("ondisk2 datasets are not supported anymore. Please convert the dataset to type=iseg");
}

std::shared_ptr<dataset::Writer> Dataset::create_writer()
{
    throw std::runtime_error("ondisk2 datasets are not supported anymore. Please convert the dataset to type=iseg");
}

std::shared_ptr<dataset::Checker> Dataset::create_checker()
{
    throw std::runtime_error("ondisk2 datasets are not supported anymore. Please convert the dataset to type=iseg");
}

}
}
}
