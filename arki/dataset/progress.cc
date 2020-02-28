#include "progress.h"
#include "arki/metadata.h"

using namespace std;

namespace arki {
namespace dataset {

QueryProgress::~QueryProgress()
{
}

void QueryProgress::start(size_t expected_count, size_t expected_bytes)
{
    this->expected_count = expected_count;
    this->expected_bytes = expected_bytes;
}

void QueryProgress::update(size_t count, size_t bytes)
{
    this->count += count;
    this->bytes += bytes;
}

void QueryProgress::done()
{
}

metadata_dest_func QueryProgress::wrap(metadata_dest_func dest)
{
    return [dest, this](std::shared_ptr<Metadata> md) {
        bool res = dest(md);
        update(1, md->data_size());
        return res;
    };
}

}
}
