#include "progress.h"
#include "arki/metadata.h"

namespace arki {
namespace query {

Progress::~Progress()
{
}

void Progress::start(size_t expected_count, size_t expected_bytes)
{
    this->expected_count = expected_count;
    this->expected_bytes = expected_bytes;
}

void Progress::update(size_t count, size_t bytes)
{
    this->count += count;
    this->bytes += bytes;
}

void Progress::done()
{
}

metadata_dest_func Progress::wrap(metadata_dest_func dest)
{
    return [dest, this](std::shared_ptr<Metadata> md) {
        bool res = dest(md);
        update(1, md->data_size());
        return res;
    };
}

}
}
