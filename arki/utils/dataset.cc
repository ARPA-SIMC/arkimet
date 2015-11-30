#include "dataset.h"
#include "arki/dataset/data.h"
#include "arki/types/source/blob.h"

using namespace std;
using namespace arki::types;

namespace arki {
namespace utils {
namespace ds {

bool DataOnly::eat(unique_ptr<Metadata>&& md)
{
    if (!writer)
        writer = dataset::data::OstreamWriter::get(md->source().format);
    writer->stream(*md, out);
    return true;
}

bool MakeAbsolute::eat(unique_ptr<Metadata>&& md)
{
    if (md->has_source())
        if (const source::Blob* blob = dynamic_cast<const source::Blob*>(&(md->source())))
            md->set_source(upcast<Source>(blob->makeAbsolute()));
    return next.eat(move(md));
}

}
}
}
