#ifndef ARKI_DATASET_DISCARD_H
#define ARKI_DATASET_DISCARD_H

/// Dataset that just discards all data

#include <arki/dataset.h>
#include <string>

namespace arki {
namespace dataset {

/**
 * Store-only dataset.
 *
 * This dataset is not used for archival, but only to store data as an outbound
 * area.
 */
class Discard : public Writer
{
public:
    using Writer::Writer;

    AcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT) override;

    void remove(Metadata&) override
    {
        // Of course, after this method is called, the metadata cannot be found
        // in the dataset
    }

    static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};

}
}
#endif
