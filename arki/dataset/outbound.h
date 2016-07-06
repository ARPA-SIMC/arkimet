#ifndef ARKI_DATASET_OUTBOUND_H
#define ARKI_DATASET_OUTBOUND_H

/// Local, non queryable, on disk dataset

#include <arki/dataset/segmented.h>
#include <string>

namespace arki {
class ConfigFile;
class Metadata;
class Matcher;

namespace dataset {

/**
 * Store-only dataset.
 *
 * This dataset is not used for archival, but only to store data as an outbound
 * area.
 */
class Outbound : public SegmentedWriter
{
protected:
    std::shared_ptr<const SegmentedConfig> m_config;

    void storeBlob(Metadata& md, const std::string& reldest);

public:
    // Initialise the dataset with the information from the configurationa in 'cfg'
    Outbound(std::shared_ptr<const SegmentedConfig> config);
    virtual ~Outbound();

    const SegmentedConfig& config() const override { return *m_config; }

    std::string type() const override;

    /**
     * Acquire the given metadata item (and related data) in this dataset.
     *
     * @return true if the data is successfully stored in the dataset, else
     * false.  If false is returned, a note is added to the dataset explaining
     * the reason of the failure.
     */
    virtual AcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT);

    virtual void remove(Metadata& id);
    virtual void removeAll(std::ostream& log, bool writable=false);

    static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};

}
}
#endif
