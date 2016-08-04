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
namespace outbound {

struct Config : public segmented::Config
{
    Config(const ConfigFile& cfg);

    std::unique_ptr<dataset::Reader> create_reader() const override;
    std::unique_ptr<dataset::Writer> create_writer() const override;

    static std::shared_ptr<const Config> create(const ConfigFile& cfg);
};

/**
 * Store-only dataset.
 *
 * This dataset is not used for archival, but only to store data as an outbound
 * area.
 */
class Writer : public segmented::Writer
{
protected:
    std::shared_ptr<const segmented::Config> m_config;

    void storeBlob(Metadata& md, const std::string& reldest);

public:
    // Initialise the dataset with the information from the configurationa in 'cfg'
    Writer(std::shared_ptr<const segmented::Config> config);
    virtual ~Writer();

    const segmented::Config& config() const override { return *m_config; }

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
}
#endif
