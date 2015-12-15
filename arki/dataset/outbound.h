#ifndef ARKI_DATASET_OUTBOUND_H
#define ARKI_DATASET_OUTBOUND_H

/// Local, non queryable, on disk dataset

#include <arki/dataset/local.h>
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
class Outbound : public WritableSegmented
{
protected:
    void storeBlob(Metadata& md, const std::string& reldest);

public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	Outbound(const ConfigFile& cfg);

	virtual ~Outbound();

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

    virtual size_t repackFile(const std::string& relpath);
    virtual void rescanFile(const std::string& relpath);
    virtual size_t removeFile(const std::string& relpath, bool withData=false);
    virtual size_t vacuum();

	static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};

}
}

// vim:set ts=4 sw=4:
#endif
