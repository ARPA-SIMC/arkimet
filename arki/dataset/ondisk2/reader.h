#ifndef ARKI_DATASET_ONDISK2_READER_H
#define ARKI_DATASET_ONDISK2_READER_H

#include <arki/dataset/local.h>
#include <string>
#include <map>
#include <vector>

namespace arki {
class ConfigFile;
class Metadata;
class Matcher;
class Summary;

namespace dataset {
class TargetFile;

namespace index {
class RContents;
}

namespace ondisk2 {

class Reader : public Local
{
protected:
    index::RContents* m_idx;
	TargetFile* m_tf;

    // Query only the data in the dataset, without the archives
    void queryLocalData(const dataset::DataQuery& q, metadata_dest_func dest);

public:
	// Initialise the dataset with the information from the configuration in 'cfg'
	Reader(const ConfigFile& cfg);
	virtual ~Reader();

    void query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void querySummary(const Matcher& matcher, Summary& summary) override;
    size_t produce_nth(metadata_dest_func cons, size_t idx=0) override;

	/**
	 * Return true if this dataset has a working index.
	 *
	 * This method is mostly used for tests.
	 */
	bool hasWorkingIndex() const { return m_idx != 0; }
};

}
}
}
#endif
