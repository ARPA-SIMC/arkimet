#ifndef ARKI_DATASET_MERGED_H
#define ARKI_DATASET_MERGED_H

/// dataset/merged - Access many datasets at the same time

#include <arki/dataset.h>
#include <string>
#include <vector>

namespace arki {
class ConfigFile;
class Metadata;
class Matcher;

namespace dataset {

/**
 * Access multiple datasets together
 */
class Merged : public Reader
{
protected:
	std::vector<Reader*> datasets;

public:
	Merged();
	virtual ~Merged();

	/// Add a dataset to the group of datasets to merge
	void addDataset(Reader& ds);

    void query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
    void query_bytes(const dataset::ByteQuery& q, int out) override;
};

/**
 * Same as Merged, but take care of instantiating and managing the child datasets
 */
class AutoMerged : public Merged
{
	void addDataset(Reader& ds);

public:
	AutoMerged();
	AutoMerged(const ConfigFile& cfg);
	virtual ~AutoMerged();
};

}
}

#endif
