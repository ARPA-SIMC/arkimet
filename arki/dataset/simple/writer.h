#ifndef ARKI_DATASET_SIMPLE_WRITER_H
#define ARKI_DATASET_SIMPLE_WRITER_H

/// dataset/simple/writer - Writer for simple datasets with no duplicate checks

#include <arki/dataset/local.h>
#include <arki/configfile.h>

#include <string>
#include <iosfwd>

namespace arki {
class Matcher;

namespace dataset {
class TargetFile;

namespace maintenance {
class MaintFileVisitor;
}

namespace index {
class Manifest;
}

namespace simple {
class Reader;
class Datafile;

class Writer : public SegmentedWriter
{
protected:
    index::Manifest* m_mft;

    /// Return a (shared) instance of the Datafile for the given relative pathname
    data::Segment* file(const Metadata& md, const std::string& format);

public:
	Writer(const ConfigFile& cfg);
	virtual ~Writer();

	/**
	 * Acquire the given metadata item (and related data) in this dataset.
	 *
	 * After acquiring the data successfully, the data can be retrieved from
	 * the dataset.  Also, information such as the dataset name and the id of
	 * the data in the dataset are added to the Metadata object.
	 *
	 * @return true if the data is successfully stored in the dataset, else
	 * false.  If false is returned, a note is added to the dataset explaining
	 * the reason of the failure.
	 */
	virtual AcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT);

	/**
	 * This dataset stores duplicates, so it cannot unambiguously locale
	 * single data items. This functions does nothing.
	 */
	void remove(Metadata& id);

	virtual void flush();

	virtual void maintenance(maintenance::MaintFileVisitor& v, bool quick=true);
	virtual void removeAll(std::ostream& log, bool writable);

	virtual void rescanFile(const std::string& relpath);
	virtual size_t repackFile(const std::string& relpath);
	virtual void archiveFile(const std::string& relpath);
	virtual size_t removeFile(const std::string& relpath, bool withData=false);
	virtual size_t vacuum();

    virtual Pending test_writelock();

	static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};

}
}
}
#endif
