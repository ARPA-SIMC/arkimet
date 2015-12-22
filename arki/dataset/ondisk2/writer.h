#ifndef ARKI_DATASET_ONDISK2_WRITER_H
#define ARKI_DATASET_ONDISK2_WRITER_H

/// dataset/ondisk2/writer - Local on disk dataset writer

#include <arki/dataset/indexed.h>
#include <arki/configfile.h>
#include <arki/dataset/index/contents.h>
#include <string>
#include <memory>

namespace arki {
class Metadata;
class Matcher;
class Summary;

namespace dataset {
namespace ondisk2 {

namespace writer {
class RealRepacker;
class RealFixer;
}

class Writer : public IndexedWriter
{
protected:
	ConfigFile m_cfg;
    index::WContents* idx;

    AcquireResult acquire_replace_never(Metadata& md);
    AcquireResult acquire_replace_always(Metadata& md);
    AcquireResult acquire_replace_higher_usn(Metadata& md);

public:
    // Initialise the dataset with the information from the configurationa in 'cfg'
    Writer(const ConfigFile& cfg);
    virtual ~Writer();

    AcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void remove(Metadata& md) override;
    void flush() override;
    virtual Pending test_writelock();

    void maintenance(segment::state_func v, bool quick=true) override;
    void sanityChecks(std::ostream& log, bool writable=false) override;
    void removeAll(std::ostream& log, bool writable) override;

    void rescanFile(const std::string& relpath) override;
    size_t repackFile(const std::string& relpath) override;
    size_t removeFile(const std::string& relpath, bool withData=false) override;
    void archiveFile(const std::string& relpath) override;
    size_t vacuum() override;

	/**
	 * Iterate through the contents of the dataset, in depth-first order.
	 */
	//void depthFirstVisit(Visitor& v);

	static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);

	friend class writer::RealRepacker;
	friend class writer::RealFixer;
};

/**
 * Temporarily override the current date used to check data age.
 *
 * This is used to be able to write unit tests that run the same independently
 * of when they are run.
 */
struct TestOverrideCurrentDateForMaintenance
{
    time_t old_ts;

    TestOverrideCurrentDateForMaintenance(time_t ts);
    ~TestOverrideCurrentDateForMaintenance();
};

}
}
}
#endif
