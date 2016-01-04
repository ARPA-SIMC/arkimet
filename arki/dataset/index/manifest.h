#ifndef ARKI_DATASET_INDEX_MANIFEST_H
#define ARKI_DATASET_INDEX_MANIFEST_H

#include <arki/dataset/index.h>
#include <arki/dataset/segment.h>
#include <vector>
#include <string>
#include <memory>

namespace arki {
class Matcher;
class Summary;

namespace dataset {
namespace index {

class Manifest : public dataset::Index
{
protected:
	std::string m_path;
	void querySummaries(const Matcher& matcher, Summary& summary);

public:
	Manifest(const ConfigFile& cfg);
	Manifest(const std::string& path);
	virtual ~Manifest();

	virtual void openRO() = 0;
	virtual void openRW() = 0;
	virtual void fileList(const Matcher& matcher, std::vector<std::string>& files) = 0;
    bool segment_timespan(const std::string& relname, types::Time& start_time, types::Time& end_time) const override = 0;
    virtual size_t vacuum() = 0;
    virtual void acquire(const std::string& relname, time_t mtime, const Summary& sum) = 0;
    virtual void remove(const std::string& relname) = 0;
    virtual void flush() = 0;

    virtual Pending test_writelock() = 0;

    /// Invalidate global summary
    void invalidate_summary();
    /// Invalidate summary for file \a relname and global summary
    void invalidate_summary(const std::string& relname);

    /**
     * Expand the given begin and end ranges to include the datetime extremes
     * of this manifest.
     *
     * If begin and end are unset, set them to the datetime extremes of this
     * manifest.
     */
    virtual void expand_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end) const = 0;

    bool query_data(const dataset::DataQuery& q, metadata_dest_func) override;
    bool query_summary(const Matcher& matcher, Summary& summary) override;
    void list_segments(std::function<void(const std::string&)> dest) override = 0;
    void scan_files(segment::contents_func v) override = 0;

    void rescanSegment(const std::string& dir, const std::string& relpath);

	static bool exists(const std::string& dir);
    static std::unique_ptr<Manifest> create(const std::string& dir, const ConfigFile* cfg=0);

	static bool get_force_sqlite();
	static void set_force_sqlite(bool val);
};

}
}
}

#endif
