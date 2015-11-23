#ifndef ARKI_DATASET_INDEX_MANIFEST_H
#define ARKI_DATASET_INDEX_MANIFEST_H

/*
 * dataset/index/manifest - Index files with no duplicate checks
 *
 * Copyright (C) 2009--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/dataset.h>
#include <vector>
#include <string>
#include <memory>

namespace arki {
class Matcher;
class Summary;

namespace dataset {

namespace data {
class SegmentManager;
}

namespace maintenance {
class MaintFileVisitor;
}

namespace index {

class Manifest : public ReadonlyDataset
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
    /**
     * Set start_time and end_time to the reftime span of the given file.
     *
     * If the file does not exist in the manifest, return false
     */
    virtual bool fileTimespan(const std::string& relname, types::Time& start_time, types::Time& end_time) const = 0;
	virtual void vacuum() = 0;
	virtual void acquire(const std::string& relname, time_t mtime, const Summary& sum) = 0;
	virtual void remove(const std::string& relname) = 0;
	virtual void check(data::SegmentManager& sm, maintenance::MaintFileVisitor& v, bool quick=true) = 0;
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

    void queryData(const dataset::DataQuery& q, metadata::Eater& consumer) override;
    void querySummary(const Matcher& matcher, Summary& summary) override;
    virtual size_t produce_nth(metadata::Eater& cons, size_t idx=0);

	void rescanFile(const std::string& dir, const std::string& relpath);

	static bool exists(const std::string& dir);
    static std::unique_ptr<Manifest> create(const std::string& dir, const ConfigFile* cfg=0);

	static bool get_force_sqlite();
	static void set_force_sqlite(bool val);
};

}
}
}

#endif
