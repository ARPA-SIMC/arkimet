#include "indexed.h"
#include "index.h"
#include "maintenance.h"
#include "reporter.h"
#include "session.h"
#include "arki/dataset/time.h"
#include "arki/dataset/lock.h"
#include "arki/dataset/query.h"
#include "arki/dataset/progress.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include "arki/core/time.h"
#include "arki/metadata/collection.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/utils/files.h"
#include "arki/scan.h"
#include "arki/nag.h"
#include <algorithm>
#include <cstring>

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace indexed {

Reader::~Reader()
{
    delete m_idx;
}

bool Reader::impl_query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    dataset::TrackProgress track(q.progress);
    dest = track.wrap(dest);

    auto lock = dataset().read_lock_dataset();
    if (!local::Reader::impl_query_data(q, dest))
        return false;
    if (!m_idx) return true;
    m_idx->lock = lock;
    return track.done(m_idx->query_data(q, dest));
}

void Reader::impl_query_summary(const Matcher& matcher, Summary& summary)
{
    auto lock = dataset().read_lock_dataset();
    // Query the archives first
    local::Reader::impl_query_summary(matcher, summary);
    if (!m_idx) return;
    m_idx->lock = lock;
    // FIXME: this is cargo culted from the old ondisk2 reader: what is the use case for this?
    if (!m_idx->query_summary(matcher, summary))
        throw std::runtime_error("cannot query " + dataset().path + ": index could not be used");
}


Checker::~Checker()
{
    delete m_idx;
}

// TODO: during checks, run file:///usr/share/doc/sqlite3-doc/pragma.html#debug
// and delete the index if it fails

void Checker::check_issue51(CheckerConfig& opts)
{
    if (!opts.online && !dataset().offline) return;
    if (!opts.offline && dataset().offline) return;

    // Broken metadata for each segment
    std::map<string, metadata::Collection> broken_mds;

    // Iterate all segments
    m_idx->list_segments([&](const std::string& relpath) {
        metadata::Collection mds;
        m_idx->query_segment(relpath, mds.inserter_func());
        if (mds.empty()) return;
        File datafile(str::joinpath(dataset().path, relpath), O_RDONLY);
        // Iterate all metadata in the segment
        unsigned count_otherformat = 0;
        unsigned count_ok = 0;
        unsigned count_issue51 = 0;
        unsigned count_corrupted = 0;
        for (const auto& md: mds) {
            const auto& blob = md->sourceBlob();
            // Keep only segments with grib or bufr files
            if (blob.format != "grib" && blob.format != "bufr")
            {
                ++count_otherformat;
                continue;
            }
            // Read the last 4 characters
            char tail[4];
            if (datafile.pread(tail, 4, blob.offset + blob.size - 4) != 4)
            {
                opts.reporter->segment_info(name(), relpath, "cannot read 4 bytes at position " + std::to_string(blob.offset + blob.size - 4));
                return;
            }
            // Check if it ends with 7777
            if (memcmp(tail, "7777", 4) == 0)
            {
                ++count_ok;
                continue;
            }
            // If it instead ends with 777?, take note of it
            if (memcmp(tail, "777", 3) == 0)
            {
                ++count_issue51;
                broken_mds[relpath].push_back(*md);
            } else {
                ++count_corrupted;
                opts.reporter->segment_info(name(), relpath, "end marker 7777 or 777? not found at position " + std::to_string(blob.offset + blob.size - 4));
            }
        }
        nag::verbose("Checked %s:%s: %u ok, %u other formats, %u issue51, %u corrupted", name().c_str(), relpath.c_str(), count_ok, count_otherformat, count_issue51, count_corrupted);
    });

    if (opts.readonly)
    {
        for (const auto& i: broken_mds)
            opts.reporter->segment_issue51(name(), i.first, "segment contains data with corrupted terminator signature");
    } else {
        for (const auto& i: broken_mds)
        {
            // Make a backup copy with .issue51 extension, if it doesn't already exist
            std::string abspath = str::joinpath(dataset().path, i.first);
            utils::files::PreserveFileTimes pf(abspath);
            std::string backup = abspath + ".issue51";
            if (!sys::exists(backup))
            {
                File src(abspath, O_RDONLY);
                File dst(backup, O_WRONLY | O_CREAT | O_EXCL, 0666);
                std::array<char, 40960> buffer;
                while (true)
                {
                    size_t sz = src.read(buffer.data(), buffer.size());
                    if (!sz) break;
                    dst.write_all_or_throw(buffer.data(), sz);
                }
            }

            // Fix the file
            File datafile(abspath, O_RDWR);
            for (const auto& md: i.second) {
                const auto& blob = md->sourceBlob();
                // Keep only segments with grib or bufr files
                if (blob.format != "grib" && blob.format != "bufr")
                    return;
                datafile.pwrite("7", 1, blob.offset + blob.size - 1);
            }
            opts.reporter->segment_issue51(name(), i.first, "fixed corrupted terminator signatures");
        }
    }

    return segmented::Checker::check_issue51(opts);
}

void Checker::test_make_overlap(const std::string& relpath, unsigned overlap_size, unsigned data_idx)
{
    metadata::Collection mds;
    m_idx->query_segment(relpath, mds.inserter_func());
    dataset().session.lock()->segment_checker(scan::Scanner::format_from_filename(relpath), dataset().path, relpath)->test_make_overlap(mds, overlap_size, data_idx);
    m_idx->test_make_overlap(relpath, overlap_size, data_idx);
}

void Checker::test_make_hole(const std::string& relpath, unsigned hole_size, unsigned data_idx)
{
    metadata::Collection mds;
    m_idx->query_segment(relpath, mds.inserter_func());
    dataset().session.lock()->segment_checker(scan::Scanner::format_from_filename(relpath), dataset().path, relpath)->test_make_hole(mds, hole_size, data_idx);
    m_idx->test_make_hole(relpath, hole_size, data_idx);
}

void Checker::test_corrupt_data(const std::string& relpath, unsigned data_idx)
{
    metadata::Collection mds;
    m_idx->query_segment(relpath, mds.inserter_func());
    dataset().session.lock()->segment_checker(scan::Scanner::format_from_filename(relpath), dataset().path, relpath)->test_corrupt(mds, data_idx);
}

void Checker::test_truncate_data(const std::string& relpath, unsigned data_idx)
{
    metadata::Collection mds;
    m_idx->query_segment(relpath, mds.inserter_func());
    dataset().session.lock()->segment_checker(scan::Scanner::format_from_filename(relpath), dataset().path, relpath)->test_truncate(mds, data_idx);
}

void Checker::test_swap_data(const std::string& relpath, unsigned d1_idx, unsigned d2_idx)
{
    metadata::Collection mds;
    m_idx->query_segment(relpath, mds.inserter_func());
    mds.swap(d1_idx, d2_idx);

    segment(relpath)->reorder(mds);
}

void Checker::test_rename(const std::string& relpath, const std::string& new_relpath)
{
    auto segment = dataset().session.lock()->segment_checker(scan::Scanner::format_from_filename(relpath), dataset().path, relpath);
    m_idx->test_rename(relpath, new_relpath);
    segment->move(dataset().path, new_relpath, str::joinpath(dataset().path, new_relpath));
}

}
}
}
