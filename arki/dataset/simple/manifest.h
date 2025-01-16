#ifndef ARKI_DATASET_SIMPLE_MANIFEST_H
#define ARKI_DATASET_SIMPLE_MANIFEST_H

#include <arki/core/fwd.h>
#include <arki/matcher/fwd.h>
#include <arki/core/time.h>
#include <arki/summary.h>
#include <vector>
#include <string>
#include <memory>
#include <filesystem>

namespace arki::dataset::simple {
struct Dataset;

namespace manifest {

/// Per-segment information contained in the manifest
struct SegmentInfo
{
    std::filesystem::path relpath;
    time_t mtime;
    core::Interval time;
};

/// Check if the given directory contains a manifest file
bool exists(const std::filesystem::path& root);

/**
 * Read a plaintext manifest file
 */
std::vector<SegmentInfo> read_plain(const std::filesystem::path& path);

/**
 * Read a legacy sqlite manifest file
 */
std::vector<SegmentInfo> read_sqlite(const std::filesystem::path& path);

/**
 * Use a MANIFEST file as index.
 *
 * Invariants:
 * * entried in the MANIFEST file are always sorted by relpath
 * * the MANIFEST file is always atomically rewritten (write new + rename) and never updated
 */
class Reader
{
protected:
    std::filesystem::path m_root;
    bool legacy_sql = false;

    std::vector<SegmentInfo> segmentinfo_cache;
    ino_t last_inode = 0;

public:
    explicit Reader(const std::filesystem::path& root);
    Reader(const Reader&) = delete;
    Reader(Reader&&) = delete;
    Reader& operator=(const Reader&) = delete;
    Reader& operator=(Reader&&) = delete;

    const std::filesystem::path root() const { return m_root; }

    /**
     * Read the MANIFEST file.
     *
     * @returns true if the MANIFEST file existed, false if not
     */
    bool reread();

    const SegmentInfo* segment(const std::filesystem::path& relpath) const;
    std::vector<SegmentInfo> file_list(const Matcher& matcher) const;
    const std::vector<SegmentInfo>& file_list() const;

    core::Interval get_stored_time_interval() const;
};

class Writer : public Reader
{
    bool eatmydata;
    bool dirty = false;

    void write(const SegmentInfo& info, core::NamedFileDescriptor& out) const;

public:
    Writer(const std::filesystem::path& root, bool eatmydata);

    bool is_dirty() const { return dirty; }

    void reread();

    void set(const std::filesystem::path& relpath, time_t mtime, const core::Interval& time);
    void remove(const std::filesystem::path& relpath);
    void flush();
    void rename(const std::filesystem::path& relpath, const std::filesystem::path& new_relpath);
};

}

}

#endif
