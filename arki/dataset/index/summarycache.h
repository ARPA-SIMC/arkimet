#ifndef ARKI_DATASET_INDEX_SUMMARYCACHE_H
#define ARKI_DATASET_INDEX_SUMMARYCACHE_H

/// dataset/index/summarycache - Cache precomputed summaries

#include <arki/core/time.h>
#include <arki/metadata/fwd.h>
#include <arki/dataset/fwd.h>
#include <filesystem>

namespace arki {
class Summary;

namespace dataset {
class Base;

namespace index {

class SummaryCache
{
protected:
    /// Absolute path to the summary cache directory
    std::filesystem::path m_scache_root;

    /// Return the pathname for the summary file for a given year and month
    std::filesystem::path summary_pathname(int year, int month) const;

public:
    SummaryCache(const std::filesystem::path& root);
    ~SummaryCache();

    void openRO();
    void openRW();

    /// Read summary for all the period. Returns true if found, false if not in cache.
    bool read(Summary& s);

    /// Read summary for the given month. Returns true if found, false if not in cache
    bool read(Summary& s, int year, int month);

    /// Write the global cache file; returns false if the cache dir is not writable.
    bool write(Summary& s);

    /// Write the cache file for the given month; returns false if the cache dir is not writable.
    bool write(Summary& s, int year, int month);

    /// Remove all contents of the cache
    void invalidate();

    /// Remove cache contents for the given month
    void invalidate(int year, int month);

    /// Remove cache contents for the period found in the given metadata
    void invalidate(const Metadata& md);

    /**
     * Remove cache contents for the period found in successfully imported
     * metadata in the batch
     */
    void invalidate(const WriterBatch& batch);

    /// Remove cache contents for all dates from tmin and tmax (inclusive)
    void invalidate(const core::Time& tmin, const core::Time& tmax);

    /// Run consistency checks on the summary cache
    bool check(const dataset::Base& ds, Reporter& reporter) const;
};

}
}
}

#endif
