#ifndef ARKI_METADATA_CLUSTERER_H
#define ARKI_METADATA_CLUSTERER_H

/// Process a stream of metadata in batches

#include <arki/metadata/consumer.h>
#include <arki/types/timerange.h>
#include <arki/types/reftime.h>

namespace arki {
namespace metadata {

class Clusterer
{
protected:
    /// Format of all the items in the current batch
    std::string format;
    /// Number of items in the current batch
    size_t count;
    /// Size of the current batch
    size_t size;
    /// Interval for the current batch
    int cur_interval[6];
    /// Timerange of all the items in the current batch, if split_timerange is true
    types::Timerange* last_timerange;
    /// Actual time span of the current batch
    std::unique_ptr<core::Time> timespan_begin;
    std::unique_ptr<core::Time> timespan_end;

    /**
     * Fill an int[6] with the datetime information in md, padding with -1 all
     * items not significant according to max_interval
     */
    void md_to_interval(const Metadata& md, int* interval) const;

    /**
     * Start a new cluster
     *
     * Child classes can hook here for processing the beginning of a batch
     */
    virtual void start_batch(const std::string& new_format);

    /**
     * Add an item to the current cluster.
     *
     * Child classes can hook here for processing md and buf as members of the
     * current batch.
     */
    virtual void add_to_batch(Metadata& md, const std::vector<uint8_t>& buf);

    /**
     * Reset information about the current batch, and start a new one.
     *
     * Subclassers will want to hook here to process the batch
     */
    virtual void flush_batch();

    /// Check if adding the given data would exceed the count limits for the current batch
    bool exceeds_count(const Metadata& md) const;

    /// Check if adding the given data would exceed the size limits for the current batch
    bool exceeds_size(const std::vector<uint8_t>& buf) const;

    /// Check if the time of the given data is inside the range acceptable for
    /// the current batch the current batch
    bool exceeds_interval(const Metadata& md) const;

    /// Check if the timerange of the given data is different than the one of
    /// the current batch
    bool exceeds_timerange(const Metadata& md) const;

public:
    /// Maximum number of data items per batch (0 to ignore)
    size_t max_count = 0;

    /// Maximum batch size in bytes, counting only data, not  metadata (0 to ignore)
    size_t max_bytes = 0;

    /**
     * Cluster by time interval (0 to ignore):
     *  1: a batch per year of data
     *  2: a batch per month of data
     *  3: a batch per day of data
     *  4: a batch per hour of data
     *  5: a batch per minute of data
     */
    size_t max_interval = 0;

    /// All items in a batch must have the same time range
    bool split_timerange = false;

    Clusterer();

    // Cannot flush here, since flush is virtual and we won't give subclassers
    // a chance to do their own flushing. Flushes must be explicit.
    ~Clusterer();

    bool eat(std::unique_ptr<Metadata>&& md);

    /**
     * Signal that no more data will be sent, and close the current partial
     * batch.
     */
    virtual void flush();

private:
    Clusterer(const Clusterer&);
    Clusterer& operator=(const Clusterer&);
};

}
}

#endif
