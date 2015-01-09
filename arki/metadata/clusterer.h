#ifndef ARKI_METADATA_CLUSTERER_H
#define ARKI_METADATA_CLUSTERER_H

/*
 * metadata/clusterer - Process a stream of metadata in batches
 *
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/metadata/consumer.h>
#include <arki/types/timerange.h>
#include <arki/types/reftime.h>
#include <wibble/sys/buffer.h>

namespace arki {
namespace metadata {

class Clusterer : public metadata::Eater
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
    types::reftime::Collector timespan;

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
    virtual void add_to_batch(Metadata& md, const wibble::sys::Buffer& buf);

    /**
     * Reset information about the current batch, and start a new one.
     *
     * Subclassers will want to hook here to process the batch
     */
    virtual void flush_batch();

    /// Check if adding the given data would exceed the count limits for the current batch
    bool exceeds_count(const Metadata& md) const;

    /// Check if adding the given data would exceed the size limits for the current batch
    bool exceeds_size(const wibble::sys::Buffer& buf) const;

    /// Check if the time of the given data is inside the range acceptable for
    /// the current batch the current batch
    bool exceeds_interval(const Metadata& md) const;

    /// Check if the timerange of the given data is different than the one of
    /// the current batch
    bool exceeds_timerange(const Metadata& md) const;

public:
    /// Maximum number of data items per batch
    size_t max_count;
    /// Maximum batch size in bytes, counting only data, not metadata
    size_t max_bytes;
    /**
     * Cluster by time interval:
     *  1: a batch per year of data
     *  2: a batch per month of data
     *  3: a batch per day of data
     *  4: a batch per hour of data
     *  5: a batch per minute of data
     */
    size_t max_interval;
    /// All items in a batch must have the same time range
    bool split_timerange;

    Clusterer();

    // Cannot flush here, since flush is virtual and we won't give subclassers
    // a chance to do their own flushing. Flushes must be explicit.
    ~Clusterer();

    virtual bool eat(std::auto_ptr<Metadata> md) override;

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
