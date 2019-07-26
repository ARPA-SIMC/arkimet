#ifndef ARKI_METADATA_DATA_H
#define ARKI_METADATA_DATA_H

#include <arki/core/file.h>
#include <vector>
#include <list>
#include <memory>

namespace arki {
class Emitter;

namespace metadata {

/**
 * Interface for representing data associated with a Metadata
 */
struct Data
{
    Data() {}
    virtual ~Data() {}

    // Copying and moving are disallowed
    Data(const Data&) = delete;
    Data(Data&&) = delete;
    Data& operator=(const Data&) = delete;
    Data& operator=(Data&&) = delete;

    /// Return the data size
    virtual size_t size() const = 0;

    /// Return the data contents as a memory buffer
    virtual std::vector<uint8_t> read() const = 0;

    /**
     * Write the data to a NamedFileDescriptor
     *
     * It returns the number of bytes successfully written. In case of any
     * error or partial write, an exception is raised.
     */
    virtual size_t write(core::NamedFileDescriptor& fd) const = 0;

    /**
     * Write the data to a NamedFileDescriptor
     *
     * It returns the number of bytes successfully written. In case of any
     * error or partial write, an exception is raised.
     */
    virtual size_t write(core::AbstractOutputFile& fd) const = 0;

    /**
     * Write the data to a NamedFileDescriptor, without leading or trailing
     * elements, to be contained in some kind of envelope like inline Metadata.
     *
     * It returns the number of bytes successfully written. In case of any
     * error or partial write, an exception is raised.
     */
    virtual size_t write_inline(core::NamedFileDescriptor& fd) const = 0;

    /**
     * Write the data to a NamedFileDescriptor, without leading or trailing
     * elements, to be contained in some kind of envelope like inline Metadata.
     *
     * It returns the number of bytes successfully written. In case of any
     * error or partial write, an exception is raised.
     */
    virtual size_t write_inline(core::AbstractOutputFile& fd) const = 0;

    /// Send data to an emitter
    virtual void emit(Emitter& e) const = 0;
};


struct DataManager;

/**
 * Track all data elements that get cached in metadata during the lifetime of
 * this object.
 */
class TrackedData
{
protected:
    DataManager& manager;

public:
    std::vector<std::weak_ptr<Data>> tracked;

    TrackedData(DataManager& manager);
    TrackedData(const TrackedData&) = delete;
    TrackedData(TrackedData&&) = delete;
    ~TrackedData();
    TrackedData& operator=(const TrackedData&) = delete;
    TrackedData& operator=(TrackedData&&) = delete;

    unsigned count_used() const;
};


/**
 * Track data cached in memory
 */
class DataManager
{
protected:
    std::list<TrackedData*> trackers;
    bool tracking = false;

    void start_tracking(TrackedData* tracker);
    void stop_tracking(TrackedData* tracker);

public:
    /// Create a Data from a buffer
    std::shared_ptr<Data> to_data(const std::string& format, std::vector<uint8_t>&& data);

    /// Create a Data that throws an error when trying to read it (used for tests)
    std::shared_ptr<Data> to_unreadable_data(size_t size);

    static DataManager& get();

    friend class TrackedData;
};


}
}

#endif
