#include "data.h"
#include "arki/emitter.h"
#include "arki/exceptions.h"
#include <sys/uio.h>


namespace arki {
namespace metadata {

struct DataUnreadable : public Data
{
    size_t m_size;

    DataUnreadable(size_t size) : m_size(size) {}

    size_t size() const override { return m_size; }
    std::vector<uint8_t> read() const
    {
        throw std::runtime_error("DataUnreadable::read() called");
    }
    size_t write(core::NamedFileDescriptor& fd) const override
    {
        throw std::runtime_error("DataUnreadable::write() called");
    }
    size_t write_inline(core::NamedFileDescriptor& fd) const override
    {
        throw std::runtime_error("DataUnreadable::write_inline() called");
    }
    void emit(Emitter& e) const override
    {
        throw std::runtime_error("DataUnreadable::emit() called");
    }
};


struct DataBuffer : public Data
{
    std::vector<uint8_t> buffer;

    DataBuffer(std::vector<uint8_t>&& buffer) : buffer(std::move(buffer)) {}

    size_t size() const override { return buffer.size(); }
    std::vector<uint8_t> read() const
    {
        return buffer;
    }
    size_t write(core::NamedFileDescriptor& fd) const override
    {
        fd.write_all_or_retry(buffer.data(), buffer.size());
        return buffer.size();
    }
    size_t write_inline(core::NamedFileDescriptor& fd) const override
    {
        fd.write_all_or_retry(buffer.data(), buffer.size());
        return buffer.size();
    }
    void emit(Emitter& e) const override
    {
        e.add_raw(buffer);
    }
};


struct DataLineBuffer : public DataBuffer
{
    using DataBuffer::DataBuffer;

    size_t write(core::NamedFileDescriptor& fd) const override
    {
        struct iovec todo[2] = {
            { (void*)buffer.data(), buffer.size() },
            { (void*)"\n", 1 },
        };
        ssize_t res = ::writev(fd, todo, 2);
        if (res < 0 || (unsigned)res != buffer.size() + 1)
            throw_system_error("cannot write " + std::to_string(buffer.size() + 1) + " bytes to " + fd.name());
        return buffer.size() + 1;
    }
    size_t write_inline(core::NamedFileDescriptor& fd) const override
    {
        fd.write_all_or_retry(buffer.data(), buffer.size());
        return buffer.size();
    }
};


TrackedData::TrackedData(DataManager& manager)
    : manager(manager)
{
    manager.start_tracking(this);
}

TrackedData::~TrackedData()
{
    manager.stop_tracking(this);
}

unsigned TrackedData::count_used() const
{
    unsigned res = 0;
    for (const auto& t: tracked)
        if (!t.expired())
            ++res;
    return res;
}


void DataManager::start_tracking(TrackedData* tracker)
{
    trackers.push_back(tracker);
}

void DataManager::stop_tracking(TrackedData* tracker)
{
    trackers.remove(tracker);
}

std::shared_ptr<Data> DataManager::to_data(const std::string& format, std::vector<uint8_t>&& data)
{
    std::shared_ptr<Data> res;

    if (format == "vm2")
        res = std::make_shared<DataLineBuffer>(std::move(data));
    else
        res = std::make_shared<DataBuffer>(std::move(data));

    for (auto& tracker: trackers)
        tracker->tracked.emplace_back(res);

    return res;
}

std::shared_ptr<Data> DataManager::to_unreadable_data(size_t size)
{
    std::shared_ptr<Data> res = std::make_shared<DataUnreadable>(size);

    for (auto& tracker: trackers)
        tracker->tracked.emplace_back(res);

    return res;
}

static DataManager data_tracker;

DataManager& DataManager::get()
{
    return data_tracker;
}


}
}
