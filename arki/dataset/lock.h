#ifndef ARKI_DATASET_LOCK_H
#define ARKI_DATASET_LOCK_H

#include <arki/core/lock.h>
#include <memory>

namespace arki {
namespace dataset {
namespace local {
class Dataset;
}

class DatasetReadLock : public core::lock::FileReadLock
{
public:
    explicit DatasetReadLock(const local::Dataset& dataset);
};

class DatasetAppendLock : public core::lock::FileAppendLock
{
public:
    explicit DatasetAppendLock(const local::Dataset& dataset);
};

class DatasetCheckLock : public core::lock::FileCheckLock
{
public:
    explicit DatasetCheckLock(const local::Dataset& dataset);
};

class SegmentReadLock : public core::lock::FileReadLock
{
public:
    SegmentReadLock(const local::Dataset& dataset, const std::filesystem::path& relpath);
};

class SegmentAppendLock : public core::lock::FileAppendLock
{
public:
    SegmentAppendLock(const local::Dataset& dataset, const std::filesystem::path& relpath);
};

class SegmentCheckLock : public core::lock::FileCheckLock
{
public:
    SegmentCheckLock(const local::Dataset& dataset, const std::filesystem::path& relpath);
};


}
}

#endif
