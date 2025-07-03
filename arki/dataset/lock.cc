#include "lock.h"
#include "local.h"
#include "arki/utils/string.h"
#include <memory>

//#define TRACE_LOCKS
#ifdef TRACE_LOCKS
#define trace(...) fprintf(stderr, __VA_ARGS__)
#else
#define trace(...) do {} while(0)
#endif

using namespace arki::utils;

namespace arki {
namespace dataset {


namespace {
const std::filesystem::path& ensure_path(const std::filesystem::path& pathname)
{
    std::filesystem::create_directories(pathname.parent_path());
    return pathname;
}
}


DatasetReadLock::DatasetReadLock(const local::Dataset& dataset)
    : core::lock::FileReadLock(dataset.path / "lock", dataset.lock_policy)
{
}

SegmentReadLock::SegmentReadLock(const local::Dataset& dataset, const std::filesystem::path& relpath)
    : core::lock::FileReadLock(dataset.path / sys::with_suffix(relpath, ".lock"), dataset.lock_policy)
{
}

DatasetAppendLock::DatasetAppendLock(const local::Dataset& dataset)
    : core::lock::FileAppendLock(dataset.path / "lock", dataset.lock_policy)
{
}

SegmentAppendLock::SegmentAppendLock(const local::Dataset& dataset, const std::filesystem::path& relpath)
    : core::lock::FileAppendLock(dataset.path / sys::with_suffix(relpath, ".lock"), dataset.lock_policy)
{
}

DatasetCheckLock::DatasetCheckLock(const local::Dataset& dataset)
    : core::lock::FileCheckLock(dataset.path / "lock", dataset.lock_policy)
{
}

SegmentCheckLock::SegmentCheckLock(const local::Dataset& dataset, const std::filesystem::path& relpath)
    : core::lock::FileCheckLock(ensure_path(dataset.path / sys::with_suffix(relpath, ".lock")), dataset.lock_policy)
{
}

}
}
