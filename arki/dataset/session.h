#ifndef ARKI_DATASET_SESSION_H
#define ARKI_DATASET_SESSION_H

#include <arki/segment/fwd.h>
#include <arki/core/fwd.h>
#include <arki/dataset/fwd.h>
#include <unordered_map>
#include <string>

namespace arki {
namespace dataset {

class Session
{
protected:
    /// Map segment absolute paths to possibly reusable reader instances
    std::unordered_map<std::string, std::weak_ptr<segment::Reader>> reader_pool;

    std::shared_ptr<segment::Reader> reader_from_pool(const std::string& abspath);

public:
    virtual ~Session();

    virtual std::shared_ptr<segment::Reader> segment_reader(const std::string& format, const std::string& root, const std::string& relpath, std::shared_ptr<core::Lock> lock);
    virtual std::shared_ptr<segment::Writer> segment_writer(const std::string& format, const std::string& root, const std::string& relpath);
    virtual std::shared_ptr<segment::Checker> segment_checker(const std::string& format, const std::string& root, const std::string& relpath);
};

}
}
#endif
