#ifndef ARKI_SEGMENT_H
#define ARKI_SEGMENT_H

#include <filesystem>
#include <string>

namespace arki {

class Segment
{
public:
    std::string format;
    std::filesystem::path root;
    std::filesystem::path relpath;
    std::filesystem::path abspath;

    Segment(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath);
    virtual ~Segment();
};

}

#endif
