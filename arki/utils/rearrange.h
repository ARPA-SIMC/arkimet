#ifndef ARKI_UTILS_REARRANGE_H
#define ARKI_UTILS_REARRANGE_H

#include <arki/core/file.h>
#include <cstdint>
#include <vector>
#include <iosfwd>

namespace arki::utils::rearrange {

struct Span
{
    size_t src_offset;
    size_t dst_offset;
    size_t size;

    bool maybe_merge(const Span& other);

    bool operator==(const Span& other) const;
};

std::ostream& operator<<(std::ostream& o, const Span& s);

class Plan : public std::vector<Span>
{
public:
    Plan() = default;

    void add(const Span& span);
    void add(size_t src_offset, size_t dst_offset, size_t size)
    {
        add(Span{src_offset, dst_offset, size});
    }

    void execute(core::File& in, core::File& out);
};

}

#endif
