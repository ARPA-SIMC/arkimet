#include "rearrange.h"
#include "arki/exceptions.h"
#include <iostream>
#include <sstream>

namespace arki::utils::rearrange {

bool Span::maybe_merge(const Span& other)
{
    if (src_offset + size == other.src_offset and
        dst_offset + size == other.dst_offset)
    {
        size += other.size;
        return true;
    }
    return false;
}

bool Span::operator==(const Span& other) const
{
    return std::tie(src_offset, dst_offset, size) ==
           std::tie(other.src_offset, other.dst_offset, other.size);
}

std::ostream& operator<<(std::ostream& o, const Span& s)
{
    return o << s.src_offset << "→" << s.dst_offset << "+" << s.size;
}

void Plan::add(const rearrange::Span& span)
{
    if (empty() || !back().maybe_merge(span))
        emplace_back(span);
}

void Plan::execute(core::File& in, core::File& out)
{
    for (const auto& span : *this)
    {
        off_t off_in  = span.src_offset;
        off_t off_out = span.dst_offset;
        size_t size   = span.size;
        while (size > 0)
        {
            ssize_t copied =
                copy_file_range(in, &off_in, out, &off_out, size, 0);
            if (copied == 0)
                throw std::runtime_error("source segment seems truncated "
                                         "compared to spans in metadata");
            else if (copied == -1)
            {
                std::stringstream buf;
                buf << span << ": copy_file_range failed";
                throw_system_error(buf.str());
            }
            else
                size -= copied;
        }
    }
}

} // namespace arki::utils::rearrange
