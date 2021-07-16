#ifndef ARKI_STREAM_ABSTRACT_TCC
#define ARKI_STREAM_ABSTRACT_TCC

#include "abstract.h"
#include "filter.h"
#include "loops.h"
#include "loops.tcc"
#include "concrete-parts.tcc"

namespace arki {
namespace stream {

template<typename Backend>
stream::FilterProcess* AbstractStreamOutput<Backend>::start_filter(const std::vector<std::string>& command)
{
    auto res = BaseStreamOutput::start_filter(command);
    filter_sender.reset(new FilterLoop<Backend, FromFilterAbstract<Backend>>(*this));
    return res;
}

template<typename Backend>
void AbstractStreamOutput<Backend>::flush_filter_output()
{
    FilterLoop<Backend, FromFilterAbstract<Backend>> sender(*this);
    sender.flush();
}

template<typename Backend>
stream::SendResult AbstractStreamOutput<Backend>::_write_output_line(const void* data, size_t size)
{
    stream::SendResult res = _write_output_buffer(data, size);
    res += _write_output_buffer("\n", 1);
    return res;
}

template<typename Backend>
SendResult AbstractStreamOutput<Backend>::send_buffer(const void* data, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    if (filter_process)
        return filter_sender->send_buffer(data, size);
    else
        result +=_write_output_buffer(data, size);

    if (progress_callback)
        progress_callback(size);
    return result;
}

template<typename Backend>
SendResult AbstractStreamOutput<Backend>::send_line(const void* data, size_t size)
{
    SendResult result;
    // Don't skip if size == 0, because sending an empty buffer needs to send
    // an empty line

    if (filter_process)
    {
        return filter_sender->send_line(data, size);
    } else {
        result += _write_output_line(data, size);
    }
    if (progress_callback)
        progress_callback(size + 1);
    return result;
}

template<typename Backend>
SendResult AbstractStreamOutput<Backend>::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    if (filter_process)
    {
        return filter_sender->send_file_segment(fd, offset, size);
    } else {
        TransferBuffer buffer;
        buffer.allocate();

        size_t pos = 0;
        while (pos < size)
        {
            size_t res = fd.pread(buffer, std::min(buffer.size, size - pos), offset + pos);
            if (res == 0)
                throw std::runtime_error("cannot sendfile() " + std::to_string(offset) + "+" + std::to_string(size) + " to output: the span does not seem to match the file");
            result += send_buffer(buffer, res);
            pos += res;
        }
    }

    return result;
}

}
}

#endif
