#ifndef ARKI_STREAM_ABSTRACT_TCC
#define ARKI_STREAM_ABSTRACT_TCC

#include "abstract.h"
#include "filter.h"
#include "loops.h"
#include "loops.tcc"
#include "concrete-parts.tcc"

namespace arki {
namespace stream {

template<typename Backend> template<template<typename> class ToPipe, typename... Args>
SendResult AbstractStreamOutput<Backend>::_send_from_pipe(Args&&... args)
{
    SenderFiltered<Backend, ToPipe<Backend>, FromFilterAbstract<Backend>> sender(*this, ToPipe<Backend>(std::forward<Args>(args)...));
    return sender.loop();
}

template<typename Backend>
void AbstractStreamOutput<Backend>::flush_filter_output()
{
    FlushFilter<Backend, FromFilterAbstract<Backend>> sender(*this);
    sender.loop();
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
    {
        return _send_from_pipe<BufferToPipe>(data, size);
    } else {
        result +=_write_output_buffer(data, size);
    }
    if (progress_callback)
        progress_callback(size);
    return result;
}

template<typename Backend>
SendResult AbstractStreamOutput<Backend>::send_line(const void* data, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    if (filter_process)
    {
        return _send_from_pipe<LineToPipe>(data, size);
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
        try {
            return _send_from_pipe<FileToPipeSendfile>(fd, offset, size);
        } catch (SendfileNotAvailable&) {
            return _send_from_pipe<FileToPipeReadWrite>(fd, offset, size);
        }
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
