#ifndef ARKI_STREAM_CONCRETE_TCC
#define ARKI_STREAM_CONCRETE_TCC

#include "concrete.h"
#include "filter.h"
#include "arki/utils/sys.h"
#include "arki/utils/accounting.h"
#include "arki/utils/process.h"
#include "arki/libconfig.h"
#include <system_error>
#include <sys/sendfile.h>
#include "loops.tcc"

namespace arki {
namespace stream {

template<typename Backend>
std::string ConcreteStreamOutputBase<Backend>::name() const { return out->name(); }

template<typename Backend>
uint32_t ConcreteStreamOutputBase<Backend>::wait_writable()
{
    pollinfo.revents = 0;
    int res = Backend::poll(&pollinfo, 1, timeout_ms);
    if (res < 0)
        throw std::system_error(errno, std::system_category(), "poll failed on " + out->name());
    if (res == 0)
        throw TimedOut("write on " + out->name() + " timed out");
    if (pollinfo.revents & POLLERR)
        return SendResult::SEND_PIPE_EOF_DEST;
    if (pollinfo.revents & POLLOUT)
        return 0;
    throw std::runtime_error("unsupported revents values when polling " + out->name());
}

template<typename Backend>
ConcreteStreamOutputBase<Backend>::ConcreteStreamOutputBase(std::shared_ptr<core::NamedFileDescriptor> out, int timeout_ms)
    : out(out), unfiltered_loop(*this)
{
    this->timeout_ms = timeout_ms;
    orig_fl = fcntl(*out, F_GETFL);
    if (orig_fl < 0)
        throw std::system_error(errno, std::system_category(), "cannot get file descriptor flags for " + out->name());
    if (fcntl(*out, F_SETFL, orig_fl | O_NONBLOCK) < 0)
        throw std::system_error(errno, std::system_category(), "cannot set nonblocking file descriptor flags for " + out->name());

    pollinfo.fd = *out;
    pollinfo.events = POLLOUT;
}

template<typename Backend>
ConcreteStreamOutputBase<Backend>::~ConcreteStreamOutputBase()
{
    // If out is still open, reset as it was before
    if (*out != -1)
        fcntl(*out, F_SETFL, orig_fl);
}

template<typename Backend>
stream::SendResult ConcreteStreamOutputBase<Backend>::_write_output_buffer(const void* data, size_t size)
{
    SendResult result;
    utils::Sigignore ignpipe(SIGPIPE);
    size_t pos = 0;
    while (true)
    {
        ssize_t res = Backend::write(*out, (const uint8_t*)data + pos, size - pos);
        if (res < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                res = 0;
            else if (errno == EPIPE) {
                result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                break;
            } else
                throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(size - pos) + " bytes to " + out->name());
        }

        pos += res;

        if (pos >= size)
            break;

        uint32_t wres = wait_writable();
        if (wres)
        {
            result.flags |= wres;
            break;
        }
    }
    return result;
}

template<typename Backend>
stream::FilterProcess* ConcreteStreamOutputBase<Backend>::start_filter(const std::vector<std::string>& command)
{
    auto res = BaseStreamOutput::start_filter(command);
    has_splice = true;
    return res;
}

template<typename Backend> template<template<typename> class ToPipe, typename... Args>
SendResult ConcreteStreamOutputBase<Backend>::_send_from_pipe(Args&&... args)
{
    if (has_splice)
    {
        try {
            FilterLoop<Backend, FromFilterSplice<Backend>> sender(*this);
            return sender.loop(ToPipe<Backend>(std::forward<Args>(args)...));
        } catch (SpliceNotAvailable&) {
            has_splice = false;
            FilterLoop<Backend, FromFilterReadWrite<Backend>> sender(*this);
            return sender.loop(ToPipe<Backend>(std::forward<Args>(args)...));
        }
    } else {
        FilterLoop<Backend, FromFilterReadWrite<Backend>> sender(*this);
        return sender.loop(ToPipe<Backend>(std::forward<Args>(args)...));
    }
}

template<typename Backend>
void ConcreteStreamOutputBase<Backend>::flush_filter_output()
{
    if (has_splice)
    {
        try {
            FilterLoop<Backend, FromFilterSplice<Backend>> sender(*this);
            sender.flush();
        } catch (SpliceNotAvailable&) {
            has_splice = false;
            FilterLoop<Backend, FromFilterReadWrite<Backend>> sender(*this);
            sender.flush();
        }
    } else {
        FilterLoop<Backend, FromFilterReadWrite<Backend>> sender(*this);
        sender.flush();
    }
}


template<typename Backend>
SendResult ConcreteStreamOutputBase<Backend>::send_buffer(const void* data, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    if (filter_process)
    {
        return _send_from_pipe<BufferToPipe>(data, size);
    } else {
        return unfiltered_loop.loop(BufferToPipe<Backend>(data, size));
    }
}

template<typename Backend>
SendResult ConcreteStreamOutputBase<Backend>::send_line(const void* data, size_t size)
{
    SendResult result;
    // TODO: error: an empty buffer should send a newline
    if (size == 0)
        return result;

    if (filter_process)
    {
        return _send_from_pipe<LineToPipe>(data, size);
    } else {
        LineToPipe<Backend> to_pipe(data, size);
        return unfiltered_loop.loop(to_pipe);
    }
    return result;
}


template<typename Backend>
SendResult ConcreteStreamOutputBase<Backend>::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
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
        try {
            return unfiltered_loop.loop(FileToPipeSendfile<Backend>(fd, offset, size));
        } catch (SendfileNotAvailable&) {
            return unfiltered_loop.loop(FileToPipeReadWrite<Backend>(fd, offset, size));
        }
    }
}

}
}

#endif
