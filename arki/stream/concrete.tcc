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
    : out(out)
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
void ConcreteStreamOutputBase<Backend>::start_filter(const std::vector<std::string>& command)
{
    BaseStreamOutput::start_filter(command);
    has_splice = true;
}

template<typename Backend> template<template<typename> class ToPipe, typename... Args>
SendResult ConcreteStreamOutputBase<Backend>::_send_from_pipe(Args&&... args)
{
    if (has_splice)
    {
        try {
            SenderFilteredSplice<Backend, ToPipe<Backend>> sender(*this, ToPipe<Backend>(std::forward<Args>(args)...));
            return sender.loop();
        } catch (SpliceNotAvailable&) {
            has_splice = false;
            SenderFilteredReadWrite<Backend, ToPipe<Backend>> sender(*this, ToPipe<Backend>(std::forward<Args>(args)...));
            return sender.loop();
        }
    } else {
        SenderFilteredReadWrite<Backend, ToPipe<Backend>> sender(*this, ToPipe<Backend>(std::forward<Args>(args)...));
        return sender.loop();
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
        SenderDirect<Backend, BufferToPipe<Backend>> sender(*this, BufferToPipe<Backend>(data, size));
        return sender.loop();
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
        SenderDirect<Backend, LineToPipe<Backend>> sender(*this, LineToPipe<Backend>(data, size));
        return sender.loop();
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
            SenderDirect<Backend, FileToPipeSendfile<Backend>> sender(*this, FileToPipeSendfile<Backend>(fd, offset, size));
            return sender.loop();
        } catch (SendfileNotAvailable&) {
            SenderDirect<Backend, FileToPipeReadWrite<Backend>> sender(*this, FileToPipeReadWrite<Backend>(fd, offset, size));
            return sender.loop();
        }
    }
}


template<typename Backend>
std::pair<size_t, SendResult> ConcreteStreamOutputBase<Backend>::send_from_pipe(int fd)
{
    bool src_nonblock = is_nonblocking(fd);
    size_t sent = 0;
    SendResult result;

    TransferBuffer buffer;
    if (data_start_callback)
    {
        // If we have data_start_callback, we need to do a regular read/write
        // cycle before attempting the handover to splice, to see if there
        // actually are data to read and thus output to generate.
        buffer.allocate();
        ssize_t res = Backend::read(fd, buffer, buffer.size);
        // FIXME: this can EAGAIN and it's not managed with a retry
        // there isn't much sense in exiting with SEND_PIPE_EAGAIN_SOURCE
        if (res < 0)
        {
            if (errno == EAGAIN) {
                result.flags |= SendResult::SEND_PIPE_EAGAIN_SOURCE;
                return std::make_pair(sent, result);
            } else
                throw std::system_error(errno, std::system_category(), "cannot read data to stream from a pipe");
        }
        if (res == 0)
        {
            result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
            return std::make_pair(sent, result);
        }

        // If we get some output, then we *do* call data_start_callback, stream
        // it out, and proceed with the splice handover attempt
        if (data_start_callback)
            result += fire_data_start_callback();
        result += _write_output_buffer(buffer, res);
        sent += res;
        if (progress_callback)
            progress_callback(res);
    }

    bool has_splice = true;
    while (true)
    {
        if (has_splice)
        {
#ifdef HAVE_SPLICE
            utils::Sigignore ignpipe(SIGPIPE);
            // Try splice
            ssize_t res = splice(fd, NULL, *out, NULL, TransferBuffer::size * 128, SPLICE_F_MORE);
            if (res > 0)
            {
                sent += res;
                if (progress_callback)
                    progress_callback(res);
            } else if (res == 0) {
                result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
                break;
            }
            else if (res < 0)
            {
                if (errno == EINVAL)
                {
                    has_splice = false;
                    buffer.allocate();
                    continue;
                } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    res = 0;
                    // In theory we don't need to call this. In practice, it
                    // helps unit tests to be able to hook here to empty the
                    // output pipe
                    if (progress_callback)
                        progress_callback(res);
                } else if (errno == EPIPE) {
                    result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                    break;
                } else
                    throw std::system_error(errno, std::system_category(), "cannot splice data to stream from a pipe");
            }

#else
            // Splice is not supported: pass it on to the traditional method
            has_splice = false;
            buffer.allocate();
            // Skip waiting for available I/O and just retry the while
            continue;
#endif
        } else {
            // Fall back to read/write
            buffer.allocate();
            ssize_t res = Backend::read(fd, buffer, buffer.size);
            if (res == 0)
            {
                result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
                break;
            }
            if (res < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    res = 0;
                else
                    throw std::system_error(errno, std::system_category(), "cannot read data from pipe input");
            }
            if (res > 0)
            {
                if (data_start_callback)
                    result += fire_data_start_callback();
                result += _write_output_buffer(buffer, res);
                if (progress_callback)
                    progress_callback(res);
            } else {
                // Call progress callback here because we're not calling
                // send_buffer. Send_buffer will take care of calling
                // progress_callback if needed.
                if (progress_callback)
                    progress_callback(res);
            }
        }

        uint32_t wres = 0;
        if (src_nonblock)
            wres = wait_readable(fd);
        if (!wres)
            wres = wait_writable();
        if (wres)
        {
            result.flags |= wres;
            break;
        }
    }

    return std::make_pair(sent, result);
}

}
}

#endif
