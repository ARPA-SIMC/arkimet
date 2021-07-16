#ifndef ARKI_STREAM_CONCRETE_TCC
#define ARKI_STREAM_CONCRETE_TCC

#include "concrete.h"
#include "filter.h"
#include "arki/utils/sys.h"
#include "arki/utils/accounting.h"
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
stream::FilterProcess* ConcreteStreamOutputBase<Backend>::start_filter(const std::vector<std::string>& command)
{
    auto res = BaseStreamOutput::start_filter(command);
    filter_sender.reset(new FilterLoop<Backend, FromFilterSplice<Backend>>(*this));
    return res;
}

template<typename Backend>
void ConcreteStreamOutputBase<Backend>::flush_filter_output()
{
    try {
        filter_sender->flush();
    } catch (SpliceNotAvailable&) {
        filter_sender.reset(new FilterLoop<Backend, FromFilterReadWrite<Backend>>(*this));
        filter_sender->flush();
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
        try {
            return filter_sender->send_buffer(data, size);
        } catch (SpliceNotAvailable&) {
            filter_sender.reset(new FilterLoop<Backend, FromFilterReadWrite<Backend>>(*this));
            return filter_sender->send_buffer(data, size);
        }
    } else {
        return unfiltered_loop.send_buffer(data, size);
    }
}

template<typename Backend>
SendResult ConcreteStreamOutputBase<Backend>::send_line(const void* data, size_t size)
{
    SendResult result;
    // Don't skip if size == 0, because sending an empty buffer needs to send
    // an empty line

    if (filter_process)
    {
        try {
            return filter_sender->send_line(data, size);
        } catch (SpliceNotAvailable&) {
            filter_sender.reset(new FilterLoop<Backend, FromFilterReadWrite<Backend>>(*this));
            return filter_sender->send_line(data, size);
        }
    } else {
        return unfiltered_loop.send_line(data, size);
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
            return filter_sender->send_file_segment(fd, offset, size);
        } catch (SpliceNotAvailable&) {
            filter_sender.reset(new FilterLoop<Backend, FromFilterReadWrite<Backend>>(*this));
            return filter_sender->send_file_segment(fd, offset, size);
        }
    } else {
        return unfiltered_loop.send_file_segment(fd, offset, size);
    }
}

}
}

#endif
