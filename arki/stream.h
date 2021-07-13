#ifndef ARKI_STREAM_H
#define ARKI_STREAM_H

#include <arki/core/fwd.h>
#include <arki/stream/fwd.h>
#include <memory>
#include <cstdint>
#include <functional>
#include <vector>
#include <stdexcept>
#include <iosfwd>
#include <sys/types.h>

namespace arki {

namespace stream {

class FilterProcess;

/// Exception thrown when a stream output times out
class TimedOut : public std::runtime_error
{
public:
    using std::runtime_error::runtime_error;
};

std::ostream& operator<<(std::ostream& out, const SendResult& r);

}


class StreamOutput
{
public:
    virtual ~StreamOutput();

    /// Return a name describing this stream output
    virtual std::string name() const = 0;

    /**
     * After each successful write operation, call the callback with the number
     * of bytes that have just been written
     */
    virtual void set_progress_callback(std::function<void(size_t)> f) = 0;

    /**
     * Set a callback to be called once before the first byte is streamed out
     */
    virtual void set_data_start_callback(std::function<stream::SendResult(StreamOutput&)>) = 0;

    /**
     * Pipe all input data through the given child command before sending it to
     * the stream output.
     */
    virtual stream::FilterProcess* start_filter(const std::vector<std::string>& command) = 0;

    /**
     * Stop sending data through a child command, and shut it down.
     */
    virtual std::pair<size_t, size_t> stop_filter() = 0;

    /**
     * Just stop the filter process
     */
    virtual void abort_filter() = 0;

    /**
     * Stream the contents of a memory buffer.
     *
     * Returns the number of bytes written, which is always size.
     */
    virtual stream::SendResult send_buffer(const void* data, size_t size) = 0;

    /**
     * Stream a line of text, adding a newline.
     *
     * Returns the number of bytes written, which is always size + the size of
     * the newline (currently 1).
     */
    virtual stream::SendResult send_line(const void* data, size_t size) = 0;

    /**
     * Stream a portion of an open file.
     *
     * Returns the number of bytes written, which is always size.
     */
    virtual stream::SendResult send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) = 0;

    /**
     * Create a StreamOutput to stream to a file.
     *
     * If timeout_ms is 0 or not specified, stream operations can block on
     * writes for as long as needed (possibly indefinitely).
     *
     * Otherwise, stream operations can block on writes for at most the given
     * number of milliseconds. If timed out, StreamTimedOut is raised
     */
    static std::unique_ptr<StreamOutput> create(std::shared_ptr<core::NamedFileDescriptor> out, unsigned timeout_ms=0);

    /**
     * Create a Streamoutput to stream to a memory buffer
     */
    static std::unique_ptr<StreamOutput> create(std::vector<uint8_t>& out);

    /**
     * Create a Streamoutput that discards its input
     */
    static std::unique_ptr<StreamOutput> create_discard();
};

}
#endif
