#ifndef ARKI_STREAM_H
#define ARKI_STREAM_H

#include <arki/core/fwd.h>
#include <memory>
#include <cstdint>
#include <functional>
#include <sys/types.h>

namespace arki {

class StreamTimedOut : public std::runtime_error
{
public:
    using std::runtime_error::runtime_error;
};


class StreamClosed : public std::runtime_error
{
public:
    using std::runtime_error::runtime_error;
};


class StreamOutput
{
public:
    virtual ~StreamOutput();

    /**
     * After each successful write operation, call the callback with the number
     * of bytes that have just been written
     */
    virtual void set_progress_callback(std::function<void(size_t)> f) = 0;

    /**
     * Set a callback to be called once before the first byte is streamed out
     */
    virtual void set_data_start_callback(std::function<void(StreamOutput&)>) = 0;

    /**
     * Stream a line of text, adding a newline.
     *
     * Returns the number of bytes written, which is always size + the size of
     * the newline (currently 1).
     */
    virtual size_t send_line(const void* data, size_t size) = 0;

    /**
     * Stream a portion of an open file.
     *
     * Returns the number of bytes written, which is always size.
     */
    virtual size_t send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) = 0;

    /**
     * Stream the contents of a memory buffer.
     *
     * Returns the number of bytes written, which is always size.
     */
    virtual size_t send_buffer(const void* data, size_t size) = 0;

    /**
     * Stream an arbitrary chunk of data from a pipe.
     *
     * Returns the number of bytes written, which depends on how much data was
     * read by a single read operation, or spliced by a single splice operation.
     */
    virtual size_t send_from_pipe(int fd) = 0;

    /**
     * Create a StreamOutput to stream to a file.
     *
     * Stream operations can block on writes for as long as needed (possibly
     * indefinitely).
     */
    static std::unique_ptr<StreamOutput> create(core::NamedFileDescriptor& out);

    /**
     * Create a StreamOutput to stream to a file.
     *
     * Stream operations can block on writes for at most the given number of
     * milliseconds. If timed out, StreamTimedOut is raised
     */
    static std::unique_ptr<StreamOutput> create(core::NamedFileDescriptor& out, unsigned timeout_ms);

    /**
     * Create a Streamoutput to stream to an abstract output file
     */
    static std::unique_ptr<StreamOutput> create(core::AbstractOutputFile& out);
};


}
#endif
