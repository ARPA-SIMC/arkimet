#ifndef ARKI_CORE_STREAM_H
#define ARKI_CORE_STREAM_H

#include <arki/core/fwd.h>
#include <memory>
#include <cstdint>
#include <sys/types.h>

namespace arki {
namespace core {


class StreamOutput
{
public:
    virtual ~StreamOutput();

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
    static std::unique_ptr<StreamOutput> create(NamedFileDescriptor& out);

    /**
     * Create a StreamOutput to stream to a file.
     *
     * Stream operations can block on writes for at most the given number of
     * milliseconds. If timed out, an exception is raised (TODO: which one?)
     */
    static std::unique_ptr<StreamOutput> create(NamedFileDescriptor& out, unsigned timeout_ms);

    /**
     * Create a Streamoutput to stream to an abstract output file
     */
    static std::unique_ptr<StreamOutput> create(AbstractOutputFile& out);
};


}
}
#endif
