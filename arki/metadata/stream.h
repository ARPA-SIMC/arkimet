#ifndef ARKI_METADATA_STREAM_H
#define ARKI_METADATA_STREAM_H

/// Read metadata incrementally from a data stream

#include <arki/defs.h>
#include <cstdint>
#include <string>
#include <vector>

namespace arki {
namespace metadata {

/**
 * Turn a stream of bytes into a stream of metadata
 */
class Stream
{
    metadata_dest_func consumer;
    std::shared_ptr<Metadata> md;
    std::string streamname;
    std::vector<uint8_t> buffer;
    enum { METADATA, DATA } state;
    size_t dataToGet;
    bool canceled = false;

    bool checkMetadata();
    bool checkData();
    bool check();

public:
    Stream(metadata_dest_func consumer, const std::string& streamname)
        : consumer(consumer), streamname(streamname), state(METADATA)
    {
    }

    /**
     * Return true if the consumer canceled receiving data
     */
    bool consumer_canceled() const { return canceled; }

    /**
     * Return the number of bytes that have not been processed yet
     */
    size_t countBytesUnprocessed() const { return buffer.size(); }

    /**
     * Send some data to the stream.
     *
     * If the data completes one or more metadata and (when appropriate) the
     * attached inline data, then they will be sent to the consumer
     */
    void readData(const void* buf, size_t size);
};

} // namespace metadata
} // namespace arki

#endif
