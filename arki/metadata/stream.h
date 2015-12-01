#ifndef ARKI_METADATA_STREAM_H
#define ARKI_METADATA_STREAM_H

/// Read metadata incrementally from a data stream

#include <arki/metadata.h>
#include <string>

namespace arki {
namespace metadata {

/**
 * Turn a stream of bytes into a stream of metadata
 */
class Stream
{
    metadata_dest_func consumer;
    std::unique_ptr<Metadata> md;
	std::string streamname;
	std::string buffer;
	enum { METADATA, DATA } state;
	size_t dataToGet;

	bool checkMetadata();
	bool checkData();
    bool check();

public:
    Stream(metadata_dest_func consumer, const std::string& streamname)
        : consumer(consumer), streamname(streamname), state(METADATA) {}

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

}
}

#endif
