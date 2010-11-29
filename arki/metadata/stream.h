#ifndef ARKI_METADATA_STREAM_H
#define ARKI_METADATA_STREAM_H

/*
 * metadata/stream - Read metadata incrementally from a data stream
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */


#include <arki/metadata.h>
#include <string>

namespace arki {
namespace metadata {
class Consumer;

/**
 * Turn a stream of bytes into a stream of metadata
 */
class Stream
{
	Consumer& consumer;
	Metadata md;
	std::string streamname;
	std::string buffer;
	enum { METADATA, DATA } state;
	size_t dataToGet;

	bool checkMetadata();
	bool checkData();
    bool check();

public:
	Stream(Consumer& consumer, const std::string& streamname)
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

// vim:set ts=4 sw=4:
#endif
