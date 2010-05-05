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

#include <arki/metadata/stream.h>
#include <arki/metadata/consumer.h>
#include <arki/utils/codec.h>

#include <wibble/exception.h>
#include <wibble/sys/buffer.h>
#include <wibble/string.h>

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {
namespace metadata {

void Stream::checkMetadata()
{
	using namespace utils::codec;

	if (buffer.size() < 8) return;

	// Ensure first 2 bytes are MD
	if (buffer[0] != 'M' || buffer[1] != 'D')
		throw wibble::exception::Consistency("checking partial buffer", "buffer contains data that is not encoded metadata");

	// Get version from next 2 bytes
	unsigned int version = codec::decodeUInt((const unsigned char*)buffer.data()+2, 2);
	// Get length from next 4 bytes
	unsigned int len = codec::decodeUInt((const unsigned char*)buffer.data()+4, 4);

	// Check if we have a full metadata, in that case read it, remove it
	// from the beginning of the string, then consume it it
	if (buffer.size() < 8 + len)
		return;

	md.read((const unsigned char*)buffer.data() + 8, len, version, streamname);

	buffer = buffer.substr(len + 8);
	if (md.source->style() == types::Source::INLINE)
	{
		Item<types::source::Inline> inl = md.source.upcast<types::source::Inline>();
		dataToGet = inl->size;
		state = DATA;
		checkData();
	} else {
		consumer(md);
	}
}

void Stream::checkData()
{
	if (buffer.size() >= dataToGet)
	{
		wibble::sys::Buffer buf(buffer.data(), dataToGet);
		buffer = buffer.substr(dataToGet);
		dataToGet = 0;
		state = METADATA;
		md.setInlineData(md.source->format, buf);
		consumer(md);
		checkMetadata();
	}
}

void Stream::readData(const void* buf, size_t size)
{
	buffer.append((const char*)buf, size);

	switch (state)
	{
		case METADATA: checkMetadata(); break;
		case DATA: checkData(); break;
	}
}

}
}

// vim:set ts=4 sw=4:
