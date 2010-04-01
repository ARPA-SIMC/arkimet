/*
 * types - arkimet metadata type system
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

#include <arki/types.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/buffer.h>

#include <cstring>

using namespace std;
using namespace wibble;

namespace arki {
namespace types {

// Registry of item information
static const size_t decoders_size = 1024;
static const MetadataType** decoders = 0;

static void default_intern_stats_func() {}

MetadataType::MetadataType(
		types::Code serialisationCode,
		int serialisationSizeLen,
		const std::string& tag,
		item_decoder decode_func,
		string_decoder string_decode_func,
		intern_stats intern_stats_func)
    : serialisationCode(serialisationCode),
	  serialisationSizeLen(serialisationSizeLen),
	  tag(tag),
	  decode_func(decode_func),
	  string_decode_func(string_decode_func),
	  intern_stats_func(intern_stats_func ? intern_stats_func : default_intern_stats_func)
{
	// Ensure that the map is created before we add items to it
	if (!decoders)
	{
		decoders = new const MetadataType*[decoders_size];
		memset(decoders, 0, decoders_size * sizeof(int));
	}

	decoders[serialisationCode] = this;
}

MetadataType::~MetadataType()
{
	if (!decoders)
		return;
	
	decoders[serialisationCode] = 0;
}

const MetadataType* MetadataType::get(types::Code code)
{
	if (!decoders || decoders[code] == 0)
		throw wibble::exception::Consistency("parsing binary data", "no decoder found for item type " + str::fmt(code));
	return decoders[code];
}

void debug_intern_stats()
{
	for (size_t i = 0; i < decoders_size; ++i)
		if (decoders[i] != 0)
			decoders[i]->intern_stats_func();
}

}
}
// vim:set ts=4 sw=4:
