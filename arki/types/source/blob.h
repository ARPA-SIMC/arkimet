#ifndef ARKI_TYPES_SOURCE_BLOB_H
#define ARKI_TYPES_SOURCE_BLOB_H

/*
 * types/source - Source information
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/types/source.h>

namespace arki {

namespace utils {
struct DataReader;
}

namespace types {
namespace source {

extern arki::utils::DataReader dataReader;

struct Blob : public Source
{
    /**
     * Base directory used to resolve relative filenames.
     *
     * Note that this is not stored when serializing, since metadata usually
     * point to files relative to the metadata location, in order to save
     * space.
     */
    std::string basedir;

    /**
     * Data file name.
     *
     * Can be an absolute or a relative path. If it is a relative path, it is
     * resolved based on \a basedir, or on the current directory if \a basedir
     * is empty.
     */
    std::string filename;

    uint64_t offset;
    uint64_t size;

    virtual Style style() const;
    virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
    virtual std::ostream& writeToOstream(std::ostream& o) const;
    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const;
    virtual const char* lua_type_name() const;
    virtual bool lua_lookup(lua_State* L, const std::string& name) const;

    virtual int compare_local(const Source& o) const;
    virtual bool operator==(const Type& o) const;

    virtual uint64_t getSize() const;

    /// Return the absolute pathname to the data file
    std::string absolutePathname() const;

    /**
     * Return a new source identical to this one, but with all the
     * directory components stripped from the file name.
     *
     * basedir is updated so that we can still reach the data file.
     */
    Item<Blob> fileOnly() const;

    /**
     * Return a new source identical to this one, but with an absolute file
     * name and no basedir.
     */
    Item<Blob> makeAbsolute() const;

    virtual wibble::sys::Buffer loadData() const;

    static Item<Blob> create(const std::string& format, const std::string& basedir, const std::string& filename, uint64_t offset, uint64_t size);
    static Item<Blob> decodeMapping(const emitter::memory::Mapping& val);
};

}
}
}
#endif
