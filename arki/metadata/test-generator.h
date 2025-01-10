#ifndef ARKI_METADATA_TEST_GENERATOR_H
#define ARKI_METADATA_TEST_GENERATOR_H

/*
 * metadata/test-generator - Metadata generator to user for tests
 *
 * Copyright (C) 2010--2015  ARPAE-SIMC <simc-urp@arpae.it>
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
#include <map>
#include <vector>

namespace arki {
namespace metadata {

namespace test {

/**
 * Generic interface for metadata consumers, used to handle a stream of
 * metadata, such as after scanning a file, or querying a dataset.
 */
struct Generator
{
    typedef std::map<types::Code, std::vector<types::Type*> > Samples;
    Samples samples;
    DataFormat format;
    int variant;

    Generator(DataFormat format=DataFormat::GRIB, int variant=0);
    ~Generator();

    /// @return true if some sample has been set for the given code
    bool has(types::Code code);

    /**
     * If no samples have been given for the given metadata type, decode \a val
     * as a Yaml representation add add it as a sample
     */
    void add_if_missing(types::Code code, const std::string& val);

    /// Add one sample to the sample list
    void add(const types::Type& item);

    /// Add one sample to the sample list
    void add(std::unique_ptr<types::Type> item);

    /// Add one sample to the sample list, decoding \a val as a Yaml value
    void add(types::Code code, const std::string& val);

    /// Fill unfilled samples with GRIB1 defaults
    void defaults_grib1();

    /// Fill unfilled samples with GRIB2 defaults
    void defaults_grib2();

    /// Fill unfilled samples with BUFR defaults
    void defaults_bufr();

    /// Fill unfilled samples with ODIMH5 defaults
    void defaults_odimh5();

    /**
     * Generate one metadata item for each combination of samples given so far
     */
    void generate(metadata_dest_func cons);

    bool _generate(const Samples::const_iterator& i, Metadata& md, metadata_dest_func cons) const;
};


}
}
}

// vim:set ts=4 sw=4:
#endif
