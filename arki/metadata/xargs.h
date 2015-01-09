#ifndef ARKI_METADATA_XARGS_H
#define ARKI_METADATA_XARGS_H

/*
 * metadata/xargs - Cluster a metadata stream and run a progrgam on each batch
 *
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/metadata/clusterer.h>

namespace arki {
namespace metadata {

namespace xargs {

struct Tempfile
{
    std::string pathname_template;
    char* pathname;
    int fd;

    Tempfile();
    ~Tempfile();

    void set_template(const std::string& tpl);
    void open();
    void close();
    void close_nothrow();
    bool is_open() const;
};

}

class Xargs : public Clusterer
{
protected:
    xargs::Tempfile tempfile;

    void start_batch(const std::string& new_format) override;
    void add_to_batch(Metadata& md, const wibble::sys::Buffer& buf) override;
    void flush_batch() override;

    int run_child();

public:
    /**
     * Command to run for each batch, split in components, the first of which
     * is the executable name.
     *
     * For each invocation, command will be appended the name of the temporary
     * file with the batch data.
     */
    std::vector<std::string> command;

    /**
     * Index of the filename argument in the command.
     *
     * If it is a valid index for command, replace that element with the file
     * name before running the command on a batch.
     *
     * If it is -1, append the file name (default).
     *
     * Else, do not pass the file name on the command line.
     */
    int filename_argument;

    Xargs();
    ~Xargs()
    {
    }

    void set_max_bytes(const std::string& val);
    void set_interval(const std::string& val);
};

}
}

#endif
