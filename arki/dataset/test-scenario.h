/**
 * dataset/test-scenario - Build dataset scenarios for testing arkimet
 *
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#ifndef ARKI_DATASET_TEST_SCENARIO_H
#define ARKI_DATASET_TEST_SCENARIO_H

#include <arki/configfile.h>
#include <string>
#include <vector>

namespace arki {
namespace dataset {
namespace test {

/**
 * One global dataset scenario
 *
 * This creates a dataset in a directory, initialised using the scenario
 * build() method.
 *
 * Scenarios are built once and then reused, to avoid an expensive rebuild at
 * every test; this means that they are only supposed to be used for testing
 * ReadonlyDataset.
 */
struct Scenario
{
    ConfigFile cfg;
    std::string name;
    std::string path;
    std::string desc;
    bool built;

    Scenario(const std::string& name);
    virtual ~Scenario();

    /**
     * Build the scenarion.
     *
     * The base class implementation takes care of cleaning the directory and doing base config initialisation based on name();
     */
    virtual void build();

    /**
     * Make a full copy of this scenario in the given directory.
     *
     * Return a new ConfigFile with the path pointing to the copy
     */
    ConfigFile clone(const std::string& newpath) const;

    // Get the scenario with the given name, building it if necessary, or
    // reusing the already built one
    static const Scenario& get(const std::string& name);

    // List all available scenario names
    static std::vector<std::string> list();
};

}
}
}

#endif
