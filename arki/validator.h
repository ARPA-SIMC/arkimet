#ifndef ARKI_VALIDATOR_H
#define ARKI_VALIDATOR_H

/*
 * validator - Arkimet validators
 *
 * Copyright (C) 2012  ARPAE-SIMC <simc-urp@arpae.it>
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

#include <string>
#include <vector>
#include <map>
#include <memory>

namespace arki {
class Metadata;

struct Validator
{
    std::string name;
    std::string desc;

    virtual ~Validator();

    /**
     * Validate a Metadata.
     *
     * @param errors
     *   validation error messages will be appended to this vector.
     * @returns
     *   true if there were no validation errors, else false.
     */
    virtual bool operator()(const Metadata& v, std::vector<std::string>& errors) const = 0;
};

namespace validators
{
    struct FailAlways : public Validator
    {
        FailAlways();
        virtual bool operator()(const Metadata& v, std::vector<std::string>& errors) const;
    };

    struct DailyImport : public Validator
    {
        DailyImport();
        virtual bool operator()(const Metadata& v, std::vector<std::string>& errors) const;
    };
}

struct ValidatorRepository : public std::map<std::string, Validator*>
{
    ~ValidatorRepository();

    /**
     * Add the validator to the repository.
     */
    void add(std::unique_ptr<Validator> v);

    /// Get the singleton version of the repository
    static const ValidatorRepository& get();
};


}

// vim:set ts=4 sw=4:
#endif
