#ifndef ARKI_METADATA_VALIDATOR_H
#define ARKI_METADATA_VALIDATOR_H

#include <arki/metadata/fwd.h>
#include <string>
#include <vector>
#include <map>
#include <memory>

namespace arki {
namespace metadata {

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
}

#endif
