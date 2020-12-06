#ifndef ARKI_QUERYMACRO_H
#define ARKI_QUERYMACRO_H

/// Macros implementing special query strategies

#include <arki/core/cfg.h>
#include <arki/dataset.h>
#include <string>
#include <functional>

namespace arki {
namespace dataset {

/**
 * Dataset that builds results by querying one or more other datasets.
 *
 * * `datasets` is the set of available datasets.
 * * `name` is a macro name, optionally followed by as space and macro arguments.
 * * `query` is a description of how to build results from other datasets, in a
 * language defined by the named macro.
 */
class QueryMacro : public dataset::Dataset
{
public:
    std::string macro_args;
    std::string query;

    QueryMacro(std::weak_ptr<Session> session, const std::string& name, const std::string& query);

    std::shared_ptr<Reader> create_reader() override;
};

namespace qmacro {

void register_parser(
        const std::string& ext,
        std::function<std::shared_ptr<dataset::Reader>(const std::string& source, std::shared_ptr<QueryMacro> dataset)> parser);

void init();

}
}
}
#endif
