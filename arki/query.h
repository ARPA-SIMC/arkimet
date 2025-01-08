#ifndef ARKI_QUERY_H
#define ARKI_QUERY_H

#include <arki/stream/fwd.h>
#include <arki/query/fwd.h>
#include <arki/matcher.h>
#include <string>

namespace arki {
namespace query {

class Progress;

struct Data
{
    /// Matcher used to select data
    Matcher matcher;

    /**
     * Hint for the dataset backend to let them know that we also want the data
     * and not just the metadata.
     *
     * This is currently used:
     *  - by the HTTP client dataset, which will only download data from the
     *    server if this option is set
     *  - by local datasets to read-lock the segments for the duration of the
     *    query
     */
    bool with_data;

    /// Optional compare function to define a custom ordering of the result
    std::shared_ptr<metadata::sort::Compare> sorter;

    /// Optional progress reporting
    std::shared_ptr<query::Progress> progress;

    Data();
    Data(const Data&) = default;
    Data(Data&&) = default;
    Data(const Matcher& matcher, bool with_data=false);
    ~Data();
    Data& operator=(const Data&) = default;
    Data& operator=(Data&&) = default;
};

struct Bytes : public Data
{
    enum Type {
        BQ_DATA = 0,
        BQ_POSTPROCESS = 1,
    };

    std::string postprocessor;
    Type type = BQ_DATA;

    Bytes();

    void setData(const Matcher& m);
    void setPostprocess(const Matcher& m, const std::string& procname);
};

}
}

#endif

