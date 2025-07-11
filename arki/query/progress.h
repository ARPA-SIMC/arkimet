#ifndef ARKI_QUERY_PROGRESS_H
#define ARKI_QUERY_PROGRESS_H

#include <arki/defs.h>

namespace arki {
namespace query {

/// Interface for generating progress updates on queries
class Progress
{
protected:
    size_t expected_count = 0;
    size_t expected_bytes = 0;
    size_t count          = 0;
    size_t bytes          = 0;

public:
    virtual ~Progress();

    virtual void start(size_t expected_count = 0, size_t expected_bytes = 0);
    virtual void update(size_t count, size_t bytes);
    virtual void done();

    /// Wrap a metadata_dest_func to provide updates to this Progress
    metadata_dest_func wrap(metadata_dest_func);
};

/**
 * RAII convenience function to call progress start/done tied to scope
 * visibility.
 *
 * It also does the right thing (i.e. transparently becomes a noop) in case
 * progress is not set.
 */
struct TrackProgress
{
    std::shared_ptr<Progress> progress;

    TrackProgress(std::shared_ptr<Progress> progress, size_t expected_count = 0,
                  size_t expected_bytes = 0)
        : progress(progress)
    {
        if (progress)
            progress->start(expected_count, expected_bytes);
    }

    ~TrackProgress() {}

    metadata_dest_func wrap(metadata_dest_func dest)
    {
        if (progress)
            return progress->wrap(dest);
        else
            return dest;
    }

    /// Returns res, so it can be called to wrap the last function in a
    /// query_data
    bool done(bool res)
    {
        if (progress)
            progress->done();
        return res;
    }

    void done()
    {
        if (progress)
            progress->done();
    }
};

} // namespace query
} // namespace arki

#endif
