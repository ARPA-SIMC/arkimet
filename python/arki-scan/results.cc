#include "results.h"
#include <sys/time.h>
#include <sstream>

#if __xlC__
// From glibc
#define timersub(a, b, result)                                                \
  do {                                                                        \
    (result)->tv_sec = (a)->tv_sec - (b)->tv_sec;                             \
    (result)->tv_usec = (a)->tv_usec - (b)->tv_usec;                          \
    if ((result)->tv_usec < 0) {                                              \
      --(result)->tv_sec;                                                     \
      (result)->tv_usec += 1000000;                                           \
    }                                                                         \
  } while (0)
#endif


namespace arki {
namespace python {
namespace arki_scan {

DispatchResults::DispatchResults()
{
    gettimeofday(&start_time, NULL);
    timerclear(&end_time);
}

void DispatchResults::end()
{
    gettimeofday(&end_time, NULL);
}

bool DispatchResults::success(bool ignore_duplicates) const
{
    bool success = !(not_imported || in_error_dataset);
    if (ignore_duplicates)
        return success && (successful || duplicates);
    else
        return success && (successful && !duplicates);
}

std::string DispatchResults::summary() const
{
    std::string timeinfo;
    if (timerisset(&end_time))
    {
        struct timeval diff;
        timersub(&end_time, &start_time, &diff);
        char buf[32];
        snprintf(buf, 32, " in %d.%06d seconds", (int)diff.tv_sec, (int)diff.tv_usec);
        timeinfo = buf;
    }
    if (!successful && !not_imported && !duplicates && !in_error_dataset)
        return "no data processed" + timeinfo;

    if (!not_imported && !duplicates && !in_error_dataset)
    {
        std::stringstream ss;
        ss << "everything ok: " << successful << " message";
        if (successful != 1)
            ss << "s";
        ss << " imported" + timeinfo;
        return ss.str();
    }

    std::stringstream res;

    if (not_imported)
        res << "serious problems: ";
    else
        res << "some problems: ";

    res << successful << " ok, "
        << duplicates << " duplicates, "
        << in_error_dataset << " in error dataset";

    if (not_imported)
        res << ", " << not_imported << " NOT imported";

    res << timeinfo;

    return res.str();
}


}
}
}
