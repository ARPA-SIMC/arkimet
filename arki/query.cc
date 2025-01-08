#include "query.h"

using namespace std;

namespace arki {
namespace query {

Data::Data() : with_data(false) {}
Data::Data(const Matcher& matcher, bool with_data) : matcher(matcher), with_data(with_data), sorter(0) {}
Data::~Data() {}


Bytes::Bytes()
{
    with_data = true;
}

void Bytes::setData(const Matcher& m)
{
    with_data = true;
    type = BQ_DATA;
    matcher = m;
}

void Bytes::setPostprocess(const Matcher& m, const std::string& procname)
{
    with_data = true;
    type = BQ_POSTPROCESS;
    matcher = m;
    postprocessor = procname;
}

}
}
