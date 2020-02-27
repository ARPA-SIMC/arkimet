#include "query.h"

using namespace std;

namespace arki {
namespace dataset {

DataQuery::DataQuery() : with_data(false) {}
DataQuery::DataQuery(const std::string& matcher, bool with_data) : matcher(Matcher::parse(matcher)), with_data(with_data), sorter(0) {}
DataQuery::DataQuery(const Matcher& matcher, bool with_data) : matcher(matcher), with_data(with_data), sorter(0) {}
DataQuery::~DataQuery() {}


ByteQuery::ByteQuery()
{
    with_data = true;
}

void ByteQuery::setData(const Matcher& m)
{
    with_data = true;
    type = BQ_DATA;
    matcher = m;
}

void ByteQuery::setPostprocess(const Matcher& m, const std::string& procname)
{
    with_data = true;
    type = BQ_POSTPROCESS;
    matcher = m;
    param = procname;
}

}
}
