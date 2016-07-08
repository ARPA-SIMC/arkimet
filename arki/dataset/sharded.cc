#include "sharded.h"

using namespace std;

namespace arki {
namespace dataset {
namespace sharded {

Reader::~Reader() {}
std::string Reader::type() const { throw std::runtime_error("not implemented"); }
void Reader::query_data(const dataset::DataQuery& q, metadata_dest_func dest) { throw std::runtime_error("not implemented"); }
void Reader::query_summary(const Matcher& matcher, Summary& summary) { throw std::runtime_error("not implemented"); }
void Reader::expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) { throw std::runtime_error("not implemented"); }

Writer::~Writer() {}
std::string Writer::type() const { throw std::runtime_error("not implemented"); }
Writer::AcquireResult Writer::acquire(Metadata& md, Writer::ReplaceStrategy replace) { throw std::runtime_error("not implemented"); }
void Writer::remove(Metadata& md) { throw std::runtime_error("not implemented"); }
void Writer::flush() { throw std::runtime_error("not implemented"); }

Checker::~Checker() {}
std::string Checker::type() const { throw std::runtime_error("not implemented"); }
void Checker::removeAll(dataset::Reporter& reporter, bool writable) { throw std::runtime_error("not implemented"); }
void Checker::repack(dataset::Reporter& reporter, bool writable) { throw std::runtime_error("not implemented"); }
void Checker::check(dataset::Reporter& reporter, bool fix, bool quick) { throw std::runtime_error("not implemented"); }

}
}
}
