#include "structure.h"
#include "arki/core/time.h"

namespace arki {
namespace emitter {

bool Reader::as_bool(const char* desc) const { throw std::invalid_argument(std::string("cannot read ") + desc + " as bool"); }
long long int Reader::as_int(const char* desc) const { throw std::invalid_argument(std::string("cannot read ") + desc + " as int"); }
double Reader::as_double(const char* desc) const { throw std::invalid_argument(std::string("cannot read ") + desc + " as double"); }
std::string Reader::as_string(const char* desc) const { throw std::invalid_argument(std::string("cannot read ") + desc + " as string"); }

unsigned Reader::list_size(const char* desc) const { throw std::invalid_argument(std::string("cannot get size of list ") + desc); }
bool Reader::as_bool(unsigned idx, const char* desc) const { throw std::invalid_argument(std::string("cannot read ") + desc + "[" + std::to_string(idx) + "] as bool"); }
long long int Reader::as_int(unsigned idx, const char* desc) const { throw std::invalid_argument(std::string("cannot read ") + desc + "[" + std::to_string(idx) + "] as int"); }
double Reader::as_double(unsigned idx, const char* desc) const { throw std::invalid_argument(std::string("cannot read ") + desc + "[" + std::to_string(idx) + "] as double"); }
std::string Reader::as_string(unsigned idx, const char* desc) const { throw std::invalid_argument(std::string("cannot read ") + desc + "[" + std::to_string(idx) + "] as string"); }
void Reader::sub(unsigned idx, const char* desc, std::function<void(const Reader&)>) const { throw std::invalid_argument(std::string("cannot access ") + desc + "[" + std::to_string(idx) + "]"); }

bool Reader::has_key(const std::string& key, NodeType type) const { return false; }
bool Reader::as_bool(const std::string& key, const char* desc) const { throw std::invalid_argument(std::string("cannot read ") + desc + "[" + key + "] as bool"); }
long long int Reader::as_int(const std::string& key, const char* desc) const { throw std::invalid_argument(std::string("cannot read ") + desc + "[" + key + "] as int"); }
double Reader::as_double(const std::string& key, const char* desc) const { throw std::invalid_argument(std::string("cannot read ") + desc + "[" + key + "] as double"); }
std::string Reader::as_string(const std::string& key, const char* desc) const { throw std::invalid_argument(std::string("cannot read ") + desc + "[" + key + "] as string"); }
core::Time Reader::as_time(const std::string& key, const char* desc) const { throw std::invalid_argument(std::string("cannot read ") + desc + "[" + key + "] as time"); }
void Reader::items(const char* desc, std::function<void(const std::string&, const Reader&)>) const { throw std::invalid_argument(std::string("cannot iterate ") + desc); }
void Reader::sub(const std::string& key, const char* desc, std::function<void(const Reader&)>) const { throw std::invalid_argument(std::string("cannot access ") + desc + "[" + key + "]"); }

}
}
