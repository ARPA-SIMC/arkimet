#include "arki/exceptions.h"
#include "arki/types/task.h"
#include "arki/types/utils.h"
#include "arki/core/binary.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/libconfig.h"
#include <sstream>
#include <cmath>

#define CODE TYPE_TASK
#define TAG "task"
#define SERSIZELEN 	1

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Task>::type_tag = TAG;
const types::Code traits<Task>::type_code = CODE;
const size_t traits<Task>::type_sersize_bytes = SERSIZELEN;

int Task::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Task* v = dynamic_cast<const Task*>(&o);
	if (!v)
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a Task, but it is a ") + typeid(&o).name() + " instead");

    return task.compare(v->task);
}

bool Task::equals(const Type& o) const
{
	const Task* v = dynamic_cast<const Task*>(&o);
	if (!v) return false;
	return task == v->task;
}

void Task::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    enc.add_varint(task.size());
    enc.add_raw(task);
}

unique_ptr<Task> Task::decode(core::BinaryDecoder& dec)
{
    size_t vallen = dec.pop_varint<size_t>("task text size");
    string val = dec.pop_string(vallen, "task text");
    return Task::create(val);
}

std::ostream& Task::writeToOstream(std::ostream& o) const
{
	return o << task;
}

void Task::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.add(keys.task_value, task);
}

std::unique_ptr<Task> Task::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return Task::create(val.as_string(keys.task_value, "Task value"));
}

unique_ptr<Task> Task::decodeString(const std::string& val)
{
	if (val.empty())
		throw_consistency_error("parsing Task", "string is empty");
	//if (val[0] != '[')
	//	throw_consistency_error("parsing Task", "string does not start with open square bracket");
	//size_t pos = val.find(']');
	//if (pos == string::npos)
	//	throw_consistency_error("parsing Task", "no closed square bracket found");
	return Task::create(val);
}

Task* Task::clone() const
{
    return new Task(task);
}

unique_ptr<Task> Task::create(const std::string& val)
{
    return unique_ptr<Task>(new Task(val));
}

/*============================================================================*/

void Task::init()
{
    MetadataType::register_type<Task>();
}

/*============================================================================*/

}
}

#include <arki/types/core.tcc>
