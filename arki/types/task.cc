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

using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Task>::type_tag = TAG;
const types::Code traits<Task>::type_code = CODE;
const size_t traits<Task>::type_sersize_bytes = SERSIZELEN;

std::string Task::get() const
{
    core::BinaryDecoder dec(data, size);
    size_t vallen = dec.pop_varint<size_t>("task text size");
    return dec.pop_string(vallen, "task text");
}

int Task::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

    // We should be the same kind, so upcast
    const Task* v = dynamic_cast<const Task*>(&o);
    if (!v)
        throw_consistency_error(
                "comparing metadata types",
                std::string("second element claims to be a Task, but it is a ") + typeid(&o).name() + " instead");

    return get().compare(v->get());
}

std::unique_ptr<Task> Task::decode(core::BinaryDecoder& dec, bool reuse_buffer)
{
    dec.ensure_size(1, "Task data");
    std::unique_ptr<Task> res;
    if (reuse_buffer)
        res.reset(new Task(dec.buf, dec.size, false));
    else
        res.reset(new Task(dec.buf, dec.size));
    dec.skip(dec.size);
    return res;
}

std::ostream& Task::writeToOstream(std::ostream& o) const
{
    return o << get();
}

void Task::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.add(keys.task_value, get());
}

std::unique_ptr<Task> Task::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return Task::create(val.as_string(keys.task_value, "Task value"));
}

std::unique_ptr<Task> Task::decodeString(const std::string& val)
{
    if (val.empty())
        throw_consistency_error("parsing Task", "string is empty");
    return Task::create(val);
}

Task* Task::clone() const
{
    return new Task(data, size);
}

std::unique_ptr<Task> Task::create(const std::string& val)
{
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_varint(val.size());
    enc.add_raw(val);
    return std::unique_ptr<Task>(new Task(buf));
}

void Task::init()
{
    MetadataType::register_type<Task>();
}

}
}
