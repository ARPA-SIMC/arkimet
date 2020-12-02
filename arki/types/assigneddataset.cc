#include "arki/exceptions.h"
#include "arki/types/assigneddataset.h"
#include "arki/types/utils.h"
#include "arki/core/binary.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/libconfig.h"
#include <sstream>
#include <cmath>

#define CODE TYPE_ASSIGNEDDATASET
#define TAG "assigneddataset"
#define SERSIZELEN 2

using namespace std;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace types {

const char* traits<AssignedDataset>::type_tag = TAG;
const types::Code traits<AssignedDataset>::type_code = CODE;
const size_t traits<AssignedDataset>::type_sersize_bytes = SERSIZELEN;

int AssignedDataset::compare(const Type& o) const
{
    if (int res = types::CoreType<AssignedDataset>::compare(o)) return res;
    const AssignedDataset* v = dynamic_cast<const AssignedDataset*>(&o);
    if (name < v->name) return -1;
    if (name > v->name) return 1;
    if (id < v->id) return -1;
    if (id > v->id) return 1;
    return 0;
}

bool AssignedDataset::equals(const Type& o) const
{
	const AssignedDataset* v = dynamic_cast<const AssignedDataset*>(&o);
	if (!v) return false;
	return name == v->name && id == v->id;
}

void AssignedDataset::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    changed.encodeWithoutEnvelope(enc);
    enc.add_unsigned(name.size(), 1); enc.add_raw(name);
    enc.add_unsigned(id.size(), 2); enc.add_raw(id);
}

unique_ptr<AssignedDataset> AssignedDataset::decode(core::BinaryDecoder& dec)
{
    Time changed = Time::decode(dec);
    size_t name_len = dec.pop_uint(1, "length of dataset name");
    string name = dec.pop_string(name_len, "dataset name");
    size_t id_len = dec.pop_uint(2, "length of dataset id");
    string id = dec.pop_string(id_len, "dataset id");
    return AssignedDataset::create(changed, name, id);
}

std::ostream& AssignedDataset::writeToOstream(std::ostream& o) const
{
	return o << name << " as " << id << " imported on " << changed;
}

void AssignedDataset::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.add(keys.assigneddataset_time); e.add(changed);
    e.add(keys.assigneddataset_name, name);
    e.add(keys.assigneddataset_id, id);
}

std::unique_ptr<AssignedDataset> AssignedDataset::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return AssignedDataset::create(
            val.as_time(keys.assigneddataset_time, "AssignedDataset time"),
            val.as_string(keys.assigneddataset_name, "AssignedDataset name"),
            val.as_string(keys.assigneddataset_id, "AssignedDataset id"));
}

unique_ptr<AssignedDataset> AssignedDataset::decodeString(const std::string& val)
{
	size_t pos = val.find(" as ");
	if (pos == string::npos)
		throw_consistency_error("parsing dataset attribution",
			"string \"" + val + "\" does not contain \" as \"");
	string name = val.substr(0, pos);
	pos += 4;
	size_t idpos = val.find(" imported on ", pos);
	if (idpos == string::npos)
		throw_consistency_error("parsing dataset attribution",
			"string \"" + val + "\" does not contain \" imported on \" after the dataset name");
	string id = val.substr(pos, idpos - pos);
	pos = idpos + 13;
    Time changed = Time::decodeString(val.substr(pos));

    return AssignedDataset::create(changed, name, id);
}

AssignedDataset* AssignedDataset::clone() const
{
    return new AssignedDataset(changed, name, id);
}

unique_ptr<AssignedDataset> AssignedDataset::create(const std::string& name, const std::string& id)
{
    return unique_ptr<AssignedDataset>(new AssignedDataset(Time::create_now(), name, id));
}

unique_ptr<AssignedDataset> AssignedDataset::create(const Time& changed, const std::string& name, const std::string& id)
{
    return unique_ptr<AssignedDataset>(new AssignedDataset(changed, name, id));
}

void AssignedDataset::init()
{
    MetadataType::register_type<AssignedDataset>();
}

}
}

#include <arki/types/core.tcc>
