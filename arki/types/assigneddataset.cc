#include "arki/exceptions.h"
#include "arki/types/assigneddataset.h"
#include "arki/types/utils.h"
#include "arki/core/binary.h"
#include "arki/structured/emitter.h"
#include "arki/structured/reader.h"
#include "arki/structured/keys.h"
#include <sstream>

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

bool AssignedDataset::equals(const Type& o) const
{
    const AssignedDataset* v = dynamic_cast<const AssignedDataset*>(&o);
    if (!v) return false;
    core::Time changed, vchanged;
    std::string name, id, vname, vid;
    get(changed, name, id);
    v->get(vchanged, vname, vid);
    return name == vname && id == vid;
}

int AssignedDataset::compare(const Type& o) const
{
    int res = Type::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const AssignedDataset* v = dynamic_cast<const AssignedDataset*>(&o);
    if (!v)
        throw_consistency_error(
            "comparing metadata types",
            std::string("second element claims to be a AssignedDataset, but it is a ") + typeid(&o).name() + " instead");

    core::Time changed, vchanged;
    std::string name, id, vname, vid;
    get(changed, name, id);
    v->get(vchanged, vname, vid);

    if (name < vname) return -1;
    if (name > vname) return 1;
    if (id < vid) return -1;
    if (id > vid) return 1;
    return 0;
}

void AssignedDataset::get(core::Time& changed, std::string& name, std::string& id) const
{
    core::BinaryDecoder dec(data, size);
    changed = Time::decode(dec);
    size_t name_len = dec.pop_uint(1, "length of dataset name");
    name = dec.pop_string(name_len, "dataset name");
    size_t id_len = dec.pop_uint(2, "length of dataset id");
    id = dec.pop_string(id_len, "dataset id");
}

std::unique_ptr<AssignedDataset> AssignedDataset::decode(core::BinaryDecoder& dec, bool reuse_buffer)
{
    dec.ensure_size(3, "Assigneddataset data");
    std::unique_ptr<AssignedDataset> res;
    if (reuse_buffer)
        res.reset(new AssignedDataset(dec.buf, dec.size, false));
    else
        res.reset(new AssignedDataset(dec.buf, dec.size));
    dec.skip(dec.size);
    return res;
}

std::ostream& AssignedDataset::writeToOstream(std::ostream& o) const
{
    core::Time changed, vchanged;
    std::string name, id, vname, vid;
    get(changed, name, id);
    return o << name << " as " << id << " imported on " << changed;
}

void AssignedDataset::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    core::Time changed, vchanged;
    std::string name, id, vname, vid;
    get(changed, name, id);
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

std::unique_ptr<AssignedDataset> AssignedDataset::create(const std::string& name, const std::string& id)
{
    return create(Time::create_now(), name, id);
}

std::unique_ptr<AssignedDataset> AssignedDataset::create(const Time& changed, const std::string& name, const std::string& id)
{
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    changed.encodeWithoutEnvelope(enc);
    enc.add_unsigned(name.size(), 1); enc.add_raw(name);
    enc.add_unsigned(id.size(), 2); enc.add_raw(id);
    return std::unique_ptr<AssignedDataset>(new AssignedDataset(buf));
}

void AssignedDataset::init()
{
    MetadataType::register_type<AssignedDataset>();
}

}
}
