#include "cfg.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "arki/utils/files.h"
#include "arki/core/file.h"
#include "arki/stream.h"
#include "common.h"
#include "files.h"

using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_cfgSections_Type = nullptr;
PyTypeObject* arkipy_cfgSection_Type = nullptr;

}


namespace {

struct section : public MethKwargs<section, arkipy_cfgSections>
{
    constexpr static const char* name = "section";
    constexpr static const char* signature = "Str";
    constexpr static const char* returns = "Optional[arki.cfg.Section]";
    constexpr static const char* summary = "return the named section, if it exists, or None if it does not";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "name", nullptr };
        const char* arg_name = nullptr;
        Py_ssize_t arg_name_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist), &arg_name, &arg_name_len))
            return nullptr;

        try {
            std::string name(arg_name, arg_name_len);
            auto res = self->ptr->section(name);
            if (!res)
                Py_RETURN_NONE;
            return to_python(res);
        } ARKI_CATCH_RETURN_PYO
    }
};

struct obtain : public MethKwargs<obtain, arkipy_cfgSections>
{
    constexpr static const char* name = "obtain";
    constexpr static const char* signature = "Str";
    constexpr static const char* returns = "arki.cfg.Section";
    constexpr static const char* summary = "return the named section, creating it if it does not exist";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "name", nullptr };
        const char* arg_name = nullptr;
        Py_ssize_t arg_name_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist), &arg_name, &arg_name_len))
            return nullptr;

        try {
            std::string name(arg_name, arg_name_len);
            auto res = self->ptr->obtain(name);
            return to_python(res);
        } ARKI_CATCH_RETURN_PYO
    }
};

struct sections_get : public MethKwargs<sections_get, arkipy_cfgSections>
{
    constexpr static const char* name = "get";
    constexpr static const char* signature = "name: str, default: Optional[Any]=None";
    constexpr static const char* returns = "Union[arki.cfg.Section, Any]";
    constexpr static const char* summary = "return the named section, or the given default value if it does not exist";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "name", "default", nullptr };
        const char* arg_name = nullptr;
        Py_ssize_t arg_name_len;
        PyObject* arg_default = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#|O", const_cast<char**>(kwlist), &arg_name, &arg_name_len, &arg_default))
            return nullptr;

        try {
            std::string name(arg_name, arg_name_len);
            auto res = self->ptr->section(name);
            if (res)
                return to_python(res);
            if (!arg_default)
                Py_RETURN_NONE;
            else
            {
                Py_INCREF(arg_default);
                return arg_default;
            }
        } ARKI_CATCH_RETURN_PYO
    }
};

struct sections_keys : public MethNoargs<sections_keys, arkipy_cfgSections>
{
    constexpr static const char* name = "keys";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "Iterable[str]";
    constexpr static const char* summary = "Iterate over section names";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try {
            pyo_unique_ptr res(throw_ifnull(PyTuple_New(self->ptr->size())));
            unsigned pos = 0;
            for (auto& si: *(self->ptr))
            {
                pyo_unique_ptr key(to_python(si.first));
                PyTuple_SET_ITEM(res.get(), pos, key.release());
                ++pos;
            }
            return res.release();
        } ARKI_CATCH_RETURN_PYO
    }
};

struct sections_items : public MethNoargs<sections_items, arkipy_cfgSections>
{
    constexpr static const char* name = "items";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "Iterable[Tuple[str, arki.cfg.Section]]";
    constexpr static const char* summary = "Iterate over section names and sections";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try {
            pyo_unique_ptr res(throw_ifnull(PyTuple_New(self->ptr->size())));
            unsigned pos = 0;
            for (auto& si: *(self->ptr))
            {
                pyo_unique_ptr key(to_python(si.first));
                pyo_unique_ptr val(to_python(si.second));
                pyo_unique_ptr pair(throw_ifnull(PyTuple_Pack(2, key.get(), val.get())));
                PyTuple_SET_ITEM(res.get(), pos, pair.release());
                ++pos;
            }
            return res.release();
        } ARKI_CATCH_RETURN_PYO
    }
};


struct section_get : public MethKwargs<section_get, arkipy_cfgSection>
{
    constexpr static const char* name = "get";
    constexpr static const char* signature = "name: str, default: Optional[Any]=None";
    constexpr static const char* returns = "Union[str, Any]";
    constexpr static const char* summary = "return the value for the given key, or the given default value if it does not exist";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "name", "default", nullptr };
        const char* arg_name = nullptr;
        Py_ssize_t arg_name_len;
        PyObject* arg_default = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#|O", const_cast<char**>(kwlist), &arg_name, &arg_name_len, &arg_default))
            return nullptr;

        try {
            std::string name(arg_name, arg_name_len);
            auto i = self->ptr->find(name);
            if (i != self->ptr->end())
                return to_python(i->second);
            if (!arg_default)
                Py_RETURN_NONE;
            else
            {
                Py_INCREF(arg_default);
                return arg_default;
            }
        } ARKI_CATCH_RETURN_PYO
    }
};
struct section_keys : public MethNoargs<section_keys, arkipy_cfgSection>
{
    constexpr static const char* name = "keys";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "Iterable[str]";
    constexpr static const char* summary = "Iterate over key names";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try {
            pyo_unique_ptr res(throw_ifnull(PyTuple_New(self->ptr->size())));
            unsigned pos = 0;
            for (auto& si: *self->ptr)
            {
                pyo_unique_ptr key(to_python(si.first));
                PyTuple_SET_ITEM(res.get(), pos, key.release());
                ++pos;
            }
            return res.release();
        } ARKI_CATCH_RETURN_PYO
    }
};

struct section_items : public MethNoargs<section_items, arkipy_cfgSection>
{
    constexpr static const char* name = "items";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "Iterable[Tuple[str, str]]";
    constexpr static const char* summary = "Iterate over key/value pairs";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try {
            pyo_unique_ptr res(throw_ifnull(PyTuple_New(self->ptr->size())));
            unsigned pos = 0;
            for (auto& si: *self->ptr)
            {
                pyo_unique_ptr key(to_python(si.first));
                pyo_unique_ptr val(to_python(si.second));
                pyo_unique_ptr pair(throw_ifnull(PyTuple_Pack(2, key.get(), val.get())));
                PyTuple_SET_ITEM(res.get(), pos, pair.release());
                ++pos;
            }
            return res.release();
        } ARKI_CATCH_RETURN_PYO
    }
};


struct parse_sections : public ClassMethKwargs<parse_sections>
{
    constexpr static const char* name = "parse";
    constexpr static const char* signature = "Union[Str, TextIO]";
    constexpr static const char* returns = "arki.cfg.Sections";
    constexpr static const char* summary = "parse the named file or open file, and return the resulting Sections object";
    constexpr static const char* doc = nullptr;

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "input", nullptr };
        PyObject* arg_input = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &arg_input))
            return nullptr;

        try {
            if (PyUnicode_Check(arg_input))
            {
                std::string fname = from_python<std::string>(arg_input);
                arki::utils::sys::File in(fname, O_RDONLY);
                auto parsed = arki::core::cfg::Sections::parse(in);
                // If string, open file and parse it
                py_unique_ptr<arkipy_cfgSections> res(throw_ifnull(PyObject_New(arkipy_cfgSections, arkipy_cfgSections_Type)));
                new (&(res->ptr)) std::shared_ptr<arki::core::cfg::Sections>(parsed);
                return (PyObject*)res.release();
            } else {
                // Else iterator or TypeError
                auto reader = linereader_from_python(arg_input);
                auto parsed = arki::core::cfg::Sections::parse(*reader, "python object");
                py_unique_ptr<arkipy_cfgSections> res(throw_ifnull(PyObject_New(arkipy_cfgSections, arkipy_cfgSections_Type)));
                new (&(res->ptr)) std::shared_ptr<arki::core::cfg::Sections>(parsed);
                return (PyObject*)res.release();
            }
        } ARKI_CATCH_RETURN_PYO
    }
};

struct parse_section : public ClassMethKwargs<parse_section>
{
    constexpr static const char* name = "parse";
    constexpr static const char* signature = "Union[Str, TextIO]";
    constexpr static const char* returns = "arki.cfg.Section";
    constexpr static const char* summary = "parse the named file or open file, and return the resulting Section object";
    constexpr static const char* doc = nullptr;

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "input", nullptr };
        PyObject* arg_input = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &arg_input))
            return nullptr;

        try {
            if (PyUnicode_Check(arg_input))
            {
                std::string fname = from_python<std::string>(arg_input);
                arki::utils::sys::File in(fname, O_RDONLY);
                auto parsed = arki::core::cfg::Section::parse(in);
                // If string, open file and parse it
                py_unique_ptr<arkipy_cfgSection> res(throw_ifnull(PyObject_New(arkipy_cfgSection, arkipy_cfgSection_Type)));
                new (&(res->ptr)) std::shared_ptr<arki::core::cfg::Section>(parsed);
                return (PyObject*)res.release();
            } else {
                // Else iterator or TypeError
                auto reader = linereader_from_python(arg_input);
                auto parsed = arki::core::cfg::Section::parse(*reader, "python object");
                py_unique_ptr<arkipy_cfgSection> res(throw_ifnull(PyObject_New(arkipy_cfgSection, arkipy_cfgSection_Type)));
                new (&(res->ptr)) std::shared_ptr<arki::core::cfg::Section>(parsed);
                return (PyObject*)res.release();
            }
        } ARKI_CATCH_RETURN_PYO
    }
};

struct write_sections : public MethKwargs<write_sections, arkipy_cfgSections>
{
    constexpr static const char* name = "write";
    constexpr static const char* signature = "TextIO";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "write the configuration to any object with a write method";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "file", nullptr };
        PyObject* arg_file = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &arg_file))
            return nullptr;

        try {
            auto out = textio_stream_output(arg_file);
            auto s = self->ptr->to_string();
            out->send_buffer(s.data(), s.size());
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct write_section : public MethKwargs<write_section, arkipy_cfgSection>
{
    constexpr static const char* name = "write";
    constexpr static const char* signature = "TextIO";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "write the configuration to any object with a write method";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "file", nullptr };
        PyObject* arg_file = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &arg_file))
            return nullptr;

        try {
            auto out = textio_stream_output(arg_file);
            auto s = self->ptr->to_string();
            out->send_buffer(s.data(), s.size());
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

template<typename Base, typename Impl>
struct MethCopy : public MethNoargs<Base, Impl>
{
    constexpr static const char* name = "copy";
    constexpr static const char* signature = "";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try {
            return to_python(std::make_shared<typename Impl::value_type>(*self->ptr));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct copy_sections : public MethCopy<copy_sections, arkipy_cfgSections>
{
    constexpr static const char* returns = "arkimet.cfg.Sections";
    constexpr static const char* summary = "return a deep copy of this Sections object";
};

struct copy_section : public MethCopy<copy_section, arkipy_cfgSection>
{
    constexpr static const char* returns = "arkimet.cfg.Section";
    constexpr static const char* summary = "return a deep copy of this Section object";
};


struct SectionsDef : public Type<SectionsDef, arkipy_cfgSections>
{
    constexpr static const char* name = "Sections";
    constexpr static const char* qual_name = "arkimet.cfg.Sections";
    constexpr static const char* doc = R"(
Arkimet configuration, as multiple sections of key/value options
)";
    GetSetters<> getsetters;
    Methods<section, obtain, sections_get, sections_keys, sections_items, parse_sections, write_sections, copy_sections> methods;

    static void _dealloc(Impl* self)
    {
        self->ptr.~shared_ptr<arki::core::cfg::Sections>();
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromString(name);
    }

    static PyObject* _repr(Impl* self)
    {
        std::string res = qual_name;
        res += " object";
        return PyUnicode_FromString(res.c_str());
    }

    static int sq_contains(Impl *self, PyObject *value)
    {
        try {
            std::string key = from_python<std::string>(value);
            return (self->ptr->find(key) != self->ptr->end()) ? 1 : 0;
        } ARKI_CATCH_RETURN_INT
    }

    static PyObject* _iter(Impl* self)
    {
        py_unique_ptr<PyTupleObject> res((PyTupleObject*)PyTuple_New(self->ptr->size()));
        unsigned pos = 0;
        for (const auto& si: *(self->ptr))
            PyTuple_SET_ITEM(res.get(), pos++, to_python(si.first));
        return PyObject_GetIter((PyObject*)res.get());
    }

    static Py_ssize_t mp_length(Impl* self)
    {
        return self->ptr->size();
    }

    static PyObject* mp_subscript(Impl* self, PyObject* key)
    {
        try {
            std::string name = from_python<std::string>(key);
            auto res = self->ptr->section(name);
            if (res)
                return to_python(res);
            return PyErr_Format(PyExc_KeyError, "section not found: '%s'", name.c_str());
        } ARKI_CATCH_RETURN_PYO
    }

    static int mp_ass_subscript(Impl* self, PyObject *key, PyObject *val)
    {
        try {
            auto name = from_python<std::string>(key);
            if (!val)
            {
                auto i = self->ptr->find(name);
                if (i == self->ptr->end())
                {
                    PyErr_Format(PyExc_KeyError, "section not found: '%s'", name.c_str());
                    return -1;
                }
                self->ptr->erase(i);
            } else {
                std::string k = from_python<std::string>(key);
                auto i = self->ptr->find(k);
                if (i == self->ptr->end())
                    self->ptr->emplace(k, section_from_python(val));
                else
                    i->second = section_from_python(val);
            }
            return 0;
        } ARKI_CATCH_RETURN_INT
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { nullptr };
        if (!PyArg_ParseTupleAndKeywords(args, kw, "", const_cast<char**>(kwlist)))
            return -1;

        try {
            new(&(self->ptr)) std::shared_ptr<arki::core::cfg::Sections>(std::make_shared<arki::core::cfg::Sections>());
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

SectionsDef* sections_def = nullptr;


void fill_section_from_dict(arki::core::cfg::Section& dest, PyObject* src)
{
    PyObject *key, *value;
    Py_ssize_t pos = 0;

    while (PyDict_Next(src, &pos, &key, &value)) {
        dest.set(from_python<std::string>(key), from_python<std::string>(value));
    }
}


struct SectionDef : public Type<SectionDef, arkipy_cfgSection>
{
    constexpr static const char* name = "Section";
    constexpr static const char* qual_name = "arkimet.cfg.Section";
    constexpr static const char* doc = R"(
Arkimet configuration, as a section of key/value options
)";
    GetSetters<> getsetters;
    Methods<section_keys, section_get, section_items, parse_section, write_section, copy_section> methods;

    static void _dealloc(Impl* self)
    {
        self->ptr.~shared_ptr<arki::core::cfg::Section>();
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromString(name);
    }

    static PyObject* _repr(Impl* self)
    {
        std::string res = qual_name;
        res += " object";
        return PyUnicode_FromString(res.c_str());
    }

    static int sq_contains(Impl *self, PyObject *value)
    {
        try {
            std::string key = from_python<std::string>(value);
            return self->ptr->has(key) ? 1 : 0;
        } ARKI_CATCH_RETURN_INT
    }

    static PyObject* _iter(Impl* self)
    {
        py_unique_ptr<PyTupleObject> res((PyTupleObject*)PyTuple_New(self->ptr->size()));
        unsigned pos = 0;
        for (const auto& si: *self->ptr)
            PyTuple_SET_ITEM(res.get(), pos++, to_python(si.first));
        return PyObject_GetIter((PyObject*)res.get());
    }

    static Py_ssize_t mp_length(Impl* self)
    {
        return self->ptr->size();
    }

    static PyObject* mp_subscript(Impl* self, PyObject* key)
    {
        try {
            std::string k = from_python<std::string>(key);
            if (!self->ptr->has(k))
                return PyErr_Format(PyExc_KeyError, "section not found: '%s'", k.c_str());
            else
                return to_python(self->ptr->value(k));
        } ARKI_CATCH_RETURN_PYO
    }

    static int mp_ass_subscript(Impl* self, PyObject *key, PyObject *val)
    {
        try {
            std::string k = from_python<std::string>(key);
            if (!val)
            {
                auto i = self->ptr->find(name);
                if (i == self->ptr->end())
                {
                    PyErr_Format(PyExc_KeyError, "key not found: '%s'", k.c_str());
                    return -1;
                }
                self->ptr->erase(i);
            } else
                self->ptr->set(k, from_python<std::string>(val));
            return 0;
        } ARKI_CATCH_RETURN_INT
    }

    static PyObject* _richcompare(Impl* self, PyObject *other, int op)
    {
        try {
            switch (op)
            {
                case Py_EQ:
                    if (arkipy_cfgSection_Check(other))
                    {
                        if (self->ptr == ((Impl*)other)->ptr)
                            Py_RETURN_TRUE;
                        else
                            Py_RETURN_FALSE;
                    } else
                        return Py_NotImplemented;
                default:
                    return Py_NotImplemented;
            }
        } ARKI_CATCH_RETURN_PYO
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        PyObject* init_dict = nullptr;

        if (PyTuple_GET_SIZE(args) == 1)
        {
            PyObject* first = PyTuple_GET_ITEM(args, 0);
            if (!PyDict_Check(first))
            {
                PyErr_SetString(PyExc_TypeError, "if a positional argument is provided to arkimet.cfg.Section(), it must be a dict");
                return -1;
            }

            init_dict = first;
        } else if (kw && PyDict_Size(kw) > 0) {
            init_dict = kw;
        }

        try {
            int argc = PyTuple_GET_SIZE(args);
            if (argc > 1)
            {
                PyErr_SetString(PyExc_TypeError, "arkimet.cfg.Section() takes at most one positional argument");
                return -1;
            }

            new (&(self->ptr)) std::shared_ptr<arki::core::cfg::Section>(std::make_shared<arki::core::cfg::Section>());

            if (init_dict)
                fill_section_from_dict(*(self->ptr), init_dict);
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

SectionDef* section_def = nullptr;


Methods<> cfg_methods;

}



extern "C" {

static PyModuleDef cfg_module = {
    PyModuleDef_HEAD_INIT,
    "cfg",          /* m_name */
    "Arkimet configuration C functions",  /* m_doc */
    -1,             /* m_size */
    cfg_methods.as_py(),    /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

}

namespace arki {
namespace python {

// TODO: Sections as_configparser
// TODO: Section as_dict
// TODO: Sections constructor with configparser
// TODO: Section constructor with dict
// TODO: Section constructor with sequence of (key, value)

std::shared_ptr<core::cfg::Section> section_from_python(PyObject* o)
{
    try {
        if (arkipy_cfgSection_Check(o)) {
            return ((arkipy_cfgSection*)o)->ptr;
        }

        if (PyBytes_Check(o)) {
            const char* v = throw_ifnull(PyBytes_AsString(o));
            return core::cfg::Section::parse(v);
        }
        if (PyUnicode_Check(o)) {
            const char* v = throw_ifnull(PyUnicode_AsUTF8(o));
            return core::cfg::Section::parse(v);
        }
        if (PyDict_Check(o))
        {
            auto res = std::make_shared<core::cfg::Section>();
            PyObject *key, *val;
            Py_ssize_t pos = 0;
            while (PyDict_Next(o, &pos, &key, &val))
                res->set(string_from_python(key), string_from_python(val));
            return res;
        }
        PyErr_SetString(PyExc_TypeError, "value must be an instance of str, bytes, or dict");
        throw PythonException();
    } ARKI_CATCH_RETHROW_PYTHON
}

std::shared_ptr<core::cfg::Sections> sections_from_python(PyObject* o)
{
    try {
        if (arkipy_cfgSections_Check(o)) {
            return ((arkipy_cfgSections*)o)->ptr;
        }

        if (PyBytes_Check(o)) {
            const char* v = throw_ifnull(PyBytes_AsString(o));
            return core::cfg::Sections::parse(v);
        }
        if (PyUnicode_Check(o)) {
            const char* v = throw_ifnull(PyUnicode_AsUTF8(o));
            return core::cfg::Sections::parse(v);
        }
        PyErr_SetString(PyExc_TypeError, "value must be an instance of str, or bytes");
        throw PythonException();
    } ARKI_CATCH_RETHROW_PYTHON
}


PyObject* sections_to_python(std::shared_ptr<core::cfg::Sections> ptr)
{
    py_unique_ptr<arkipy_cfgSections> res(throw_ifnull(PyObject_New(arkipy_cfgSections, arkipy_cfgSections_Type)));
    new (&(res->ptr)) std::shared_ptr<arki::core::cfg::Sections>(ptr);
    return (PyObject*)res.release();
}

PyObject* section_to_python(std::shared_ptr<core::cfg::Section> ptr)
{
    py_unique_ptr<arkipy_cfgSection> res(throw_ifnull(PyObject_New(arkipy_cfgSection, arkipy_cfgSection_Type)));
    new (&(res->ptr)) std::shared_ptr<arki::core::cfg::Section>(ptr);
    return (PyObject*)res.release();
}

void register_cfg(PyObject* m)
{
    pyo_unique_ptr cfg = throw_ifnull(PyModule_Create(&cfg_module));

    sections_def = new SectionsDef;
    sections_def->define(arkipy_cfgSections_Type, cfg);

    section_def = new SectionDef;
    section_def->define(arkipy_cfgSection_Type, cfg);

    if (PyModule_AddObject(m, "cfg", cfg.release()) == -1)
        throw PythonException();
}

}
}

