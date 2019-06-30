#include "cfg.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"

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
            arki::core::cfg::Section* res = self->sections.section(name);
            if (!res)
                Py_RETURN_NONE;
            return cfg_section_reference((PyObject*)self, res);
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
            arki::core::cfg::Section& res = self->sections.obtain(name);
            return cfg_section_reference((PyObject*)self, &res);
        } ARKI_CATCH_RETURN_PYO
    }
};

// TODO: Section write
// TODO: Section parse
// TODO: Sections write
// TODO: Sections parse
// TODO: Sections as_configparser
// TODO: Section as_dict
// TODO: Sections constructor with configparser
// TODO: Section constructor with dict
// TODO: Section constructor with sequence of (key, value)


struct SectionsDef : public Type<SectionsDef, arkipy_cfgSections>
{
    constexpr static const char* name = "Sections";
    constexpr static const char* qual_name = "arkimet.cfg.Sections";
    constexpr static const char* doc = R"(
Arkimet configuration, as multiple sections of key/value options
)";
    GetSetters<> getsetters;
    Methods<section, obtain> methods;

    static void _dealloc(Impl* self)
    {
        self->sections.~Sections();
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

    static Py_ssize_t mp_length(Impl* self)
    {
        return self->sections.size();
    }

    static PyObject* mp_subscript(Impl* self, PyObject* key)
    {
        try {
            std::string name = from_python<std::string>(key);
            arki::core::cfg::Section* res = self->sections.section(name);
            if (res)
                return cfg_section_reference((PyObject*)self, res);
            return PyErr_Format(PyExc_KeyError, "section not found: '%s'", name.c_str());
        } ARKI_CATCH_RETURN_PYO
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { nullptr };
        if (!PyArg_ParseTupleAndKeywords(args, kw, "", const_cast<char**>(kwlist)))
            return -1;

        try {
            new(&(self->sections)) arki::core::cfg::Sections();
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

SectionsDef* sections_def = nullptr;


struct SectionDef : public Type<SectionDef, arkipy_cfgSection>
{
    constexpr static const char* name = "Section";
    constexpr static const char* qual_name = "arkimet.cfg.Section";
    constexpr static const char* doc = R"(
Arkimet configuration, as a section of key/value options
)";
    GetSetters<> getsetters;
    Methods<> methods;

    static void _dealloc(Impl* self)
    {
        if (self->owner)
            Py_DECREF(self->owner);
        else
            delete self->section;
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

    static Py_ssize_t mp_length(Impl* self)
    {
        return self->section->size();
    }

    static PyObject* mp_subscript(Impl* self, PyObject* key)
    {
        try {
            std::string k = from_python<std::string>(key);
            if (!self->section->has(k))
                return PyErr_Format(PyExc_KeyError, "section not found: '%s'", k.c_str());
            else
                return to_python(self->section->value(k));
        } ARKI_CATCH_RETURN_PYO
    }

    static int mp_ass_subscript(Impl* self, PyObject *key, PyObject *val)
    {
        try {
            std::string k = from_python<std::string>(key);
            self->section->set(k, from_python<std::string>(val));
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
                        if (self->section == ((Impl*)other)->section)
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
        static const char* kwlist[] = { nullptr };
        if (!PyArg_ParseTupleAndKeywords(args, kw, "", const_cast<char**>(kwlist)))
            return -1;

        try {
            self->owner = nullptr;
            self->section = new arki::core::cfg::Section;
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
    "Arkimet configuration infrastructure",  /* m_doc */
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

PyObject* cfg_section_reference(PyObject* owner, core::cfg::Section* section)
{
    py_unique_ptr<arkipy_cfgSection> res(throw_ifnull(PyObject_New(arkipy_cfgSection, arkipy_cfgSection_Type)));
    res->owner = owner;
    res->section = section;
    Py_INCREF(owner);
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

