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

#if 0
struct run_ : public MethKwargs<run_, arkipy_cfgSections>
{
    constexpr static const char* name = "run";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-mergeconf";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "args", nullptr };
        PyObject* pyargs = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &pyargs))
            return nullptr;

        if (!PySequence_Check(pyargs))
        {
            PyErr_SetString(PyExc_TypeError, "args argument must be a sequence of strings");
            throw PythonException();
        }

        // Unpack a sequence of strings into argc/argv
        unsigned argc = PySequence_Size(pyargs);
        std::vector<const char*> argv;
        argv.reserve(argc + 1);
        for (unsigned i = 0; i < argc; ++i)
        {
            pyo_unique_ptr o(throw_ifnull(PySequence_ITEM(pyargs, i)));
            argv.emplace_back(from_python<const char*>(o));
        }
        argv.emplace_back(nullptr);

        try {
            int res;
            {
                ReleaseGIL rg;
                res = self->arki_mergeconf->run(argc, argv.data());
            }
            return throw_ifnull(PyLong_FromLong(res));
        } ARKI_CATCH_RETURN_PYO
    }
};
#endif


struct SectionsDef : public Type<SectionsDef, arkipy_cfgSections>
{
    constexpr static const char* name = "Sections";
    constexpr static const char* qual_name = "arkimet.cfg.Sections";
    constexpr static const char* doc = R"(
Arkimet configuration, as multiple sections of key/value options
)";
    GetSetters<> getsetters;
    Methods<> methods;

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
        std::string k = from_python<std::string>(key);
        // TODO: lookup
        return PyErr_Format(PyExc_KeyError, "section not found: '%s'", k.c_str());
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
        std::string k = from_python<std::string>(key);
        // TODO: lookup
        return PyErr_Format(PyExc_KeyError, "section not found: '%s'", k.c_str());
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

