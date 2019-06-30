#include "arki/runtime/arki-mergeconf.h"
#include "arki-mergeconf.h"
#include "cfg.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"

using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_ArkiMergeconf_Type = nullptr;

}


namespace {

struct run_ : public MethKwargs<run_, arkipy_ArkiMergeconf>
{
    constexpr static const char* name = "run";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-mergeconf";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "sections", "sources", "cfgfiles", "restrict", "ignore_system_datasets", "extra", nullptr };
        arkipy_cfgSections* sections = nullptr;
        PyObject* sources = nullptr;
        PyObject* cfgfiles = nullptr;
        const char* restrict = nullptr;
        int ignore_system_ds = 0;
        int extra = 0;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "O!O|Ozpp", const_cast<char**>(kwlist),
                    arkipy_cfgSections_Type, &sections, &sources, &cfgfiles,
                    &restrict, &ignore_system_ds, &extra))
            return nullptr;

        try {
            self->arki_mergeconf->sources = from_python<std::vector<std::string>>(sources);

            if (cfgfiles && cfgfiles != Py_None)
                self->arki_mergeconf->cfgfiles = from_python<std::vector<std::string>>(cfgfiles);
            else
                self->arki_mergeconf->cfgfiles.clear();

            if (restrict)
            {
                self->arki_mergeconf->restrict = true;
                self->arki_mergeconf->restrict_expr = restrict;
            } else {
                self->arki_mergeconf->restrict = false;
            }

            self->arki_mergeconf->ignore_system_ds = ignore_system_ds;
            self->arki_mergeconf->extra = extra;

            {
                ReleaseGIL rg;
                self->arki_mergeconf->run(sections->sections);
            }

            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};


struct ArkiMergeconfDef : public Type<ArkiMergeconfDef, arkipy_ArkiMergeconf>
{
    constexpr static const char* name = "ArkiMergeconf";
    constexpr static const char* qual_name = "arkimet.ArkiMergeconf";
    constexpr static const char* doc = R"(
arki-mergeconf implementation
)";
    GetSetters<> getsetters;
    Methods<run_> methods;

    static void _dealloc(Impl* self)
    {
        delete self->arki_mergeconf;
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

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { nullptr };
        if (!PyArg_ParseTupleAndKeywords(args, kw, "", const_cast<char**>(kwlist)))
            return -1;

        try {
            self->arki_mergeconf = new arki::runtime::ArkiMergeconf;
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

ArkiMergeconfDef* arki_mergeconf_def = nullptr;


}

namespace arki {
namespace python {

void register_arki_mergeconf(PyObject* m)
{
    arki_mergeconf_def = new ArkiMergeconfDef;
    arki_mergeconf_def->define(arkipy_ArkiMergeconf_Type, m);

}

}
}
