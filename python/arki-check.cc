#include "arki/runtime/arki-check.h"
#include "arki/core/cfg.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/dataset.h"
#include "arki/datasets.h"
#include "arki/nag.h"
#include "arki-check.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"
#include <sstream>
#include <iostream>

using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_ArkiCheck_Type = nullptr;

}


namespace {

struct remove : public MethKwargs<remove, arkipy_ArkiCheck>
{
    constexpr static const char* name = "remove";
    constexpr static const char* signature = "config: arkimet.cfg.Sections, metadata_file: str, fix: bool=False";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "run arki-check --remove=metadata_file";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "config", "metadata_file", "fix", nullptr };
        PyObject* arg_config = nullptr;
        const char* arg_metadata_file = nullptr;
        Py_ssize_t arg_metadata_file_len;
        int arg_fix = 0;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "Os#|p", const_cast<char**>(kwlist),
                    &arg_config, &arg_metadata_file, &arg_metadata_file_len, &arg_fix))
            return nullptr;

        try {
            auto config = sections_from_python(arg_config);
            std::string metadata_file(arg_metadata_file, arg_metadata_file_len);

            {
                ReleaseGIL rg;
                arki::Datasets datasets(config);
                arki::WriterPool pool(datasets);
                // Read all metadata from the file specified in --remove
                arki::metadata::Collection todolist;
                todolist.read_from_file(metadata_file);
                // Datasets where each metadata comes from
                std::vector<std::string> dsnames;
                // Verify that all metadata items can be mapped to a dataset
                std::map<std::string, unsigned> counts;
                unsigned idx = 1;
                for (const auto& md: todolist)
                {
                    if (!md->has_source_blob())
                    {
                        std::stringstream ss;
                        ss << "cannot remove data #" << idx << ": metadata does not come from an on-disk dataset";
                        throw std::runtime_error(ss.str());
                    }

                    auto ds = datasets.locate_metadata(*md);
                    if (!ds)
                    {
                        std::stringstream ss;
                        ss << "cannot remove data #" << idx << " is does not come from any known dataset";
                        throw std::runtime_error(ss.str());
                    }

                    dsnames.push_back(ds->name);
                    ++counts[ds->name];
                    ++idx;
                }
                if (arg_fix)
                {
                    // Perform removals
                    idx = 1;
                    for (unsigned i = 0; i < todolist.size(); ++i)
                    {
                        auto ds = pool.get(dsnames[i]);
                        try {
                            ds->remove(todolist[i]);
                        } catch (std::exception& e) {
                            std::cerr << "Cannot remove message #" << idx << ": " << e.what() << std::endl;
                        }
                    }
                    for (const auto& i: counts)
                        arki::nag::verbose("%s: %u data deleted", i.first.c_str(), i.second);
                } else {
                    for (const auto& i: counts)
                        printf("%s: %u data would be deleted\n", i.first.c_str(), i.second);
                }
                fflush(stdout);
                fflush(stderr);
            }
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct run_ : public MethKwargs<run_, arkipy_ArkiCheck>
{
    constexpr static const char* name = "run";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-check";
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
                res = self->arki_check->run(argc, argv.data());
            }
            return throw_ifnull(PyLong_FromLong(res));
        } ARKI_CATCH_RETURN_PYO
    }
};


struct ArkiCheckDef : public Type<ArkiCheckDef, arkipy_ArkiCheck>
{
    constexpr static const char* name = "ArkiCheck";
    constexpr static const char* qual_name = "arkimet.ArkiCheck";
    constexpr static const char* doc = R"(
arki-check implementation
)";
    GetSetters<> getsetters;
    Methods<remove, run_> methods;

    static void _dealloc(Impl* self)
    {
        delete self->arki_check;
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
            self->arki_check = new arki::runtime::ArkiCheck;
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

ArkiCheckDef* arki_check_def = nullptr;


}

namespace arki {
namespace python {

void register_arki_check(PyObject* m)
{
    arki_check_def = new ArkiCheckDef;
    arki_check_def->define(arkipy_ArkiCheck_Type, m);

}

}
}
