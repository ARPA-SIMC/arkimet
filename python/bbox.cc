#include "bbox.h"
#include "structured.h"
#include "arki/bbox.h"
#include "arki/types.h"
#include "arki/runtime.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/geos.h"
#include "arki/nag.h"
#include "python/utils/values.h"
#include "python/utils/methods.h"
#include "python/utils/type.h"
#include "python/common.h"

using namespace arki::utils;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_BBox_Type = nullptr;

}

namespace {

PyObject* bbox_object = nullptr;

void load_bbox_object()
{
    using namespace arki;
    std::vector<std::string> sources = arki::Config::get().dir_bbox.list_files(".py");
    for (const auto& source: sources)
    {
        std::string basename = str::basename(source);

        // Check if the bbox module had already been imported
        std::string module_name = "arkimet.bbox." + basename.substr(0, basename.size() - 3);
        pyo_unique_ptr py_module_name(string_to_python(module_name));
        pyo_unique_ptr module(ArkiPyImport_GetModule(py_module_name));
        if (PyErr_Occurred())
            throw PythonException();

        // Import it if needed
        if (!module)
        {
            std::string source_code = utils::sys::read_file(source);
            pyo_unique_ptr code(throw_ifnull(Py_CompileStringExFlags(
                            source_code.c_str(), source.c_str(),
                            Py_file_input, nullptr, -1)));
            module = pyo_unique_ptr(throw_ifnull(PyImport_ExecCodeModule(
                            module_name.c_str(), code)));
        }
    }

    // Get arkimet.bbox.BBox
    pyo_unique_ptr module(throw_ifnull(PyImport_ImportModule("arkimet.bbox")));
    pyo_unique_ptr cls(throw_ifnull(PyObject_GetAttrString(module, "BBox")));
    pyo_unique_ptr obj(throw_ifnull(PyObject_CallFunction(cls, nullptr)));

    // Hold a reference to arki.python.BBox forever once loaded the first time
    bbox_object = obj.release();
}


std::pair<double, double> get_coord_pair(PyObject* obj)
{
    Py_ssize_t size = PyTuple_Size(obj);
    if (size != 2)
        throw std::invalid_argument("python bbox function did not return a list of coordinate pairs");
    PyObject* first = PyTuple_GET_ITEM(obj, 0); // Borrowed reference
    PyObject* second = PyTuple_GET_ITEM(obj, 1); // Borrowed reference
    return std::make_pair(from_python<double>(first), from_python<double>(second));
}


struct PythonBBox : public arki::BBox
{
    arki::utils::geos::Geometry compute(const arki::types::Area& v) const override
    {
        AcquireGIL gil;
        if (!bbox_object)
            load_bbox_object();

        arki::python::PythonEmitter e;
        v.serialise(e, arki::structured::keys_python);

        pyo_unique_ptr obj(PyObject_CallMethod(
                        bbox_object, "compute", "O", e.res.get()));

        if (!obj)
        {
            PyObject *type, *value, *traceback;
            PyErr_Fetch(&type, &value, &traceback);
            pyo_unique_ptr exc_type(type);
            pyo_unique_ptr exc_value(value);
            pyo_unique_ptr exc_traceback(traceback);

            arki::nag::debug_tty("python bbox failed: %s", exc_type.str().c_str());
            arki::nag::warning("python bbox failed: %s", exc_type.str().c_str());
            return arki::utils::geos::Geometry();
        }

        if (obj == Py_None)
            return arki::utils::geos::Geometry();

        Py_ssize_t size = PyList_Size(obj);
        if (size == -1)
            throw PythonException();

        switch (size)
        {
            case 0: return arki::utils::geos::Geometry();
            case 1:
            {
                PyObject* pair = PyList_GET_ITEM(obj.get(), 0); // Borrowed reference
                auto point = get_coord_pair(pair);
                return arki::utils::geos::Geometry::create_point(point.first, point.second);
            }
            default:
            {
                arki::utils::geos::CoordinateSequence seq(size);
                for (Py_ssize_t i = 0; i < size; ++i)
                {
                    PyObject* pair = PyList_GET_ITEM(obj.get(), i); // Borrowed reference
                    auto point = get_coord_pair(pair);
                    seq.setxy(i, point.second, point.first);
                }
                arki::utils::geos::Geometry linear_ring(arki::utils::geos::Geometry::create_linear_ring(std::move(seq)));
                return arki::utils::geos::Geometry::create_polygon(std::move(linear_ring), arki::utils::geos::GeometryVector());
            }
        }
    }
};


struct compute : public MethKwargs<compute, arkipy_BBox>
{
    constexpr static const char* name = "compute";
    constexpr static const char* signature = "type: Union[Dict[str, Any], str]";
    constexpr static const char* returns = "str"; // TODO
    constexpr static const char* summary = "compute the bounding box for the given area";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "type", nullptr };
        PyObject* py_type = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &py_type))
            return nullptr;

        try {
            std::unique_ptr<arki::types::Type> type;
            if (PyUnicode_Check(py_type))
            {
                std::string strval = from_python<std::string>(py_type);
                type = arki::types::decodeString(arki::TYPE_AREA, strval);
            } else {
                PythonReader reader(py_type);
                type = arki::types::decode_structure(arki::structured::keys_python, reader);
            }
            std::unique_ptr<arki::types::Area> area = arki::downcast<arki::types::Area>(std::move(type));
            auto hull = self->bbox->compute(*area);
            if (!hull)
                Py_RETURN_NONE;
            arki::utils::geos::WKTWriter writer;
            return to_python(writer.write(hull));
        } ARKI_CATCH_RETURN_PYO
    }
};


struct BBoxDef : public Type<BBoxDef, arkipy_BBox>
{
    constexpr static const char* name = "BBox";
    constexpr static const char* qual_name = "arkimet.BBox";
    constexpr static const char* doc = R"(
BBox for arkimet metadata.
)";
    GetSetters<> getsetters;
    Methods<compute> methods;

    static void _dealloc(Impl* self)
    {
        delete self->bbox;
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromString("BBox");
    }

    static PyObject* _repr(Impl* self)
    {
        return PyUnicode_FromString("BBox");
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { nullptr };
        if (!PyArg_ParseTupleAndKeywords(args, kw, "", const_cast<char**>(kwlist)))
            return -1;

        try {
            self->bbox = arki::BBox::create().release();
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

BBoxDef* bbox_def = nullptr;

}

namespace arki {
namespace python {

void register_bbox(PyObject* m)
{
    bbox_def = new BBoxDef;
    bbox_def->define(arkipy_BBox_Type, m);
}

namespace bbox {

void init()
{
    arki::BBox::set_factory([] {
        return std::unique_ptr<arki::BBox>(new PythonBBox);
    });
}

}
}
}

