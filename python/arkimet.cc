#include <Python.h>
#include "common.h"
#include "metadata.h"
#include "summary.h"
#include "dataset.h"
#include "config.h"

using namespace std;
using namespace arki;
using namespace arki::python;

extern "C" {

static PyMethodDef arkimet_methods[] = {
    { NULL }
};

static PyModuleDef arkimet_module = {
    PyModuleDef_HEAD_INIT,
    "_arkimet",       /* m_name */
    "Arkimet Python interface.",  /* m_doc */
    -1,             /* m_size */
    arkimet_methods, /* m_methods */
    NULL,           /* m_reload */
    NULL,           /* m_traverse */
    NULL,           /* m_clear */
    NULL,           /* m_free */

};

PyMODINIT_FUNC PyInit__arkimet(void)
{
    using namespace arki::python;

    PyObject* m = PyModule_Create(&arkimet_module);

    register_metadata(m);
    register_summary(m);
    register_dataset(m);

    return m;
}

}
