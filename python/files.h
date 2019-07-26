#ifndef ARKI_PYTHON_FILES_H
#define ARKI_PYTHON_FILES_H

#include "utils/core.h"
#include <arki/core/file.h>

namespace arki {
namespace python {

/**
 * Turn a python object into an open input file
 */
struct InputFile
{
    core::AbstractInputFile* abstract = nullptr;
    core::NamedFileDescriptor* fd = nullptr;

    InputFile(PyObject* o);
    InputFile(const InputFile&) = delete;
    InputFile(InputFile&&) = delete;
    InputFile& operator=(const InputFile&) = delete;
    InputFile& operator=(InputFile&&) = delete;
    ~InputFile();

    // void close();
};


/**
 * Turn a python object into an open input file
 */
struct OutputFile
{
    core::AbstractOutputFile* abstract = nullptr;
    core::NamedFileDescriptor* fd = nullptr;

    OutputFile(PyObject* o);
    OutputFile(const OutputFile&) = delete;
    OutputFile(OutputFile&&) = delete;
    OutputFile& operator=(const OutputFile&) = delete;
    OutputFile& operator=(OutputFile&&) = delete;
    ~OutputFile();

    // void close();
};

}
}

#endif
