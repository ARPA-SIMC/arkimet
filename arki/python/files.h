#ifndef ARKI_PYTHON_FILES_H
#define ARKI_PYTHON_FILES_H

#include "utils/core.h"
#include <arki/core/file.h>
#include <arki/stream/fwd.h>

extern "C" {
/// arki.File python object
typedef struct
{
    PyObject_HEAD arki::utils::sys::NamedFileDescriptor fd;
} arkipy_File;
extern PyTypeObject* arkipy_File_Type;
#define arkipy_File_Check(ob)                                                  \
    (Py_TYPE(ob) == arkipy_File_Type ||                                        \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_File_Type))
}

namespace arki::python {

template <typename AbstractFile> struct FileBase
{
    AbstractFile* abstract        = nullptr;
    core::NamedFileDescriptor* fd = nullptr;

    FileBase()                           = default;
    FileBase(const FileBase&)            = delete;
    FileBase(FileBase&&)                 = delete;
    FileBase& operator=(const FileBase&) = delete;
    FileBase& operator=(FileBase&&)      = delete;
    ~FileBase()
    {
        delete abstract;
        delete fd;
    }

    std::shared_ptr<AbstractFile> detach_abstract()
    {
        std::shared_ptr<AbstractFile> res(abstract);
        abstract = nullptr;
        return res;
    }

    std::shared_ptr<core::NamedFileDescriptor> detach_fd()
    {
        std::shared_ptr<core::NamedFileDescriptor> res(fd);
        fd = nullptr;
        return res;
    }
};

/// Turn a python object into an open input file
struct TextInputFile : public FileBase<core::AbstractInputFile>
{
    explicit TextInputFile(PyObject* o, const std::filesystem::path& name_hint =
                                            std::filesystem::path()

    );
};

/// Turn a python object into an open input file
struct BinaryInputFile : public FileBase<core::AbstractInputFile>
{
    explicit BinaryInputFile(
        PyObject* o,
        const std::filesystem::path& name_hint = std::filesystem::path());
};

/**
 * Wrap a python TextIO into a StreamOutput
 */
std::unique_ptr<StreamOutput> textio_stream_output(PyObject* o);

/**
 * Wrap a python BinaryIO into a StreamOutput
 */
std::unique_ptr<StreamOutput> binaryio_stream_output(PyObject* o);

/**
 * Create a Python proxy for a NamedFileDescriptor
 */
PyObject* file_to_python(arki::utils::sys::NamedFileDescriptor& fd);

void register_files(PyObject* m);

} // namespace arki::python

#endif
