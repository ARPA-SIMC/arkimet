#ifndef ARKI_PYTHON_FILES_H
#define ARKI_PYTHON_FILES_H

#include "utils/core.h"
#include <arki/core/file.h>

namespace arki {
namespace python {

template<typename AbstractFile>
struct FileBase
{
    AbstractFile* abstract = nullptr;
    core::NamedFileDescriptor* fd = nullptr;

    FileBase() = default;
    FileBase(const FileBase&) = delete;
    FileBase(FileBase&&) = delete;
    FileBase& operator=(const FileBase&) = delete;
    FileBase& operator=(FileBase&&) = delete;
    ~FileBase()
    {
        delete abstract;
        delete fd;
    }

    std::shared_ptr<AbstractFile> detach_abstract()
    {
        std::shared_ptr<core::AbstractOutputFile> res(abstract);
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
    TextInputFile(PyObject* o);
};


/// Turn a python object into an open input file
struct BinaryInputFile : public FileBase<core::AbstractInputFile>
{
    BinaryInputFile(PyObject* o);
};


/// Turn a python object into an open input file
struct TextOutputFile : public FileBase<core::AbstractOutputFile>
{
    TextOutputFile(PyObject* o);
};


/// Turn a python object into an open input file
struct BinaryOutputFile : public FileBase<core::AbstractOutputFile>
{
    BinaryOutputFile(PyObject* o);
};

}
}

#endif
