#include "arki/core/cfg.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/dataset.h"
#include "arki/dataset/session.h"
#include "arki/dataset/segmented.h"
#include "arki/dataset/pool.h"
#include "arki/nag.h"
#include "arki-check.h"
#include "dataset/reporter.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "dataset/reporter.h"
#include "common.h"
#include "dataset.h"
#include "dataset/session.h"
#include "cfg.h"
#include <sstream>
#include <memory>

using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_ArkiCheck_Type = nullptr;

}


namespace {

void foreach_checker(std::shared_ptr<arki::dataset::Session> session, std::function<void(std::shared_ptr<arki::dataset::Checker>)> dest);

struct SkipDataset : public std::exception
{
    std::string msg;

    SkipDataset(const std::string& msg)
        : msg(msg) {}
    virtual ~SkipDataset() throw () {}

    virtual const char* what() const throw() { return msg.c_str(); }
};


struct remove : public MethKwargs<remove, arkipy_ArkiCheck>
{
    constexpr static const char* name = "remove";
    constexpr static const char* signature = "metadata_file: str";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "run arki-check --remove=metadata_file";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "metadata_file", nullptr };
        const char* arg_metadata_file = nullptr;
        Py_ssize_t arg_metadata_file_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist),
                    &arg_metadata_file, &arg_metadata_file_len))
            return nullptr;

        try {
            std::string metadata_file(arg_metadata_file, arg_metadata_file_len);

            {
                ReleaseGIL rg;
                arki::dataset::WriterPool pool(self->session);
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

                    auto ds = self->session->locate_metadata(*md);
                    if (!ds)
                    {
                        std::stringstream ss;
                        ss << "cannot remove data #" << idx << " is does not come from any known dataset";
                        throw std::runtime_error(ss.str());
                    }

                    dsnames.push_back(ds->name());
                    ++counts[ds->name()];
                    ++idx;
                }
                if (not self->checker_config.readonly)
                {
                    // Perform removals
                    idx = 1;
                    for (unsigned i = 0; i < todolist.size(); ++i)
                    {
                        auto ds = pool.get(dsnames[i]);
                        try {
                            ds->remove(todolist[i]);
                        } catch (std::exception& e) {
                            arki::nag::warning("Cannot remove message #%u: %s", idx, e.what());
                        }
                    }
                    for (const auto& i: counts)
                        arki::nag::verbose("%s: %u data deleted", i.first.c_str(), i.second);
                } else {
                    for (const auto& i: counts)
                        arki::nag::warning("%s: %u data would be deleted", i.first.c_str(), i.second);
                }
            }
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

template<typename Base, typename Impl>
struct checker_base : public MethKwargs<Base, Impl>
{
    constexpr static const char* signature = "";
    constexpr static const char* returns = "";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { nullptr };
        if (!PyArg_ParseTupleAndKeywords(args, kw, "", const_cast<char**>(kwlist)))
            return nullptr;
        try {
            ReleaseGIL rg;
            {
                foreach_checker(self->session, [&](std::shared_ptr<arki::dataset::Checker> checker) {
                    Base::process(self, *checker);
                });
            }
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct remove_all : public checker_base<remove_all, arkipy_ArkiCheck>
{
    constexpr static const char* name = "remove_all";
    constexpr static const char* summary = "run arki-check --remove-all";

    static void process(Impl* self, arki::dataset::Checker& checker)
    {
        checker.remove_all(self->checker_config);
    }
};

struct remove_old : public checker_base<remove_old, arkipy_ArkiCheck>
{
    constexpr static const char* name = "remove_old";
    constexpr static const char* summary = "run arki-check --remove-old";

    static void process(Impl* self, arki::dataset::Checker& checker)
    {
        checker.remove_old(self->checker_config);
    }
};

struct repack : public checker_base<repack, arkipy_ArkiCheck>
{
    constexpr static const char* name = "repack";
    constexpr static const char* summary = "run arki-check --repack";

    static void process(Impl* self, arki::dataset::Checker& checker)
    {
        checker.repack(self->checker_config);
    }
};

struct tar : public checker_base<tar, arkipy_ArkiCheck>
{
    constexpr static const char* name = "tar";
    constexpr static const char* summary = "run arki-check --tar";

    static void process(Impl* self, arki::dataset::Checker& checker)
    {
        checker.tar(self->checker_config);
    }
};

struct zip : public checker_base<zip, arkipy_ArkiCheck>
{
    constexpr static const char* name = "zip";
    constexpr static const char* summary = "run arki-check --zip";

    static void process(Impl* self, arki::dataset::Checker& checker)
    {
        checker.zip(self->checker_config);
    }
};

struct state : public checker_base<state, arkipy_ArkiCheck>
{
    constexpr static const char* name = "state";
    constexpr static const char* summary = "run arki-check --state";

    static void process(Impl* self, arki::dataset::Checker& checker)
    {
        checker.state(self->checker_config);
    }
};

struct check_issue51 : public checker_base<check_issue51, arkipy_ArkiCheck>
{
    constexpr static const char* name = "check_issue51";
    constexpr static const char* summary = "run arki-check --issue51";

    static void process(Impl* self, arki::dataset::Checker& checker)
    {
        checker.check_issue51(self->checker_config);
    }
};

struct check : public checker_base<check, arkipy_ArkiCheck>
{
    constexpr static const char* name = "check";
    constexpr static const char* summary = "run arki-check";

    static void process(Impl* self, arki::dataset::Checker& checker)
    {
        checker.check(self->checker_config);
    }
};

void foreach_checker(std::shared_ptr<arki::dataset::Session> session, std::function<void(std::shared_ptr<arki::dataset::Checker>)> dest)
{
    session->foreach_dataset([&](std::shared_ptr<arki::dataset::Dataset> ds) {
        std::shared_ptr<arki::dataset::Checker> checker;
        try {
            checker = ds->create_checker();
        } catch (std::exception& e) {
            arki::nag::warning("Skipping dataset %s: %s", ds->name().c_str(), e.what());
            return true;
        }
        dest(checker);
        return true;
    });
}

struct compress : public checker_base<compress, arkipy_ArkiCheck>
{
    constexpr static const char* name = "compress";
    constexpr static const char* summary = "run arki-check --compress";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "group_size", nullptr };
        int group_size;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "i", const_cast<char**>(kwlist), &group_size))
            return nullptr;
        try {
            ReleaseGIL rg;
            {
                foreach_checker(self->session, [&](std::shared_ptr<arki::dataset::Checker> checker) {
                    checker->compress(self->checker_config, group_size);
                });
            }
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct unarchive : public checker_base<unarchive, arkipy_ArkiCheck>
{
    constexpr static const char* name = "unarchive";
    constexpr static const char* summary = "run arki-check --unarchive";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "pathname", nullptr };
        const char* arg_pathname = nullptr;
        Py_ssize_t arg_pathname_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist),
                    &arg_pathname, &arg_pathname_len))
            return nullptr;
        try {
            std::string pathname(arg_pathname, arg_pathname_len);
            ReleaseGIL rg;
            {
                foreach_checker(self->session, [&](std::shared_ptr<arki::dataset::Checker> checker) {
                    if (auto c = std::dynamic_pointer_cast<arki::dataset::segmented::Checker>(checker))
                        c->segment(pathname)->unarchive();
                });
            }
            Py_RETURN_NONE;
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
    Methods<remove, remove_all, remove_old, repack, tar, zip, compress, unarchive, state, check_issue51, check> methods;

    static void _dealloc(Impl* self)
    {
        self->checker_config.~CheckerConfig();
        self->session.~shared_ptr<arki::dataset::Session>();
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
        static const char* kwlist[] = { "session", "filter", "accurate", "offline", "online", "readonly", nullptr };
        arkipy_DatasetSession* session = nullptr;
        const char* arg_filter = nullptr;
        Py_ssize_t arg_filter_len;
        int arg_accurate = 0;
        int arg_offline = 0;
        int arg_online = 0;
        int arg_readonly = 0;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O!|z#pppp", const_cast<char**>(kwlist),
                    arkipy_DatasetSession_Type, &session,
                    &arg_filter, &arg_filter_len,
                    &arg_accurate, &arg_offline, &arg_online, &arg_readonly))
            return -1;

        try {
            std::shared_ptr<arki::dataset::Reporter> reporter;

            pyo_unique_ptr mod_sys(throw_ifnull(PyImport_ImportModule("sys")));
            pyo_unique_ptr py_stdout(throw_ifnull(PyObject_GetAttrString(mod_sys, "stdout")));
            reporter = std::make_shared<dataset::TextIOReporter>(py_stdout);

            new (&(self->checker_config)) arki::dataset::CheckerConfig(reporter, arg_readonly);
            new (&(self->session)) std::shared_ptr<arki::dataset::Session>(session->ptr);

            if (arg_filter)
                self->checker_config.segment_filter = self->session->matcher(std::string(arg_filter, arg_filter_len));
            self->checker_config.accurate = arg_accurate;
            self->checker_config.online = arg_online;
            self->checker_config.offline = arg_offline;
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
