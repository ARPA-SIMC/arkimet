#include "arki-bufr-prepare.h"
#include "arki/nag.h"
#include <dballe/file.h>
#include <wreport/bulletin.h>
#include <dballe/importer.h>
#include <dballe/message.h>
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"

using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_ArkiBUFRPrepare_Type = nullptr;

}


namespace {

struct Output
{
    std::unique_ptr<dballe::File> output_ok;
    std::unique_ptr<dballe::File> output_fail;

    void open_ok_stdout()
    {
        output_ok = dballe::File::create(dballe::Encoding::BUFR, stdout, false, "[standard output]");
    }

    void open_ok_file(const std::string& pathname)
    {
        output_ok = dballe::File::create(dballe::Encoding::BUFR, pathname, "wb");
    }

    void open_fail_file(const std::string& pathname)
    {
        output_fail = dballe::File::create(dballe::Encoding::BUFR, pathname, "wb");
    }

    void ok(const std::string& buf)
    {
        if (!output_ok.get()) return;
        output_ok->write(buf);
    }

    void fail(const std::string& buf)
    {
        if (!output_fail.get()) return;
        output_fail->write(buf);
    }
};

struct Copier
{
protected:
    bool override_usn_active;
    int override_usn_value;

    void copy_base_msg(wreport::BufrBulletin& dst, const wreport::BufrBulletin& src)
    {
        // Copy message attributes
        dst.master_table_number = src.master_table_number;
        dst.data_category = src.data_category;
        dst.data_subcategory = src.data_subcategory;
        dst.data_subcategory_local = src.data_subcategory_local;
        dst.originating_centre = src.originating_centre;
        dst.originating_subcentre = src.originating_subcentre;
        if (override_usn_active)
            dst.update_sequence_number = override_usn_value;
        else
            dst.update_sequence_number = src.update_sequence_number;
        dst.rep_year = src.rep_year;
        dst.rep_month = src.rep_month;
        dst.rep_day = src.rep_day;
        dst.rep_hour = src.rep_hour;
        dst.rep_minute = src.rep_minute;
        dst.rep_second = src.rep_second;
        dst.edition_number = src.edition_number;
        dst.master_table_version_number = src.master_table_version_number;
        dst.master_table_version_number_local = src.master_table_version_number_local;
        // Do not compress, since it only makes sense for multisubset messages
        dst.compression = 0;

        // FIXME: the original optional section is lost

        // Copy data descriptor section
        dst.datadesc = src.datadesc;

        // Load encoding tables
        dst.load_tables();
    }

    void splitmsg(const dballe::BinaryMessage& rmsg, const wreport::BufrBulletin& msg, dballe::Importer& importer, Output& output)
    {
        // Create new message with the same info as the old one
        auto newmsg(wreport::BufrBulletin::create());
        copy_base_msg(*newmsg, msg);

        // Loop over subsets
        for (size_t i = 0; i < msg.subsets.size(); ++i)
        {
            // Create a bufrex_msg with the subset contents

            // Remove existing subsets
            newmsg->subsets.clear();

            // Copy subset
            newmsg->subsets.push_back(msg.subsets[i]);

            // Parse into dba_msg
            try {
                auto msgs = importer.from_bulletin(*newmsg);

                // Update reference time
                const dballe::Datetime& dt = msgs[0]->get_datetime();
                if (!dt.is_missing())
                {
                    newmsg->rep_year = dt.year;
                    newmsg->rep_month = dt.month;
                    newmsg->rep_day = dt.day;
                    newmsg->rep_hour = dt.hour;
                    newmsg->rep_minute = dt.minute;
                    newmsg->rep_second = dt.second;
                }
            } catch (wreport::error& e) {
                // Don't bother with updating reference time if
                // we cannot understand the layout of this BUFR
            }

            // Write out the message
            std::string newrmsg = newmsg->encode();
            output.ok(newrmsg);
        }
    }

public:
    Copier() : override_usn_active(false)
    {
    }

    void override_usn(int value)
    {
        override_usn_active = true;
        override_usn_value = value;
    }

    void process_stdin(Output& output)
    {
        auto file(dballe::File::create(dballe::Encoding::BUFR, stdin, false, "[standard input]").release());
        process(*file, output);
    }

    void process(const std::string& filename, Output& output)
    {
        auto file(dballe::File::create(dballe::Encoding::BUFR, filename.c_str(), "r").release());
        process(*file, output);
    }

    void process(dballe::File& infile, Output& output)
    {
        // Use .release() so the code is the same even with the new C++11's dballe
        auto importer = dballe::Importer::create(dballe::Encoding::BUFR);

        while (dballe::BinaryMessage rmsg = infile.read())
        {
            // Decode message
            std::unique_ptr<wreport::BufrBulletin> msg;
            bool decoded;
            try {
                msg = wreport::BufrBulletin::decode(rmsg.data, rmsg.pathname.c_str(), rmsg.offset);
                decoded = true;
            } catch (std::exception& e) {
                arki::nag::warning("%s:%ld: BUFR #%d failed to decode: %s.",
                    rmsg.pathname.c_str(), rmsg.offset, rmsg.index, e.what());
                decoded = false;
            }

            if (!decoded)
                output.fail(rmsg.data);
            else if (msg->subsets.size() == 1u && !override_usn_active)
                output.ok(rmsg.data);
            else
                splitmsg(rmsg, *msg, *importer, output);
        }
    }
};

struct run_ : public MethKwargs<run_, arkipy_ArkiBUFRPrepare>
{
    constexpr static const char* name = "run";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-bufr-prepare";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "inputs", "usn", "outfile", "failfile", nullptr };
        PyObject* arg_inputs = nullptr;
        PyObject* arg_usn = nullptr;
        const char* arg_outfile = nullptr;
        Py_ssize_t arg_outfile_len;
        const char* arg_failfile = nullptr;
        Py_ssize_t arg_failfile_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "|OOz#z#", const_cast<char**>(kwlist),
                    &arg_inputs, &arg_usn,
                    &arg_outfile, &arg_outfile_len,
                    &arg_failfile, &arg_failfile_len))
            return nullptr;

        try {
            std::vector<std::string> inputs;
            if (arg_inputs)
                inputs = stringlist_from_python(arg_inputs);

            std::string outfile;
            if (arg_outfile)
                outfile = std::string(arg_outfile, arg_outfile_len);

            std::string failfile;
            if (arg_failfile)
                failfile = std::string(arg_failfile, arg_failfile_len);

            bool force_usn = false;
            int forced_usn;
            if (arg_usn && arg_usn != Py_None)
            {
                force_usn = true;
                forced_usn = from_python<int>(arg_usn);
            }

            {
                ReleaseGIL rg;
                Copier copier;
                if (force_usn)
                    copier.override_usn(forced_usn);

                Output output;
                if (!outfile.empty())
                    output.open_ok_file(outfile);
                else
                    output.open_ok_stdout();

                if (!failfile.empty())
                    output.open_fail_file(failfile);

                if (inputs.empty())
                    copier.process_stdin(output);
                else
                    for (const auto& input: inputs)
                        copier.process(input.c_str(), output);
            }

            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};


struct ArkiBUFRPrepareDef : public Type<ArkiBUFRPrepareDef, arkipy_ArkiBUFRPrepare>
{
    constexpr static const char* name = "ArkiBUFRPrepare";
    constexpr static const char* qual_name = "arkimet.ArkiBUFRPrepare";
    constexpr static const char* doc = R"(
arki-bufr-prepare implementation
)";
    GetSetters<> getsetters;
    Methods<run_> methods;

    static void _dealloc(Impl* self)
    {
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

        return 0;
    }
};

ArkiBUFRPrepareDef* arki_bufr_prepare_def = nullptr;


}

namespace arki {
namespace python {

void register_arki_bufr_prepare(PyObject* m)
{
    arki_bufr_prepare_def = new ArkiBUFRPrepareDef;
    arki_bufr_prepare_def->define(arkipy_ArkiBUFRPrepare_Type, m);

}

}
}
