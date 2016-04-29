import sys
import os
import io
import tempfile
import configparser
import logging
import arkimet as arki

class ArkiView:
    """
    Base class for Django-CBV-style query handlers
    """
    content_type = "application/octet-stream"

    def __init__(self, request, handler, **kw):
        """
        request: werkzeug Request
        handler: Handler class for this request
        kw: keyword arguments from werkzeug URL routing patterns
        """
        self.request = request
        self.handler = handler
        self.kwargs = kw
        self.headers_sent = False

    def get_dataset_config(self):
        """
        Return a dict with the configuration of the dataset named in
        self.kwargs["name"]
        """
        if not self.handler.server.cfg.has_section(self.kwargs["name"]):
            raise NotFound("Dataset {} not found".format(self.kwargs["name"]))
        return dict(self.handler.server.cfg.items(self.kwargs["name"]))

    def get_dataset_reader(self):
        """
        Return the arki.DatasetReader for the dataset named in
        self.kwargs["name"]
        """
        return arki.DatasetReader(self.get_dataset_config())

    def get_query(self):
        """
        Return the dataset query matcher string
        """
        return self.request.form.get("query", "").strip()

    def get_sort(self):
        """
        Return the sort order string
        """
        return self.request.form.get("sort", "").strip()

    def get_headers_filename(self):
        """
        Return the file name to use in the Content-Disposition header.

        None if the Content-Disposition header should not be sent
        """
        return getattr(self, "headers_filename", None)

    def send_headers(self):
        """
        Send headers for a successful response
        """
        self.handler.send_response(200)
        self.handler.send_header("Content-Type", self.content_type)
        fname = self.get_headers_filename()
        if fname is not None:
            self.handler.send_header("Content-Disposition", "attachment; filename=" + fname)
        self.handler.end_headers()
        self.handler.flush_headers()
        self.headers_sent = True

    def handle_exception(self):
        """
        Called in an exception handler, send a response to communicate the
        exception.

        If response headers have already been sent, then there is nothing we
        can do, and just log the exception.
        """
        logging.exception("Exception caught after headers have been sent")
        if not self.headers_sent:
            ex = sys.exc_info()[1]
            self.handler.send_response(500)
            self.handler.send_header("Content-Type", "text/plain")
            self.handler.send_header("Arkimet-Exception", str(ex))
            self.handler.end_headers()
            self.handler.flush_headers()
            self.handler.wfile.write(str(ex).encode("utf-8"))
            self.handler.wfile.write(b"\n")

    def run(self):
        """
        Generate a response
        """
        try:
            self.stream()
            if not self.headers_sent:
                self.send_headers()
        except Exception:
            self.handle_exception()


class ArkiConfig(ArkiView):
    content_type = "text/plain"
    headers_filename = "config"

    def stream(self):
        # ./run-local arki-query "" http://localhost:8080
        # curl http://localhost:8080/config
        out = io.StringIO()
        self.handler.server.remote_cfg.write(out)
        self.send_headers()
        self.handler.wfile.write(out.getvalue().encode("utf-8"))


class ArkiQExpand(ArkiView):
    content_type = "text/plain"

    def stream(self):
        # ./run-local arki-query "" http://localhost:8080
        query = self.request.form["query"].strip()
        expanded = arki.expand_query(query)
        self.send_headers()
        self.handler.wfile.write(expanded.encode("utf-8"))


class ArkiAliases(ArkiView):
    content_type = "text/plain"

    def stream(self):
        # ./run-local arki-query "" http://localhost:8080
        self.send_headers()
        out = arki.matcher_alias_database()
        self.handler.wfile.write(out.encode("utf-8"))


class ArkiDatasetConfig(ArkiView):
    content_type = "text/plain"

    def stream(self):
        # ./run-local arki-query "" http://localhost:8080/dataset/<name>
        # curl http://localhost:8080/dataset/<name>/config
        if not self.handler.server.remote_cfg.has_section(self.kwargs["name"]):
            raise NotFound("Dataset {} not found".format(self.kwargs["name"]))
        self.headers_filename = self.kwargs["name"] + ".config"
        cfg = configparser.ConfigParser()
        cfg.add_section(self.kwargs["name"])
        for k, v in self.handler.server.remote_cfg.items(self.kwargs["name"]):
            cfg.set(self.kwargs["name"], k, v)

        out = io.StringIO()
        cfg.write(out)

        self.send_headers()
        self.handler.wfile.write(out.getvalue().encode("utf-8"))


class TempdirMixin:
    """
    Move to a temporary directory while running the ArkiView
    """
    def run(self):
        origdir = os.getcwd()
        try:
            with tempfile.TemporaryDirectory(prefix="arki-server-") as tmpdir:
                os.chdir(tmpdir)
                self.stream()
                if not self.headers_sent:
                    self.send_headers()
        except Exception:
            self.handle_exception()
        finally:
            os.chdir(origdir)


class ArkiDatasetQuery(TempdirMixin, ArkiView):
    headers_ext = None

    def get_headers_filename(self):
        if self.headers_ext:
            return self.kwargs["name"] + "." + self.headers_ext
        else:
            return self.kwargs["name"]


class ArkiDatasetSummary(ArkiDatasetQuery):
    headers_ext = "summary"

    def stream(self):
        summary = self.get_dataset_reader().query_summary(self.get_query())
        self.send_headers()
        summary.write(self.handler.wfile)


class DatasetQueryData(ArkiDatasetQuery):
    headers_ext = "data"

    def stream(self):
        self.get_dataset_reader().query_bytes(
            file=self.handler.wfile,
            matcher=self.get_query(),
            sort=self.get_sort(),
            data_start_hook=self.send_headers)


class DatasetQueryRepMetadata(ArkiDatasetQuery):
    content_type = "text/plain"
    headers_ext = "txt"

    def stream(self):
        self.get_dataset_reader().query_bytes(
            file=self.handler.wfile,
            matcher=self.get_query(),
            sort=self.get_sort(),
            data_start_hook=self.send_headers,
            metadata_report=self.request.form.get("command", ""))


class DatasetQueryRepSummary(ArkiDatasetQuery):
    content_type = "text/plain"
    headers_ext = "txt"

    def stream(self):
        self.get_dataset_reader().query_bytes(
            file=self.handler.wfile,
            matcher=self.get_query(),
            data_start_hook=self.send_headers,
            summary_report=self.request.form.get("command", ""))


class DatasetQueryMetadata(ArkiDatasetQuery):
    headers_ext = "arkimet"

    def on_metadata(self, md):
        if not self.headers_sent:
            self.send_headers()
        md.make_url(self.url)
        md.write(self.handler.wfile)

    def get_metadata_url(self):
        return self.handler.server.url + "/dataset/" + self.kwargs["name"]

    def stream(self):
        self.url = self.get_metadata_url()
        self.get_dataset_reader().query_data(
            on_metadata=self.on_metadata,
            matcher=self.get_query(),
            sort=self.get_sort())


class DatasetQueryMetadataInline(ArkiDatasetQuery):
    headers_ext = "arkimet"

    def on_metadata(self, md):
        if not self.headers_sent:
            self.send_headers()
        md.make_inline()
        md.write(self.handler.wfile)

    def stream(self):
        self.get_dataset_reader().query_data(
            on_metadata=self.on_metadata,
            matcher=self.get_query(),
            with_data=True,
            sort=self.get_sort())


class DatasetQueryPostprocess(ArkiDatasetQuery):
    headers_ext = "postprocessed"

    def stream(self):
        # Iterate submitted files and export information about them to the
        # environment
        names = []
        for name, file in self.request.files.items():
            basename = os.path.basename(file.filename)
            file.save(basename)
            names.append(basename)

        if names:
            names.sort()
            os.environ["ARKI_POSTPROC_FILES"] = ":".join(names)

        self.get_dataset_reader().query_bytes(
            file=self.handler.wfile,
            matcher=self.get_query(),
            with_data=True,
            sort=self.get_sort(),
            data_start_hook=self.send_headers,
            postprocess=self.request.form.get("command", ""))


def get_view_for_style(style):
    if style == "metadata":
        return DatasetQueryMetadata
    elif style == "inline":
        return DatasetQueryMetadataInline
    elif style == "data":
        return DatasetQueryData
    elif style == "postprocess":
        return DatasetQueryPostprocess
    elif style == "rep_metadata":
        return DatasetQueryRepMetadata
    elif style == "rep_summary":
        return DatasetQueryRepSummary
    else:
        raise NotFound("TODO: query style {}".format(style))


def arki_dataset_query(request, handler, **kw):
    """
    Create the right view for a dataset query, given the `style` form value.
    """
    style = request.form.get("style", "metadata").strip()
    View = get_view_for_style(style)
    return View(request, handler, **kw)


class QMacroMixin:
    def get_headers_filename(self):
        if self.headers_ext:
            return self.request.form.get("qmacro", "").strip() + "." + self.headers_ext
        else:
            return self.request.form.get("qmacro", "").strip()

    def get_metadata_url(self):
        #return self.handler.server.url + "/dataset/" + self.request.form.get("qmacro", "").strip()
        return self.handler.server.url + "/query"

    def get_dataset_reader(self):
        cfg = io.StringIO()
        self.handler.server.cfg.write(cfg)
        return arki.make_qmacro_dataset(
            "url = " + self.handler.server.url,
            cfg.getvalue(),
            self.request.form.get("qmacro", "").strip(),
            self.request.form.get("query", "").strip()
        )

    def get_query(self):
        return ""

def arki_query(request, handler, **kw):
    style = request.form.get("style", "metadata").strip()
    View = get_view_for_style(style)
    class QMacroView(QMacroMixin, View):
        pass
    return QMacroView(request, handler, **kw)

# TODO: def arki_index(self, request):
# TODO:     raise NotFound("TODO: index")
# TODO:     # local_handlers.add("", new IndexHandler);

#    def arki_dataset(self, path):
#        self.send_404("TODO: dataset")
#        # local_handlers.add("dataset", new DatasetHandler);

#// Dispatch dataset-specific actions
#struct DatasetHandler : public LocalHandler
#{
#    DatasetHandler()
#    {
#    }
#
#    // Show the summary of a dataset
#    void do_index(dataset::Reader& ds, const std::string& dsname, Request& req)
#    {
#        req.log_action("index of dataset " + dsname);
#
#        // Query the summary
#        Summary sum;
#        ds.query_summary(Matcher(), sum);
#
#        // Create the output page
#        stringstream res;
#        res << "<html>" << endl;
#        res << "<head><title>Dataset " << dsname << "</title></head>" << endl;
#        res << "<body>" << endl;
#        res << "<ul>" << endl;
#        res << "<li><a href='/'>All datasets</a></li>" << endl;
#        res << "<li><a href='/dataset/" << dsname << "/queryform'>Query</a></li>" << endl;
#        res << "<li><a href='/dataset/" << dsname << "/summary'>Download summary</a></li>" << endl;
#        res << "</ul>" << endl;
#        res << "<p>Summary of dataset <b>" << dsname << "</b>:</p>" << endl;
#        res << "<pre>" << endl;
#        try {
#            Report rep("dsinfo");
#            rep.captureOutput(res);
#            rep(sum);
#            rep.report();
#        } catch (std::exception& e) {
#            sum.writeYaml(res);
#        }
#        res << "</pre>" << endl;
#        res << "</body>" << endl;
#        res << "</html>" << endl;
#
#        req.send_result(res.str());
#    }
#
#    // Show a form to query the dataset
#    void do_queryform(const std::string& dsname, Request& req)
#    {
#        req.log_action("query form for dataset " + dsname);
#
#        stringstream res;
#        res << "<html>" << endl;
#        res << "<head><title>Query dataset " << dsname << "</title></head>" << endl;
#        res << "<body>" << endl;
#        res << "  Please type or paste your query and press submit:" << endl;
#        res << "  <form action='/dataset/" << dsname << "/query' method='push'>" << endl;
#        res << "    <textarea name='query' cols='80' rows='15'>" << endl;
#        res << "    </textarea>" << endl;
#        res << "    <br/>" << endl;
#        res << "    <select name='style'>" << endl;
#        res << "      <option value='data'>Data</option>" << endl;
#        res << "      <option value='yaml'>Human-readable metadata</option>" << endl;
#        res << "      <option value='json'>JSON metadata</option>" << endl;
#        res << "      <option value='inline'>Binary metadata and data</option>" << endl;
#        res << "      <option value='md'>Binary metadata</option>" << endl;
#        res << "    </select>" << endl;
#        res << "    <br/>" << endl;
#        res << "    <input type='submit'>" << endl;
#        res << "  </form>" << endl;
#        res << "</body>" << endl;
#        res << "</html>" << endl;
#        req.send_result(res.str());
#    }
#
#    virtual void operator()(Request& req)
#    {
#        string dsname = req.pop_path_info();
#        if (dsname.empty())
#            throw net::http::error404();
#
#        string action = req.pop_path_info();
#        if (action.empty())
#            action = "index";
#
#        unique_ptr<dataset::Reader> ds = req.get_dataset(dsname);
#
#        if (action == "index")
#            do_index(*ds, dsname, req);
#        else if (action == "queryform")
#            do_queryform(dsname, req);
#        else if (action == "config")
#        {
#            req.log_action("configuration for dataset " + dsname);
#            dataset::http::ReaderServer srv(*ds, dsname);
#            srv.do_config(req.get_config_remote(dsname), req);
#        }
#        else if (action == "summary")
#        {
#            req.log_action("summary for dataset " + dsname);
#            dataset::http::ReaderServer srv(*ds, dsname);
#            dataset::http::LegacySummaryParams params;
#            params.parse_get_or_post(req);
#            srv.do_summary(params, req);
#        }
#        else if (action == "summaryshort")
#        {
#            req.log_action("summary-short for dataset " + dsname);
#            dataset::http::ReaderServer srv(*ds, dsname);
#            dataset::http::LegacySummaryParams params;
#            params.parse_get_or_post(req);
#            srv.do_summary_short(params, req);
#        }
#        else if (action == "query")
#        {
#            req.log_action("query dataset " + dsname);
#            dataset::http::ReaderServer srv(*ds, dsname);
#            utils::MoveToTempDir tempdir("/tmp/arki-server.XXXXXX");
#            dataset::http::LegacyQueryParams params(tempdir.tmp_dir);
#            params.parse_get_or_post(req);
#            srv.do_query(params, req);
#        }
#        else if (action == "querydata")
#        {
#            req.log_action("querydata in dataset " + dsname);
#            dataset::http::ReaderServer srv(*ds, dsname);
#            dataset::http::QueryDataParams params;
#            params.parse_get_or_post(req);
#            srv.do_queryData(params, req);
#        }
#        else if (action == "querysummary")
#        {
#            req.log_action("querysummary in dataset " + dsname);
#            dataset::http::ReaderServer srv(*ds, dsname);
#            dataset::http::QuerySummaryParams params;
#            params.parse_get_or_post(req);
#            srv.do_querySummary(params, req);
#        }
#        else if (action == "querybytes")
#        {
#            req.log_action("querybytes in dataset " + dsname);
#            dataset::http::ReaderServer srv(*ds, dsname);
#            utils::MoveToTempDir tempdir("/tmp/arki-server.XXXXXX");
#            dataset::http::QueryBytesParams params(tempdir.tmp_dir);
#            params.parse_get_or_post(req);
#            srv.do_queryBytes(params, req);
#        }
#        else
#            throw std::runtime_error("Unknown dataset action: \"" + action + "\"");
#    }
#};

#    def arki_summary(self, path):
#        self.send_404("TODO: summary")
#        # local_handlers.add("summary", new RootSummaryHandler);

#struct RootSummaryHandler : public LocalHandler
#{
#    virtual void operator()(Request& req)
#    {
#        req.log_action("root-level summary");
#
#        using namespace net::http;
#        Params params;
#        ParamSingle* style = params.add<ParamSingle>("style");
#        ParamSingle* query = params.add<ParamSingle>("query");
#        ParamSingle* qmacro = params.add<ParamSingle>("qmacro");
#        params.parse_get_or_post(req);
#
#        string macroname = str::strip(*qmacro);
#
#        unique_ptr<dataset::Reader> ds;
#        if (macroname.empty())
#            // Create a merge dataset with all we have
#            ds.reset(new dataset::AutoMerged(req.arki_conf));
#        else
#        {
#            // Create qmacro dataset
#            ConfigFile dsconf;
#            string url(req.server_name);
#            url += req.script_name;
#            dsconf.setValue("url", url);
#            ds = runtime::make_qmacro_dataset(
#                dsconf, req.arki_conf, macroname, *query);
#        }
#
#        // Query the summary
#        Summary sum;
#        ds->query_summary(Matcher(), sum);
#
#        if (*style == "yaml")
#        {
#            stringstream res;
#            sum.writeYaml(res);
#            req.send_result(res.str(), "text/x-yaml", macroname + "-summary.yaml");
#        }
#        else if (*style == "json")
#        {
#            stringstream res;
#            emitter::JSON json(res);
#            sum.serialise(json);
#            req.send_result(res.str(), "application/json", macroname + "-summary.json");
#        }
#        else
#        {
#            vector<uint8_t> res = sum.encode(true);
#            req.send_result(res, "application/octet-stream", macroname + "-summary.bin");
#        }
#    }
#};


#    #    if (opts.inbound->isSet())
#    #        local_handlers.add("inbound", new InboundHandler(opts.inbound->stringValue()));


