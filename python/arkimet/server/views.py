import configparser
import contextlib
import html
import io
import logging
import os
import sys
import tempfile
import time
from typing import Optional
from urllib.parse import quote

from werkzeug.exceptions import NotFound


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
        self.session = handler.server.session
        self.kwargs = kw
        self.headers_sent = False
        # Information to be logged about this query
        self.info = {
            "view": self.__class__.__name__,
            "ts_start": time.time(),
        }

    def get_dataset_config(self):
        """
        Return a dict with the configuration of the dataset named in
        self.kwargs["name"]
        """
        name = self.kwargs["name"]
        self.info["dataset"] = name
        if name not in self.handler.server.cfg:
            raise NotFound("Dataset {} not found".format(name))
        return self.handler.server.cfg[name]

    def get_dataset_reader(self):
        """
        Return the arki.dataset.Reader for the dataset named in
        self.kwargs["name"]
        """
        name = self.kwargs["name"]
        self.info["dataset"] = name
        try:
            return self.session.dataset_reader(name=name)
        except RuntimeError:
            raise NotFound(f"Dataset {name} not found")

    def get_query(self):
        """
        Return the dataset query matcher string
        """
        query = self.request.values.get("query", "").strip()
        if query:
            self.info["query"] = query
        return query

    def get_sort(self):
        """
        Return the sort order string
        """
        sort = self.request.values.get("sort", "").strip()
        if sort:
            self.info["sort"] = sort
        return sort

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
        self.info["ts_headers"] = time.time()
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
        if not self.headers_sent:
            logging.exception("Exception caught before headers have been sent")
            ex = sys.exc_info()[1]
            # If the exception has code attribute, use for the status code
            code = getattr(ex, "code", 500)
            self.handler.send_response(code)
            self.handler.send_header("Content-Type", "text/plain")
            self.handler.send_header("Arkimet-Exception", str(ex))
            self.handler.end_headers()
            self.handler.flush_headers()
            self.headers_sent = True
            self.handler.wfile.write(str(ex).encode("utf-8"))
            self.handler.wfile.write(b"\n")
        else:
            logging.exception("Exception caught after headers have been sent")

    def http_error(self, code: int, message: Optional[str] = None):
        """
        Send the given HTTP error
        """
        if not self.headers_sent:
            self.handler.send_response(code, message=message)
            self.handler.send_header("Content-Type", "text/plain")
            self.handler.end_headers()
            self.handler.flush_headers()
            self.headers_sent = True
            if message:
                self.handler.wfile.write(message)
            self.handler.wfile.write(b"\n")
        else:
            logging.error("Sending HTTP error %d after headers have been sent", code)

    def log_end(self):
        self.info["ts_end"] = time.time()
        logging.info("Query: %r", self.info, extra={"perf": self.info})

    @contextlib.contextmanager
    def response(self):
        try:
            yield
            if not self.headers_sent:
                self.send_headers()
        except Exception:
            self.handle_exception()
        self.log_end()

    def get(self):
        self.http_error(405)
        self.log_end()

    def post(self):
        self.http_error(405)
        self.log_end()


class HTMLWriter:
    def __init__(self):
        self.out = io.StringIO()

    def print(self, *args, **kw):
        kw["file"] = self.out
        print(*args, **kw)

    @contextlib.contextmanager
    def html(self):
        self.print("<html>")
        yield
        self.print("</html>")

    @contextlib.contextmanager
    def body(self):
        self.print("<body>")
        yield
        self.print("</body>")

    @contextlib.contextmanager
    def ul(self):
        self.print("<ul>")
        yield
        self.print("</ul>")

    @contextlib.contextmanager
    def li(self):
        self.print("<li>")
        yield
        self.print("</li>")

    @contextlib.contextmanager
    def tag(self, name, **kw):
        self.print("<{name}{attrs}>".format(
            name=name,
            attrs=" ".join("{key}='{val}'".format(key=key, val=quote(val)) for key, val in kw.items()),
        ))
        yield
        self.print("</{name}>".format(name=name))

    def inline_tag(self, name, text="", **kw):
        self.print("<{name}{attrs}>{text}</{name}>".format(
            name=name,
            attrs=" ".join("{key}='{val}'".format(key=key, val=quote(val)) for key, val in kw.items()),
            text=text,
        ))

    def head(self, title):
        self.print("<head><title>{title}</title></head>".format(title=html.escape(title)))

    def p(self, text):
        self.print("<p>{text}</p>".format(text=html.escape(text)))

    def h1(self, text):
        self.print("<h1>{text}</h1>".format(text=html.escape(text)))

    def a(self, href, text):
        self.print("<a href='{href}'>{text}</a>".format(
            href=quote(href), text=html.escape(text)))


class ArkiIndex(ArkiView):
    content_type = "text/html"

    def get(self):
        with self.response():
            page = HTMLWriter()
            with page.html():
                page.head("Dataset index")
                with page.body():
                    page.p("Available datasets:")
                    with page.ul():
                        for name in self.handler.server.cfg.keys():
                            with page.li():
                                page.a("/dataset/" + name, name)
                # page.a("/query", "Perform a query")
            self.send_headers()
            self.handler.wfile.write(page.out.getvalue().encode("utf-8"))


class ArkiConfig(ArkiView):
    content_type = "text/plain"
    headers_filename = "config"

    def get(self):
        with self.response():
            # ./run-local arki-query "" http://localhost:8080
            # curl http://localhost:8080/config
            out = io.StringIO()
            self.handler.server.remote_cfg.write(out)
            self.send_headers()
            self.handler.wfile.write(out.getvalue().encode("utf-8"))


class ArkiQExpand(ArkiView):
    content_type = "text/plain"

    def post(self):
        with self.response():
            # ./run-local arki-query "" http://localhost:8080
            query = self.request.values["query"].strip()
            expanded = self.session.expand_query(query)
            self.send_headers()
            self.handler.wfile.write(expanded.encode("utf-8"))


class ArkiAliases(ArkiView):
    content_type = "text/plain"

    def get(self):
        with self.response():
            # ./run-local arki-query "" http://localhost:8080
            self.send_headers()
            out = self.session.get_alias_database()
            with io.StringIO() as buf:
                out.write(buf)
                self.handler.wfile.write(buf.getvalue().encode())


class ArkiDatasetConfig(ArkiView):
    content_type = "text/plain"

    def get(self):
        with self.response():
            # ./run-local arki-query "" http://localhost:8080/dataset/<name>
            # curl http://localhost:8080/dataset/<name>/config
            if self.kwargs["name"] not in self.handler.server.remote_cfg:
                raise NotFound("Dataset {} not found".format(self.kwargs["name"]))
            self.headers_filename = self.kwargs["name"] + ".config"
            cfg = configparser.ConfigParser()
            cfg.add_section(self.kwargs["name"])
            for k, v in self.handler.server.remote_cfg[self.kwargs["name"]].items():
                cfg.set(self.kwargs["name"], k, v)

            out = io.StringIO()
            cfg.write(out)

            self.send_headers()
            self.handler.wfile.write(out.getvalue().encode("utf-8"))


class TempdirMixin:
    """
    Move to a temporary directory while running the ArkiView
    """
    @contextlib.contextmanager
    def response(self):
        origdir = os.getcwd()
        try:
            with tempfile.TemporaryDirectory(prefix="arki-server-") as tmpdir:
                os.chdir(tmpdir)
                yield
                if not self.headers_sent:
                    self.send_headers()
        except Exception:
            self.handle_exception()
        finally:
            os.chdir(origdir)
        self.log_end()


class ArkiDatasetQuery(TempdirMixin, ArkiView):
    headers_ext = None

    def get_headers_filename(self):
        if self.headers_ext:
            return self.kwargs["name"] + "." + self.headers_ext
        else:
            return self.kwargs["name"]


class ArkiDatasetSummary(ArkiDatasetQuery):
    headers_ext = "summary"

    def post(self):
        with self.response():
            summary = self.get_dataset_reader().query_summary(self.get_query())
            self.send_headers()
            summary.write(self.handler.wfile, format=self.request.values.get("style", "binary").strip())


class ArkiDatasetSummaryShort(ArkiDatasetQuery):
    headers_ext = "summary"

    def post(self):
        with self.response():
            summary = self.get_dataset_reader().query_summary(self.get_query())
            self.send_headers()
            summary.write_short(self.handler.wfile, format=self.request.values.get("style", "yaml").strip())


class DatasetQueryData(ArkiDatasetQuery):
    headers_ext = "data"

    def post(self):
        with self.response():
            self.get_dataset_reader().query_bytes(
                file=self.handler.wfile,
                matcher=self.get_query(),
                sort=self.get_sort(),
                data_start_hook=self.send_headers)


class DatasetQueryMetadata(ArkiDatasetQuery):
    headers_ext = "arkimet"

    def on_metadata(self, md):
        if not self.headers_sent:
            self.send_headers()
        md.make_url(self.url)
        md.write(self.handler.wfile)

    def get_metadata_url(self):
        return self.handler.server.url + "/dataset/" + self.kwargs["name"]

    def post(self):
        with self.response():
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

    def post(self):
        with self.response():
            self.get_dataset_reader().query_data(
                on_metadata=self.on_metadata,
                matcher=self.get_query(),
                with_data=True,
                sort=self.get_sort())


class DatasetQueryPostprocess(ArkiDatasetQuery):
    headers_ext = "postprocessed"

    # This needs to be accessible also over get. See #289
    def get(self):
        self.post()

    def post(self):
        with self.response():
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

            command = self.request.values.get("command", "")
            self.info["postprocess"] = command

            self.get_dataset_reader().query_bytes(
                file=self.handler.wfile,
                matcher=self.get_query(),
                with_data=True,
                sort=self.get_sort(),
                data_start_hook=self.send_headers,
                postprocess=command)


def get_view_for_style(style):
    if style == "metadata":
        return DatasetQueryMetadata
    elif style == "inline":
        return DatasetQueryMetadataInline
    elif style == "data":
        return DatasetQueryData
    elif style == "postprocess":
        return DatasetQueryPostprocess
    else:
        raise NotFound("TODO: query style {}".format(style))


def arki_dataset_query(request, handler, **kw):
    """
    Create the right view for a dataset query, given the `style` form value.
    """
    style = request.values.get("style", "metadata").strip()
    View = get_view_for_style(style)
    return View(request, handler, **kw)


class QMacroMixin:
    def get_headers_filename(self):
        if self.headers_ext:
            return self.request.values.get("qmacro", "").strip() + "." + self.headers_ext
        else:
            return self.request.values.get("qmacro", "").strip()

    def get_metadata_url(self):
        # return self.handler.server.url + "/dataset/" + self.request.values.get("qmacro", "").strip()
        return self.handler.server.url + "/query"

    def get_dataset_reader(self):
        qmacro = self.request.values.get("qmacro", "").strip()
        if not qmacro:
            return self.session.merged().reader()
        else:
            return self.session.querymacro(
                qmacro,
                self.request.values.get("query", "").strip()
            ).reader()

    def get_query(self):
        return ""


def arki_query(request, handler, **kw):
    style = request.values.get("style", "metadata").strip()
    View = get_view_for_style(style)

    class QMacroView(QMacroMixin, View):
        pass
    return QMacroView(request, handler, **kw)


class ArkiSummary(QMacroMixin, ArkiView):
    headers_ext = "summary"

    def post(self):
        with self.response():
            summary = self.get_dataset_reader().query_summary(self.get_query())
            self.send_headers()
            summary.write(self.handler.wfile, format=self.request.values.get("style", "binary").strip())


class ArkiDatasetIndex(ArkiView):
    content_type = "text/html"

    def get(self):
        with self.response():
            name = self.kwargs["name"]
            cfg = self.get_dataset_config()

            # // Query the summary
            # Summary sum;
            # ds.query_summary(Matcher(), sum);

            page = HTMLWriter()
            with page.html():
                page.head("Dataset " + name)
                with page.body():
                    page.h1("Dataset " + name)
                    page.p("Configuration:")
                    with page.tag("pre"):
                        for k, v in sorted(cfg.items()):
                            print("{k} = {v}".format(k=html.escape(k), v=html.escape(v)), file=page.out)
                    with page.ul():
                        with page.li():
                            page.a("/dataset/" + name + "/summary", "Download summary")
                        with page.li():
                            page.a("/", "List of all datasets")

                    # res << "<p>Summary of dataset <b>" << dsname << "</b>:</p>" << endl;
                    # res << "<pre>" << endl;
                    # try {
                    #     Report rep("dsinfo");
                    #     rep.captureOutput(res);
                    #     rep(sum);
                    #     rep.report();
                    # } catch (std::exception& e) {
                    #     sum.writeYaml(res);
                    # }
                    # res << "</pre>" << endl;
                    # res << "</body>" << endl;
                    # res << "</html>" << endl;
            self.send_headers()
            self.handler.wfile.write(page.out.getvalue().encode("utf-8"))
