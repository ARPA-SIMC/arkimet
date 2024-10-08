# python 3.7+ from __future__ import annotations
import argparse
import datetime
import logging
import re
import socket
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ForkingMixIn
from urllib.parse import unquote, urlsplit

from werkzeug.exceptions import HTTPException, NotFound

import arkimet as arki
from arkimet.server import views

try:
    from setproctitle import setproctitle
except ModuleNotFoundError:
    setproctitle = None
from arkimet.cmdline.base import App


class ArkiServer(ForkingMixIn, HTTPServer):
    def __init__(self, *args, aliases=None, **kw):
        super().__init__(*args, **kw)
        from werkzeug.routing import Map, Rule

        self.url = "http://{s.server_name}:{s.server_port}".format(s=self)
        self.url_map = Map(
            [
                Rule("/", endpoint="ArkiIndex"),
                Rule("/config", endpoint="ArkiConfig"),
                Rule("/qexpand", endpoint="ArkiQExpand"),
                Rule("/aliases", endpoint="ArkiAliases"),
                Rule("/query", endpoint="arki_query"),
                Rule("/summary", endpoint="ArkiSummary"),
                Rule("/dataset/<name>", endpoint="ArkiDatasetIndex"),
                Rule("/dataset/<name>/query", endpoint="arki_dataset_query"),
                Rule("/dataset/<name>/summary", endpoint="ArkiDatasetSummary"),
                Rule("/dataset/<name>/summaryshort", endpoint="ArkiDatasetSummaryShort"),
                Rule("/dataset/<name>/config", endpoint="ArkiDatasetConfig"),
            ]
        )
        # Session to use to manage aliases and datasets
        if aliases is None:
            # Load default system aliases
            self.session = arki.dataset.Session()
        else:
            self.session = arki.dataset.Session(load_aliases=False)
            self.session.load_aliases(aliases)

    def server_bind(self):
        if self.server_address[1] != 0:
            super().server_bind()
        else:
            # Bind to a random port and reread the server address
            self.socket.bind(self.server_address)
            self.server_address = self.socket.getsockname()
            host, port = self.server_address[:2]
            self.server_name = socket.getfqdn(host)
            self.server_port = port

    def set_config(self, config):
        self.cfg = config

        # Amend configuration turning local datasets into remote dataset
        self.remote_cfg = arki.cfg.Sections()
        for name, sec in self.cfg.items():
            # Add the dataset to the session pool
            self.session.add_dataset(sec)
            self.remote_cfg[name] = sec.copy()
            self.remote_cfg[name]["path"] = self.url + "/dataset/" + name
            self.remote_cfg[name]["type"] = "remote"
            self.remote_cfg[name]["server"] = self.url


class Handler(BaseHTTPRequestHandler):
    re_pathsplit = re.compile(r"/+")

    def make_environ(self):
        """
        Create an environment that can be used with werkzeug
        """
        try:
            from werkzeug._compat import wsgi_encoding_dance as _wsgi_encoding_dance
        except ModuleNotFoundError:
            from werkzeug._internal import _wsgi_encoding_dance

        request_url = urlsplit(self.path)
        url_scheme = "http"

        # If there was no scheme but the path started with two slashes,
        # the first segment may have been incorrectly parsed as the
        # netloc, prepend it to the path again.
        if not request_url.scheme and request_url.netloc:
            path_info = f"/{request_url.netloc}{request_url.path}"
        else:
            path_info = request_url.path

        path_info = unquote(path_info)

        environ = {
            "wsgi.version": (1, 0),
            "wsgi.url_scheme": url_scheme,
            "wsgi.input": self.rfile,
            "wsgi.errors": sys.stderr,
            "wsgi.multithread": False,
            "wsgi.multiprocess": True,
            "wsgi.run_once": False,
            "SERVER_SOFTWARE": self.server_version,
            "REQUEST_METHOD": self.command,
            "SCRIPT_NAME": "",
            "PATH_INFO": _wsgi_encoding_dance(path_info),
            "QUERY_STRING": _wsgi_encoding_dance(request_url.query),
            "CONTENT_TYPE": self.headers.get("Content-Type", ""),
            "CONTENT_LENGTH": self.headers.get("Content-Length", ""),
            "REMOTE_ADDR": self.client_address[0],
            "REMOTE_PORT": self.client_address[1],
            "SERVER_NAME": self.server.server_address[0],
            "SERVER_PORT": str(self.server.server_address[1]),
            "SERVER_PROTOCOL": self.request_version,
        }

        for key, value in self.headers.items():
            key = "HTTP_" + key.upper().replace("-", "_")
            if key not in ("HTTP_CONTENT_TYPE", "HTTP_CONTENT_LENGTH"):
                environ[key] = value

        if request_url.netloc:
            environ["HTTP_HOST"] = request_url.netloc

        return environ

    def log_request(self, code="-", size="-"):
        """
        Wrap BaseHTTPRequestHandler's send_response to add logging of the
        request
        """
        logging.info(
            '%s - - [%s] "%s" %s %s',
            self.client_address[0],
            datetime.datetime.utcnow().strftime("%d/%b/%Y:%H:%M:%S +0000"),
            self.requestline,
            code,
            size,
            extra={"access_log": True},
        )

    def dispatch(self, method: str):
        """
        Dispatch a request to the endpoint given by self.server.url_map
        """
        from werkzeug.wrappers import Request

        request = Request(self.make_environ())
        if setproctitle is not None:
            setproctitle("arki-server {} for {}".format(request.base_url, request.remote_addr))
        adapter = self.server.url_map.bind_to_environ(request.environ)
        try:
            endpoint, values = adapter.match()
            meth = getattr(views, endpoint, None)
            if meth is None:
                raise NotFound("endpoint {} not found".format(endpoint))
            view = meth(request, self, **values)
            getattr(view, method)()
        except HTTPException as e:
            self.send_response(e.code, e.description)
            for k, v in e.get_headers():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(e.get_body(request.environ).encode("utf-8"))

    def do_GET(self):
        self.dispatch(method="get")

    def do_POST(self):
        self.dispatch(method="post")


class JournaldFormatter(logging.Formatter):
    """
    Format log entries for printing to stdout/stderr adding logging priority in
    a way that can be interpreted by journald.

    See http://0pointer.de/blog/projects/journal-submit.html
    """

    # http://elinux.org/Debugging_by_printing#Log_Levels
    # Name      String Meaning alias function
    # KERN_EMERG   "0" Emergency messages, system is about to crash or is unstable pr_emerg
    # KERN_ALERT   "1" Something bad happened and action must be taken immediately pr_alert
    # KERN_CRIT    "2" A critical condition occurred like a serious hardware/software failure  pr_crit
    # KERN_ERR     "3" An error condition, often used by drivers to indicate difficulties with the hardware    pr_err
    # KERN_WARNING "4" A warning, meaning nothing serious by itself but might indicate problems    pr_warning
    # KERN_NOTICE  "5" Nothing serious, but notably nevertheless. Often used to report security events.    pr_notice
    # KERN_INFO    "6" Informational message e.g. startup information at driver initialization pr_info
    # KERN_DEBUG   "7" Debug messages  pr_debug, pr_devel if DEBUG is defined
    # KERN_DEFAULT "d" The default kernel loglevel
    # KERN_CONT    ""  "continued" line of log printout (only done after a line that had no enclosing \n) [1]  pr_cont
    level_map = {
        logging.DEBUG: 7,
        logging.INFO: 6,
        logging.WARNING: 4,
        logging.ERROR: 3,
        logging.CRITICAL: 2,
    }

    def format(self, record):
        return "<{}>{}".format(
            self.level_map.get(record.levelno, 4),
            record.getMessage(),
        )
        return ""


class DefaultLogFilter:
    """
    Filter for the default log message destination (usually stdout)
    """

    def __init__(self, has_accesslog):
        self.has_accesslog = bool(has_accesslog)

    def filter(self, record):
        if self.has_accesslog and getattr(record, "access_log", False):
            return False
        return getattr(record, "perf", None) is None


def make_server(host, port, config, url=None, aliases=None):
    httpd = ArkiServer((host, port), Handler, aliases=aliases)
    if url is not None:
        httpd.url = url
    httpd.set_config(config)
    return httpd


class Server(App):
    """
    Start the arkimet server, serving the datasets found in the configuration file
    """

    @classmethod
    def make_parser(cls) -> argparse.ArgumentParser:
        parser = super().make_parser()
        parser.add_argument("configfile", help="dataset configuration file")
        parser.add_argument(
            "--host", "--hostname", metavar="host", default="", help="interface to listen to. Default: all interfaces"
        )
        parser.add_argument(
            "--port", "-p", metavar="port", type=int, default=8080, help="port to listen not. Default: 8080"
        )
        parser.add_argument("--url", metavar="url", help="url to use to reach the server")

        parser.add_argument("--accesslog", metavar="file", help="file where to log normal access information")
        parser.add_argument(
            "--perflog", metavar="file", help="file where to log query information and performance statistics"
        )
        parser.add_argument("--errorlog", metavar="file", help="file where to log errors")
        parser.add_argument("--syslog", action="store_true", help="log to system log")
        parser.add_argument("--quiet", action="store_true", help="do not log to standard output")
        parser.add_argument(
            "--journald", action="store_true", help="log to standard error in a way compatible with journald"
        )

        parser.add_argument(
            "--runtest", metavar="cmd", help="start the server, run the given test command and return its exit status"
        )
        return parser

    def setup_logging(self):
        root_logger = logging.getLogger()

        if self.args.debug:
            root_logger.setLevel(logging.DEBUG)
        else:
            root_logger.setLevel(logging.INFO)

        if not self.args.quiet:
            h = logging.StreamHandler()
            if self.args.journald:
                h.setFormatter(JournaldFormatter())
            else:
                h.setFormatter(logging.Formatter("%(asctime)-15s %(levelname)s %(message)s"))
            if self.args.debug:
                h.setLevel(logging.DEBUG)
            elif self.args.verbose:
                h.setLevel(logging.INFO)
            else:
                h.setLevel(logging.WARN)
            h.addFilter(DefaultLogFilter(self.args.accesslog))
            root_logger.addHandler(h)

        if self.args.accesslog:
            from logging.handlers import WatchedFileHandler

            h = WatchedFileHandler(self.args.accesslog)
            h.setFormatter(logging.Formatter("%(message)s"))
            h.setLevel(logging.INFO)

            class Filter:
                def filter(self, record):
                    if not getattr(record, "access_log", False):
                        return False
                    return record.levelno < logging.WARN

            h.addFilter(Filter())
            root_logger.addHandler(h)

        if self.args.perflog:
            from logging.handlers import WatchedFileHandler

            h = WatchedFileHandler(self.args.perflog)

            class PerfFormatter(logging.Formatter):
                def format(self, record):
                    import json

                    info = getattr(record, "perf", None)
                    return json.dumps(info, sort_keys=True)

            h.setFormatter(PerfFormatter())
            h.setLevel(logging.INFO)

            class Filter:
                def filter(self, record):
                    return getattr(record, "perf", None) is not None

            h.addFilter(Filter())
            root_logger.addHandler(h)

        if self.args.errorlog:
            from logging.handlers import WatchedFileHandler

            h = WatchedFileHandler(self.args.errorlog)
            h.setFormatter(logging.Formatter("%(asctime)-15s %(levelname)s %(message)s"))
            h.setLevel(logging.WARN)
            root_logger.addHandler(h)

        if self.args.syslog:
            h = logging.SyslogHandler()
            h.setFormatter(logging.Formatter("%(asctime)-15s %(levelname)s %(message)s"))
            root_logger.addHandler(h)

    def run(self):
        super().run()

        logging.info("Reading configuration from %s", self.args.configfile)
        config = arki.cfg.Sections.parse(self.args.configfile)

        httpd = make_server(self.args.host, self.args.port, config, self.args.url)

        try:
            if self.args.runtest:
                exit_code = 0
                args = self.args

                class TestThread(threading.Thread):
                    def run(self):
                        try:
                            subprocess.check_call(args.runtest, shell=True)
                        except subprocess.CalledProcessError:
                            nonlocal exit_code
                            exit_code = 1
                        httpd.shutdown()

                test_thread = TestThread()
                test_thread.start()
                httpd.serve_forever()
                test_thread.join()
                sys.exit(exit_code)
            else:
                httpd.serve_forever()
        finally:
            httpd.server_close()
