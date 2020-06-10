# python 3.7+ from __future__ import annotations
import re
import datetime


DATASET_ALIASES = {
    "LMSMR": ["lmsmr4x52", "lmsmr4x55", "lmsmr4x53"],
    "cosmo_5M_ita_any": ["cosmo_5M_ita"],
}


class Row:
    def __init__(self, lineno, line, reftime, dsname, matcher):
        self.lineno = lineno
        self.line = line
        self.reftime = reftime
        self.dsname = dsname
        self.matcher = matcher


class Querymacro:
    re_line = re.compile(r"""
        ^
        ds:(?P<ds>[^.]+)\.\s+  # Dataset
        d:(?P<d>[^.]+)\.\s+    # Date
        t:(?P<t>[^.]+)\.\s+    # Time
        s:(?P<s>[^.]+)\.\s+    # Timerange
        l:(?P<l>[^.]+)\.\s+    # Level
        v:(?P<v>[^.]+)\.\s*    # Product
        $""", re.X)

    re_date_today = re.compile(r"^[Tt]$")
    re_date_today_relative = re.compile(r"^[Tt]\s*(?:(?P<dir>[+-])\s*(?P<days>\d+))?$")
    re_date_relative = re.compile(r"^@\s*(?:(?P<dir>[+-])\s*(?P<days>\d+))?$")
    re_date_iso = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    re_time = re.compile(r"^(?P<h>\d{2})(?P<m>\d{2})$")

    def __init__(self, session, macro_args, query):
        self.session = session

        # The argument, if provided, is a date used to expand @ in date expressions
        macro_args = macro_args.strip()
        if macro_args:
            self.date_ref = datetime.datetime.strptime(macro_args, "%Y-%m-%d").date()
        else:
            self.date_ref = None

        # Rows to query
        self.rows = []

        # Parse query
        for idx, line in enumerate(query.splitlines(), start=1):
            mo = self.re_line.match(line)
            if not mo:
                raise RuntimeError("query:{}: line not parsed: {}".format(idx, repr(line)))

            dsname = mo.group("ds")
            reftime = datetime.datetime.combine(
                    self._to_date(mo.group("d")),
                    self._to_time(mo.group("t")))

            matcher = self.session.matcher(
                "reftime: ={reftime:%Y-%m-%dT%H:%M:%S}Z; "
                "timerange: {trange}; level: {level}; product: {product}".format(
                    reftime=reftime,
                    trange=self._to_matcher(mo.group("s")),
                    level=self._to_matcher(mo.group("l")),
                    product=self._to_matcher(mo.group("v"))
                )
            )

            self.rows.append(Row(idx, line, reftime, dsname, matcher))

        # Sort rows by reftime
        self.rows.sort(key=lambda x: x.reftime)

        # Cache open readers for reuse
        self.readers = {}

    def get_reader(self, name):
        res = self.readers.get(name)
        if res is None:
            res = self.session.dataset_reader(name=name)
            self.readers[name] = res
        return res

    def row_to_md(self, row, with_data):
        datasets = DATASET_ALIASES.get(row.dsname)
        if datasets is None:
            datasets = [row.dsname]
        for ds in datasets:
            reader = self.get_reader(ds)
            mds = reader.query_data(matcher=row.matcher, with_data=with_data)
            if not mds:
                raise RuntimeError("row {}:{} did not produce any results".format(row.lineno, repr(row.line)))
            if len(mds) > 1:
                raise RuntimeError("row {}:{} produced {} results instead of one".format(
                    row.lineno, repr(row.line), len(mds)))
            return mds[0]

    def query_data(self, matcher=None, with_data=False, on_metadata=None):
        # Note: matcher and sort are currently ignored
        # FIXME: change the api to remove them?

        res = []
        for row in self.rows:
            res.append(self.row_to_md(row, with_data))

        if on_metadata:
            for md in res:
                on_metadata(md)
        else:
            return res

    def _to_matcher(self, s):
        return s.replace("/", ",")

    def _to_date(self, d):
        d = d.strip()

        # Today
        mo = self.re_date_today.match(d)
        if mo:
            return datetime.date.today()

        # Relative to today
        mo = self.re_date_today_relative.match(d)
        if mo:
            if mo.group("dir") is None:
                return datetime.date.today()
            elif mo.group("dir") == "+":
                return datetime.date.today() + datetime.timedelta(days=int(mo.group("days")))
            else:
                return datetime.date.today() - datetime.timedelta(days=int(mo.group("days")))

        # Relative to argument
        mo = self.re_date_relative.match(d)
        if mo:
            if not self.date_ref:
                raise RuntimeError("if using @, please invoke as --qmacro=\"expa YYYY-MM-DD\"")

            if mo.group("dir") is None:
                return self.date_ref
            if mo.group("dir") == "+":
                return self.date_ref + datetime.timedelta(days=int(mo.group("days")))
            else:
                return self.date_ref - datetime.timedelta(days=int(mo.group("days")))

        # YYYY-MM-DD
        mo = self.re_date_iso.match(d)
        if mo:
            return datetime.datetime.strptime(d, "%Y-%m-%d").date()

        raise RuntimeError("date expression {} not recognised".format(repr(d)))

    def _to_time(self, t):
        mo = self.re_time.match(t)
        if not mo:
            raise RuntimeError("time {} is not in form HHMM".format(repr(t)))

        return datetime.time(int(mo.group("h")), int(mo.group("m")))
