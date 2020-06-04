from __future__ import annotations


class Querymacro:
    def __init__(self, session, macro_args, query):
        self.session = session
        self.macro_args = macro_args
        self.query = query
        self.ds = self.session.dataset_reader(name=query)

    def query_data(self, matcher=None, with_data=False, on_metadata=None):
        mds = self.ds.query_data(matcher=matcher, with_data=with_data)
        for md in mds:
            if not on_metadata(md):
                return False

    def query_summary(self, matcher, summary):
        return self.ds.query_summary(matcher, summary)
