# python 3.7+ from __future__ import annotations


class Querymacro:
    def __init__(self, session, macro_args, query):
        self.session = session
        self.macro_args = macro_args
        self.query = query
        self.ds = self.session.dataset_reader(name=query)

    def query_data(self, matcher=None, with_data=False, sort=None, on_metadata=None):
        return self.ds.query_data(matcher=matcher, with_data=with_data, sort=sort, on_metadata=on_metadata)

    def query_summary(self, matcher, summary):
        return self.ds.query_summary(matcher, summary)
