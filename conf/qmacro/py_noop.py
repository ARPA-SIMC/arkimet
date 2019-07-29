import arkimet as arki


class Querymacro:
    def __init__(self, macro_cfg, datasets_cfg, macro_args, query):
        self.macro_cfg = macro_cfg,
        self.datasets_cfg = datasets_cfg
        self.macro_args = macro_args
        self.query = query
        self.ds = arki.dataset.Reader(datasets_cfg[query])

    def query_data(self, matcher=None, with_data=False, sort=None, on_metadata=None):
        return self.ds.query_data(matcher=matcher, with_data=with_data, sort=sort, on_metadata=on_metadata)

    def query_summary(self, q, sum):
        return self.ds.query_summary(q, sum)
