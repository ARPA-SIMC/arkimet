import _arkimet

_export = (
    "plain_data_read_count", "gzip_data_read_count", "gzip_forward_seek_bytes",
    "gzip_idx_reposition_count", "acquire_single_count", "acquire_batch_count",
)
__all__ = []

for _name in _export:
    _obj = getattr(_arkimet.counters, _name)
    globals()[_name] = _obj
    __all__.append(_obj)
