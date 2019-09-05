import _arkimet

_export = (
    "Section", "Sections",
)
__all__ = []

for _name in _export:
    _obj = getattr(_arkimet.cfg, _name)
    globals()[_name] = _obj
    __all__.append(_obj.__name__)
