from typing import Dict, Any, Callable, Optional, List, Tuple
from collections import defaultdict
import logging

log = logging.getLogger("arkimet.bbox")


class BBox:
    by_style = defaultdict(list)

    def compute(self, t: Dict[str, Any]) -> List[Tuple[float, float]]:
        # Find the formatter list for this style
        bboxes = self.by_style.get(t["style"])
        if bboxes is None:
            return None

        # Try all bbox functions in the list, returning the result of the first
        # that returns not-None.
        # Iterate in reverse order, so that bbox functions loaded later (like
        # from /etc) can be called earlier and fall back on the shipped ones
        for bbox in reversed(bboxes):
            try:
                res = bbox(t)
            except Exception:
                log.exception("bbox function failed")
                res = None

            if res is not None:
                return res

        # Otherwise return None
        return None

    @classmethod
    def register(cls, style: str, bbox: Callable[[Dict[str, Any]], Optional[List[Tuple[float, float]]]]):
        if bbox not in cls.by_style[style]:
            cls.by_style[style].append(bbox)
