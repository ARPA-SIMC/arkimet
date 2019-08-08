from arkimet.bbox import BBox


def bbox_vm2(area):
    """
    Compute bounding boxes for VM2 areas
    """
    if area.dval and area.dval.lat and area.dval.lon:
        return [
            (area.dval.lat / 100000, area.dval.lon / 100000),
        ]


BBox.register("VM2", bbox_vm2)
