from arkimet.bbox import BBox
import arkimet.scan.vm2


def bbox_vm2(area):
    """
    Compute bounding boxes for VM2 areas
    """
    station = arkimet.scan.vm2.get_station(area["id"])
    lat = station.get("lat")
    lon = station.get("lon")

    if lat is not None and lon is not None:
        return [
            (lat / 100000, lon / 100000),
        ]


BBox.register("VM2", bbox_vm2)
