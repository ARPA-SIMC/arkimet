import unittest
import arkimet as arki
from arkimet.test import skip_unless_geos
try:
    import shapely
    import shapely.wkt

    def skip_unless_shapely():
        pass
except ImportError:
    def skip_unless_shapely():
        raise unittest.SkipTest("shapely support not available")


def test_coord_list(coords):
    res = []
    for x, y in coords:
        res.append((
            round(x * 10000) / 10000,
            round(y * 10000) / 10000,
        ))
    return res


class TestBBox(unittest.TestCase):
    def test_empty(self):
        """
        Empty or unsupported area should give None
        """
        bbox = arki.BBox()
        res = bbox.compute({"type": "area", "style": "GRIB", "value": {}})
        self.assertIsNone(res)

    def test_latlon(self):
        """
        Test normal latlon areas
        """
        skip_unless_geos()
        skip_unless_shapely()

        bbox = arki.BBox()
        res = bbox.compute({"type": "area", "style": "GRIB", "value": {
            "type": 0,
            "Ni": 97, "Nj": 73,
            "latfirst": 40000000, "latlast": 46000000,
            "lonfirst": 12000000, "lonlast": 20000000,
        }})

        self.assertIsNotNone(res)

        shape = shapely.wkt.loads(res)
        self.assertEqual(shape.geom_type, "Polygon")
        self.assertEqual(test_coord_list(shape.exterior.coords), [
            (12.0, 40.0),
            (12.0, 46.0),
            (20.0, 46.0),
            (20.0, 40.0),
            (12.0, 40.0)
        ])

    def test_utm(self):
        """
        Test UTM areas
        """
        skip_unless_geos()
        skip_unless_shapely()

        bbox = arki.BBox()
        res = bbox.compute({"type": "area", "style": "GRIB", "value": {
            "type": 0, "utm": 1,
            "Ni": 90, "Nj": 52,
            "latfirst": 4852500, "latlast": 5107500,
            "lonfirst": 402500, "lonlast": 847500,
        }})

        self.assertIsNotNone(res)

        shape = shapely.wkt.loads(res)
        self.assertEqual(shape.geom_type, "Polygon")
        # These are the values computed with the old BB algorithm that however
        # only produced rectangles.
        self.assertEqual(test_coord_list(shape.exterior.coords), [
            (3.3241, 43.6864),
            (8.8445, 43.8274),
            (8.8382, 46.1229),
            (3.0946, 45.9702),
            (3.3241, 43.6864),
        ])

    def test_rotated(self):
        """
        Test rotated latlon areas
        """
        skip_unless_geos()
        skip_unless_shapely()

        bbox = arki.BBox()
        res = bbox.compute({"type": "area", "style": "GRIB", "value": {
            "Ni": 447, "Nj": 532,
            "type": 10, "rot": 0,
            "latfirst": -21925000, "latlast": -8650000,
            "lonfirst": -3500000, "lonlast": 7650000,
            "latp": -32500000, "lonp": 10000000,
        }})

        self.assertIsNotNone(res)

        shape = shapely.wkt.loads(res)
        self.assertEqual(shape.geom_type, "Polygon")
        # These are the values computed with the old BB algorithm that however
        # only produced rectangles.
        self.assertEqual(test_coord_list(shape.exterior.coords), [
            (06.0124, 35.4723),
            (08.1280, 35.5524),
            (10.2471, 35.5746),
            (12.3657, 35.5389),
            (14.4800, 35.4453),
            (16.5860, 35.2942),
            (18.6800, 35.0860),
            (18.6800, 35.0860),
            (19.0614, 37.2769),
            (19.4657, 39.4666),
            (19.8963, 41.6548),
            (20.3571, 43.8413),
            (20.8530, 46.0258),
            (21.3897, 48.2079),
            (21.3897, 48.2079),
            (18.6560, 48.4808),
            (15.8951, 48.6793),
            (13.1154, 48.8024),
            (10.3255, 48.8495),
            (07.5346, 48.8202),
            (04.7517, 48.7148),
            (04.7517, 48.7148),
            (05.0025, 46.5087),
            (05.2337, 44.3022),
            (05.4482, 42.0952),
            (05.6482, 39.8879),
            (05.8357, 37.6802),
            (06.0124, 35.4723),
        ])

    def test_latlon_digits(self):
        """
        Test that latlon areas are reported with the right amount of significant digits
        """
        skip_unless_geos()
        skip_unless_shapely()

        bbox = arki.BBox()
        res = bbox.compute({"type": "area", "style": "GRIB", "value": {
            "type": 0,
            "Ni": 135, "Nj": 98,
            "latfirst": 49200000, "latlast": 34650000,
            "lonfirst": 2400000, "lonlast": 22500000,
        }})

        self.assertIsNotNone(res)

        shape = shapely.wkt.loads(res)
        self.assertEqual(shape.geom_type, "Polygon")
        self.assertEqual(list(shape.exterior.coords), [
            (2.4, 49.2),
            (2.4, 34.65),
            (22.5, 34.65),
            (22.5, 49.2),
            (2.4, 49.2)
        ])

    def test_bufr_mobile(self):
        """
        Simplified BUFR mobile station areas
        """
        skip_unless_geos()
        skip_unless_shapely()

        bbox = arki.BBox()
        res = bbox.compute({"type": "area", "style": "GRIB", "value": {
            "type": "mob", "x": 11, "y": 45,
        }})

        self.assertIsNotNone(res)

        shape = shapely.wkt.loads(res)
        self.assertEqual(shape.geom_type, "Polygon")
        self.assertEqual(list(shape.exterior.coords), [
            (11.0, 45.0),
            (11.0, 46.0),
            (12.0, 46.0),
            (12.0, 45.0),
            (11.0, 45.0)
        ])

    def test_experimental_utm(self):
        """
        Experimental UTM areas
        """
        skip_unless_geos()
        skip_unless_shapely()

        bbox = arki.BBox()
        res = bbox.compute({"type": "area", "style": "GRIB", "value": {
            "fe": 0, "fn": 0,
            "latfirst": 4852500, "latlast": 5107500,
            "lonfirst": 402500, "lonlast": 847500,
            "tn": 32768, "utm": 1, "zone": 32,
        }})

        self.assertIsNotNone(res)

        shape = shapely.wkt.loads(res)
        self.assertEqual(shape.geom_type, "Polygon")
        self.maxDiff = None
        self.assertEqual(test_coord_list(shape.exterior.coords), [
            (13.9963, 43.7182),
            (19.4533, 43.3469),
            (19.8689, 45.6030),
            (14.1986, 46.0046),
            (13.9963, 43.7182),
        ])
