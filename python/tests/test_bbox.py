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
        self.assertEqual(list(shape.exterior.coords), [
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
        self.assertEqual(list(shape.exterior.coords), [
            (3.3240791837842467, 43.686350231261166),
            (8.844543730483698, 43.82741288567573),
            (8.838214922274625, 46.12292397275863),
            (3.094605819690117, 45.97020367068632),
            (3.3240791837842467, 43.686350231261166),
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
        self.assertEqual(list(shape.exterior.coords), [
            (6.012435066304445, 35.47226591257227),
            (8.128045092686419, 35.552381143086215),
            (10.247118590504117, 35.57460592913863),
            (12.365734218925105, 35.538868787675696),
            (14.47997658675164, 35.445284605728006),
            (16.58598691095113, 35.29415281919491),
            (18.680011894214072, 35.08595270343381),
            (18.680011894214072, 35.08595270343381),
            (19.061419189065354, 37.27693711451049),
            (19.465699429949204, 39.46662451211235),
            (19.896252566122747, 41.654821151172655),
            (20.35709162869118, 43.84129806331103),
            (20.852994104979892, 46.02578227347583),
            (21.389699305289877, 48.20794531236258),
            (21.389699305289877, 48.20794531236258),
            (18.65604555932061, 48.4807649800572),
            (15.895141316676515, 48.6792845570402),
            (13.115366994809433, 48.80241791791446),
            (10.325516720484007, 48.84948091031436),
            (7.534609452299553, 48.820209893446105),
            (4.751687893514312, 48.71476898033081),
            (4.751687893514312, 48.71476898033081),
            (5.002484238080096, 46.50873491217853),
            (5.233705730556198, 44.30219549014641),
            (5.448166560317644, 42.09522345890339),
            (5.648195368428386, 39.88787899105973),
            (5.8357379278170605, 37.68021235055321),
            (6.012435066304445, 35.47226591257227),
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
        self.assertEqual(list(shape.exterior.coords), [
            (13.99630132204117, 43.718170709677985),
            (19.45328777706591, 43.34692651332638),
            (19.868920680403313, 45.60297865483628),
            (14.198614863489942, 46.00464847518428),
            (13.99630132204117, 43.718170709677985),
        ])
