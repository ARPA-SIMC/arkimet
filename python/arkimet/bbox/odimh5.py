from arkimet.bbox import BBox
import math


def bbox_odimh5(area):
    """
    Compute bounding boxes for ODIM areas
    """
    v = area["value"]

    if v.get("radius"):
        # estraggo i valori in gradi in notazione decimale
        LON0 = v["lon"] / 1000000
        LAT0 = v["lat"] / 1000000
        RADIUS = v["radius"] / 1000

        # print("Calculating bbox for polar area...");
        # print("LON: " , LON0, " degree");
        # print("LAT: " , LAT0, " degree");
        # print("RAD: " , RADIUS, " km");

        # se e' indicato il raggio allora e' un area polare
        # quindi creiamo un ottagono che circoscrive la circonfernza
        # calcoli sono approssimati, bisognerebbe tenere conto della curvatura
        # terrestre, della differenza nel raggio della terra ecc

        def torad(degree):
            return degree * math.pi / 180  # _PI_180

        def todegree(rad):
            return rad * 180 / math.pi  # _180_PI

        def moveto(p, km, angle):
            lon1 = torad(p[0])
            lat1 = torad(p[1])
            EARTH_RADIUS = 6372.795477598
            d_R = km / EARTH_RADIUS
            sin_d_R = math.sin(d_R)
            cos_d_R = math.cos(d_R)
            sin_lat1 = math.sin(lat1)
            cos_lat1 = math.cos(lat1)
            sin_angle = math.sin(angle)
            cos_angle = math.cos(angle)
            lat2 = math.asin(sin_lat1 * cos_d_R + cos_lat1 * sin_d_R * cos_angle)
            lon2 = lon1 + math.atan2(sin_angle * sin_d_R * cos_lat1, cos_d_R - sin_lat1 * sin_lat1)
            return todegree(lon2), todegree(lat2)

        DIST = RADIUS
        ORIG = (0, 0)
        N = moveto(ORIG, DIST, 0)
        E = moveto(ORIG, DIST, math.pi / 2)

        # print("N:", N[0], ",", N[1])
        # print("E:", E[0], ",", E[1])

        LATO2 = 2 / (1 + math.sqrt(2)) / 2  # meta' del lato di un ottagono
        c1 = moveto(N, LATO2 * DIST, math.pi / 2)  # 0 radianti punta in NORD
        c2 = moveto(E, LATO2 * DIST, 0)  # PI/2 radianti punta a EST
        c3 = (+c2[0], -c2[1])
        c4 = (+c1[0], -c1[1])
        c5 = (-c4[0], +c4[1])
        c6 = (-c3[0], +c3[1])
        c7 = (-c2[0], +c2[1])
        c8 = (-c1[0], +c1[1])

        # traslo le coordinate rispeto all'origine
        def add(p1, p2):
            return p1[0] + p2[0], p1[1] + p2[1]

        ORIG = (LON0, LAT0)
        c1 = add(c1, ORIG)
        c2 = add(c2, ORIG)
        c3 = add(c3, ORIG)
        c4 = add(c4, ORIG)
        c5 = add(c5, ORIG)
        c6 = add(c6, ORIG)
        c7 = add(c7, ORIG)
        c8 = add(c8, ORIG)

        # creiamo la boundig box chiusa NOTA: bbox vuole coordinate in LAT,LON!!!
        return [
            (c1[1], c1[0]),
            (c2[1], c2[0]),
            (c3[1], c3[0]),
            (c4[1], c4[0]),
            (c5[1], c5[0]),
            (c6[1], c6[0]),
            (c7[1], c7[0]),
            (c8[1], c8[0]),
            (c1[1], c1[0]),
        ]


BBox.register("ODIMH5", bbox_odimh5)
