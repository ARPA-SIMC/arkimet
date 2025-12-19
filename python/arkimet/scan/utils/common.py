import math


def utm2ll2(x, y, falseEasting, falseNorthing, zone):
    """
    Convert UTM to Lat-Lon (more complete version used for GRIB2)
    """
    # Constants
    k0 = 0.9996
    a = 6378206.4
    e1 = 0.001697916
    e11 = 3.0 * e1 / 2.0 - 27.0 * e1 * e1 * e1 / 32.0
    e12 = 21.0 * e1 * e1 / 16.0 - 55.0 * e1 * e1 * e1 * e1 / 32.0
    e13 = 151.0 * e1 * e1 * e1 / 96.0
    e14 = 1097.0 * e1 * e1 * e1 * e1 / 512.0
    e2 = 0.00676866
    e4 = e2 * e2
    e6 = e2 * e4
    ep2 = 0.0068148
    rtd = 180.0/3.141592654

    xm = x - falseEasting
    ym = y - falseNorthing

    # Central meridian
    rlon0 = zone * 6.0 - 183.0
    M = ym / k0
    u = M / (a * (1.0 - e2 / 4.0 - 3.0 * e4 / 64.0 - 5.0 * e6 / 256.0))
    p1 = u + e11 * math.sin(2.0 * u) + e12 * math.sin(4.0 * u) + e13 * math.sin(6.0 * u) + e14 * math.sin(8.0 * u)
    cosp1 = math.cos(p1)
    C1 = ep2 * cosp1**2
    C2 = C1**2
    tanp1 = math.tan(p1)
    T1 = tanp1**2
    T2 = T1**2
    sinp1 = math.sin(p1)
    sin2p1 = sinp1**2
    N1 = a / math.sqrt(1.0 - e2 * sin2p1)
    R0 = 1.0 - e2 * sin2p1
    R1 = a * (1.0 - e2) / math.sqrt(R0**3)

    D = xm / (N1 * k0)
    D2 = D**2
    D3 = D * D2
    D4 = D * D3
    D5 = D * D4
    D6 = D * D5

    p = (p1
         - (N1 * tanp1 / R1)
         * (D2 / 2.0 - (5.0 + 3.0 * T1 + 10.0 * C1 - 4.0 * C2 - 9.0 * ep2) * D4 / 24.0
            + (61.0 + 90.0 * T1 + 298.0 * C1 + 45.0 * T2 - 252 * ep2 - 3.0 * C2)
            * D6 / 720.0)
         )
    rlat = rtd * p
    ltmp = (D - (1.0 + 2.0 * T1 + C1) * D3 / 6.0
            + (5.0 - 2.0 * C1 + 28 * T1 - 3.0 * C2 + 8.0 * ep2 + 24.0 * T2) * D5 / 120.0) / cosp1
    rlon = rtd * ltmp + rlon0

    return (rlat, rlon)


def utm2ll(x, y):
    """
    Convert UTM to Lat-Lon
    """
    # Constants
    k0 = 0.9996
    a = 6378206.4
    e1 = 0.001697916
    e11 = 3.0 * e1/2.0 - 27.0 * e1 * e1 * e1 / 32.0
    e12 = 21.0 * e1 * e1 / 16.0 - 55.0 * e1 * e1 * e1 * e1 / 32.0
    e13 = 151.0 * e1 * e1 * e1 / 96.0
    e14 = 1097.0 * e1 * e1 * e1 * e1 / 512.0
    e2 = 0.00676866
    e4 = e2 * e2
    e6 = e2 * e4
    ep2 = 0.0068148
    false_e = 500000.0
    rtd = 180.0 / 3.141592654

    xm = 1000.0 * x - false_e
    ym = 1000 * y

    # Central meridian
    rlon0 = 32 * 6.0 - 183.0
    M = ym / k0
    u = M / (a * (1.0 - e2 / 4.0 - 3.0 * e4 / 64.0 - 5.0 * e6 / 256.0))
    p1 = u + e11 * math.sin(2.0*u) + e12 * math.sin(4.0 * u) + e13 * math.sin(6.0 * u) + e14 * math.sin(8.0 * u)
    cosp1 = math.cos(p1)
    C1 = ep2 * cosp1**2
    C2 = C1**2
    tanp1 = math.tan(p1)
    T1 = tanp1**2
    T2 = T1**2
    sinp1 = math.sin(p1)
    sin2p1 = sinp1**2
    N1 = a / math.sqrt(1.0 - e2 * sin2p1)
    R0 = 1.0 - e2 * sin2p1
    R1 = a * (1.0 - e2) / math.sqrt(R0**3)

    D = xm / (N1 * k0)
    D2 = D**2
    D3 = D * D2
    D4 = D * D3
    D5 = D * D4
    D6 = D * D5

    p = (p1 - (N1 * tanp1 / R1) * (D2 / 2.0
         - (5.0 + 3.0 * T1 + 10.0 * C1 - 4.0 * C2 - 9.0 * ep2) * D4 / 24.0
         + (61.0 + 90.0 * T1 + 298.0 * C1 + 45.0 * T2 - 252 * ep2 - 3.0 * C2) * D6 / 720.0)
         )
    rlat = rtd * p
    ltmp = (D - (1.0 + 2.0 * T1 + C1) * D3 / 6.0
            + (5.0 - 2.0 * C1 + 28 * T1 - 3.0 * C2 + 8.0 * ep2 + 24.0 * T2) * D5 / 120.0) / cosp1
    rlon = rtd * ltmp + rlon0
    return (rlat, rlon)


def norm_lon(lon):
    if lon > 180:
        return lon - 360
    else:
        return lon
