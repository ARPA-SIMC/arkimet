-- Library functions

-- Convert UTM to Lat-Lon (more complete version used for GRIB2)
function utm2ll2(x, y, falseEasting, falseNorthing, zone)
  -- Constants
  local k0=0.9996
  local a=6378206.4
  local e1=0.001697916
  local e11=3.0*e1/2.0 - 27.0*e1*e1*e1/32.0
  local e12=21.0*e1*e1/16.0 - 55.0*e1*e1*e1*e1/32.0
  local e13=151.0*e1*e1*e1/96.0
  local e14=1097.0*e1*e1*e1*e1/512.0
  local e2=0.00676866
  local e4=e2*e2
  local e6=e2*e4
  local ep2=0.0068148
  local rtd=180.0/3.141592654

  local xm = x - falseEasting
  local ym = y - falseNorthing

  -- Central meridian
  local rlon0 = zone*6.0 - 183.0
  local M = ym/k0
  local u = M/(a*(1.0-e2/4.0 - 3.0*e4/64.0 - 5.0*e6/256.0))
  local p1 = u + e11*math.sin(2.0*u) + e12*math.sin(4.0*u) + e13*math.sin(6.0*u)
             + e14*math.sin(8.0*u)
  local cosp1 = math.cos(p1)
  local C1 = ep2*cosp1^2
  local C2 = C1^2
  local tanp1 = math.tan(p1)
  local T1 = tanp1^2
  local T2 = T1^2
  local sinp1 = math.sin(p1)
  local sin2p1 = sinp1^2
  local N1 = a/math.sqrt(1.0-e2*sin2p1)
  local R0 = 1.0-e2*sin2p1
  local R1 = a*(1.0-e2)/math.sqrt(R0^3)

  local D = xm/(N1*k0)
  local D2=D^2
  local D3=D*D2
  local D4=D*D3
  local D5=D*D4
  local D6=D*D5

  local p = p1 - (N1*tanp1/R1) * (D2/2.0 
            - (5.0+3.0*T1+10.0*C1-4.0*C2-9.0*ep2)*D4/24.0
            + (61.0+90.0*T1+298.0*C1+45.0*T2-252*ep2-3.0*C2)*D6/720.0)
  local rlat = rtd*p
  local l = (D - (1.0+2.0*T1+C1)*D3/6.0
            + (5.0-2.0*C1+28*T1-3.0*C2+8.0*ep2+24.0*T2)*D5/120.0)/cosp1
  local rlon = rtd*l + rlon0

  return { rlat, rlon }
end


-- Convert UTM to Lat-Lon
function utm2ll(x, y)
  -- Constants
  local k0=0.9996
  local a=6378206.4
  local e1=0.001697916
  local e11=3.0*e1/2.0 - 27.0*e1*e1*e1/32.0
  local e12=21.0*e1*e1/16.0 - 55.0*e1*e1*e1*e1/32.0
  local e13=151.0*e1*e1*e1/96.0
  local e14=1097.0*e1*e1*e1*e1/512.0
  local e2=0.00676866
  local e4=e2*e2
  local e6=e2*e4
  local ep2=0.0068148
  local false_e=500000.0
  local rtd=180.0/3.141592654

  local xm = 1000.0*x - false_e
  local ym = 1000 * y

  -- Central meridian
  local rlon0 = 32*6.0 - 183.0
  local M = ym/k0
  local u = M/(a*(1.0-e2/4.0 - 3.0*e4/64.0 - 5.0*e6/256.0))
  local p1 = u + e11*math.sin(2.0*u) + e12*math.sin(4.0*u) + e13*math.sin(6.0*u)
             + e14*math.sin(8.0*u)
  local cosp1 = math.cos(p1)
  local C1 = ep2*cosp1^2
  local C2 = C1^2
  local tanp1 = math.tan(p1)
  local T1 = tanp1^2
  local T2 = T1^2
  local sinp1 = math.sin(p1)
  local sin2p1 = sinp1^2
  local N1 = a/math.sqrt(1.0-e2*sin2p1)
  local R0 = 1.0-e2*sin2p1
  local R1 = a*(1.0-e2)/math.sqrt(R0^3)

  local D = xm/(N1*k0)
  local D2=D^2
  local D3=D*D2
  local D4=D*D3
  local D5=D*D4
  local D6=D*D5

  local p = p1 - (N1*tanp1/R1) * (D2/2.0 
            - (5.0+3.0*T1+10.0*C1-4.0*C2-9.0*ep2)*D4/24.0
            + (61.0+90.0*T1+298.0*C1+45.0*T2-252*ep2-3.0*C2)*D6/720.0)
  local rlat = rtd*p
  local l = (D - (1.0+2.0*T1+C1)*D3/6.0
            + (5.0-2.0*C1+28*T1-3.0*C2+8.0*ep2+24.0*T2)*D5/120.0)/cosp1
  local rlon = rtd*l + rlon0

  return { rlat, rlon }
end

function norm_lon(lon)
  if lon > 180 then
    return lon - 360
  else
    return lon
  end
end


-- Bounding box

  ---- Area
  --arki.area.type = grib.dataRepresentationType
  --arki.area.latfirst = grib.latitudeOfFirstGridPoint
  --arki.area.lonfirst = grib.longitudeOfFirstGridPoint
  --
  --if grib.numberOfPointsAlongAParallel then
  --  arki.area.Ni = grib.numberOfPointsAlongAParallel
  --  arki.area.Nj = grib.numberOfPointsAlongAMeridian
  --  arki.area.latlast = grib.latitudeOfLastGridPoint
  --  arki.area.lonlast = grib.longitudeOfLastGridPoint
  --end
  --if grib.numberOfPointsAlongXAxis then
  --  arki.area.Ni = grib.numberOfPointsAlongXAxis
  --  arki.area.Nj = grib.numberOfPointsAlongYAxis
  --end
  ---- Area
  --if grib.angleOfRotationInDegrees then
  --  arki.area.rot = grib.angleOfRotationInDegrees
  --  arki.area.latp = latitudeOfSouthernPoleInDegrees
  --  arki.area.lonp = longitudeOfSouthernPoleInDegrees
  --end

if area.style == "GRIB" then
  a = area.val
  if a.utm == 1 and a.tn==32768 then
      local latfirst = a.latfirst
      local latlast = a.latlast
      local lonfirst = a.lonfirst 
      local lonlast = a.lonlast
      local fe = a.fe
      local fn = a.fn
      local zone = a.zone

      -- Compute the bounding box for the Calmet gribs, that memorize the UTM
      -- coordinates instead of lat and lon
      -- print ("CALMET")
      bbox = {}
      bbox[1] = utm2ll2(lonfirst, latfirst, fe, fn, zone)
      bbox[2] = utm2ll2(lonlast,  latfirst, fe, fn, zone)
      bbox[3] = utm2ll2(lonlast,  latlast, fe, fn, zone)
      bbox[4] = utm2ll2(lonfirst, latlast, fe, fn, zone)
      bbox[5] = bbox[1]
  elseif a.utm == 1 then
      local latfirst = a.latfirst / 1000.0
      local latlast = a.latlast / 1000.0
      local lonfirst = norm_lon(a.lonfirst / 1000.0)
      local lonlast = norm_lon(a.lonlast / 1000.0)

      -- Compute the bounding box for the Calmet gribs, that memorize the UTM
      -- coordinates instead of lat and lon
      -- print ("CALMET")
      bbox = {}
      bbox[1] = utm2ll(lonfirst, latfirst)
      bbox[2] = utm2ll(lonlast,  latfirst)
      bbox[3] = utm2ll(lonlast,  latlast)
      bbox[4] = utm2ll(lonfirst, latlast)
      bbox[5] = bbox[1]
  elseif (a.type == 0 or a.tn == 0) and a.latfirst ~= nil then
      -- Normal latlon
      local latfirst = a.latfirst / 1000000.0
      local latlast = a.latlast / 1000000.0
      local lonfirst = norm_lon(a.lonfirst / 1000000.0)
      local lonlast = norm_lon(a.lonlast / 1000000.0)

      bbox = {}
      bbox[1] = { latfirst, lonfirst }
      bbox[2] = { latlast,  lonfirst }
      bbox[3] = { latlast,  lonlast }
      bbox[4] = { latfirst, lonlast }
      bbox[5] = bbox[1]
  elseif (a.type == 10 or a.tn == 1) and a.latp ~= nil and a.lonp ~= nil and a.rot == 0 then
    -- ...ProjectionInDegrees è grib2
    -- print ("ROTATED")
    local latsp = a.latp / 1000000.0
    local lonsp = a.lonp / 1000000.0
    
    -- Common parameters to unrotate coordinates
    local latsouthpole = math.acos(-math.sin(math.rad(latsp)))
    local cy0 = math.cos(latsouthpole)
    local sy0 = math.sin(latsouthpole)
    local lonsouthpole = lonsp
    
    function r2ll(x, y)
      local x = math.rad(x)
      local y = math.rad(y)
      local rlat = math.asin(sy0 * math.cos(y) * math.cos(x) + cy0 * math.sin(y))
      local lat = math.deg(rlat)
      local lon = lonsouthpole + math.deg(math.asin(math.sin(x) * math.cos(y) / math.cos(rlat)))
      return { lat, lon }
    end
    
    -- Number of points to sample
    local xsamples = math.floor(math.log(a.Ni))
    local ysamples = math.floor(math.log(a.Nj))
    
    bbox = {}
  
    local latmin = a.latfirst / 1000000.0
    local latmax = a.latlast / 1000000.0
    local lonmin = a.lonfirst / 1000000.0
    local lonmax = a.lonlast / 1000000.0
    if lonmax < lonmin then
	    lonsize = lonmax + 360 - lonmin
    else
	    lonsize = lonmax - lonmin
    end
    
    idx = 1
    for i = 0, xsamples do
      bbox[idx] = r2ll(lonmin + lonsize*i/xsamples, latmin)
      idx = idx + 1
    end
    for i = 0, ysamples do
      bbox[idx] = r2ll(lonmax, latmin + (latmax-latmin)*i/ysamples)
      idx = idx + 1
    end
    for i = xsamples,0,-1 do
      bbox[idx] = r2ll(lonmin + lonsize*i/xsamples, latmax)
      idx = idx + 1
    end
    for i = ysamples,0,-1 do
      bbox[idx] = r2ll(lonmin, latmin + (latmax-latmin)*i/ysamples)
      idx = idx + 1
    end
    
    -- Print a polygon definition to be tested in arkimeow
    -- function map(func, array)
    --   local new_array = {}
    --   for i,v in ipairs(array) do
    --     new_array[i] = func(v)
    --   end
    --   return new_array
    -- end
    -- function strgfy(a)
    --   return a[2].." "..a[1]
    -- end
    -- print ("POLYGON(("..table.concat(map(strgfy, bbox),",").."))")
    
    
    -- arki.bbox = { }
    --  r2ll(arki.lonmin, arki.latmin),
    --  r2ll(arki.lonmax, arki.latmin),
    --  r2ll(arki.lonmax, arki.latmax),
    --  r2ll(arki.lonmin, arki.latmax)
    -- }
  elseif a.type == "mob" then
      local x = a.x
      local y = a.y
      bbox = {}
      bbox[1] = { y, x }
      bbox[2] = { y + 1,  x }
      bbox[3] = { y + 1,  x + 1 }
      bbox[4] = { y, x + 1 }
      bbox[5] = bbox[1]
  elseif a.lat ~= nil and a.lon ~= nil then
      bbox = {}
      bbox[1] = { a.lat / 100000, a.lon / 100000 }
  end
elseif area.style == "ODIMH5" then
	a = area.val

	-- print("Calculating bbox for odimh5 area...");

	if a.radius then

		-- estraggo i valori in gradi in notazione decimale
		local LON0		= a.lon / 1000000
		local LAT0		= a.lat / 1000000
		local RADIUS		= a.radius / 1000

		-- print("Calculating bbox for polar area...");
		-- print("LON: " , LON0, " degree");
		-- print("LAT: " , LAT0, " degree");
		-- print("RAD: " , RADIUS, " km");

		-- se e' indicato il raggio allora e' un area polare
		-- quindi creiamo un ottagono che circoscrive la circonfernza 
		-- calcoli sono approssimati, bisognerebbe tenere conto della curvatura terrestre, della differenza nel raggio della terra ecc

		function torad(degree)
			return degree * math.pi / 180 --_PI_180
		end
		function todegree(rad)			
			return rad * 180 / math.pi -- _180_PI
		end
		function moveto(p, km, angle)
			local  lon1 		= torad(p[1])
			local  lat1 		= torad(p[2])
			local  EARTH_RADIUS	= 6372.795477598
			local  d_R		= km / EARTH_RADIUS
			local  sin_d_R		= math.sin(d_R)
			local  cos_d_R		= math.cos(d_R)
			local  sin_lat1		= math.sin(lat1)
			local  cos_lat1		= math.cos(lat1)	
			local  sin_angle 	= math.sin(angle)
			local  cos_angle 	= math.cos(angle)
			local  lat2		=        math.asin (sin_lat1   * cos_d_R + cos_lat1 * sin_d_R * cos_angle)
			local  lon2		= lon1 + math.atan2(sin_angle  * sin_d_R * cos_lat1, cos_d_R - sin_lat1 * sin_lat1)
			return { todegree(lon2) , todegree(lat2) }
		end

		local DIST 	= RADIUS
		local O 	= {0, 0}
		local N 	= moveto(O, DIST,	0)		
		local E 	= moveto(O, DIST,	math.pi / 2)		

		-- print("N:", N[1], ",", N[2])
		-- print("E:", E[1], ",", E[2])
		
		local LATO2  	= 2 / (1 + math.sqrt(2)) / 2		-- meta' del lato di un ottagono		
		local c1	= moveto(N, LATO2 * DIST, math.pi / 2)	-- 0 radianti punta in NORD		
		local c2	= moveto(E, LATO2 * DIST, 0)		-- PI/2 radianti punta a EST
		local c3	= {  c2[1], -c2[2] }
		local c4	= {  c1[1], -c1[2] }
		local c5	= { -c4[1],  c4[2] }
		local c6	= { -c3[1],  c3[2] }
		local c7	= { -c2[1],  c2[2] }
		local c8	= { -c1[1],  c1[2] }

		-- traslo le coordinate rispeto all'origine 
		function add(p1,p2)
			return { p1[1] + p2[1], p1[2] + p2[2] }
		end
		O 		= {LON0, LAT0}
		c1 		= add(c1, O)
		c2 		= add(c2, O)
		c3 		= add(c3, O)
		c4 		= add(c4, O)
		c5 		= add(c5, O)
		c6 		= add(c6, O)
		c7 		= add(c7, O)
		c8 		= add(c8, O)
		
		-- creiamo la boundig box chiusa NOTA: bbox vuole coordinate in LAT,LON!!!
		bbox = {}
		bbox[1] = {c1[2], c1[1]}
		bbox[2] = {c2[2], c2[1]}
		bbox[3] = {c3[2], c3[1]}
		bbox[4] = {c4[2], c4[1]}
		bbox[5] = {c5[2], c5[1]}
		bbox[6] = {c6[2], c6[1]}
		bbox[7] = {c7[2], c7[1]}
		bbox[8] = {c8[2], c8[1]}
		bbox[9] = {c1[2], c1[1]}

		-- function printbbox(val)		
		-- 	print(val[2],",",val[1])
		-- end
		-- printbbox(bbox[1])
		-- printbbox(bbox[2])
		-- printbbox(bbox[3])
		-- printbbox(bbox[4])
		-- printbbox(bbox[5])
		-- printbbox(bbox[6])
		-- printbbox(bbox[7])
		-- printbbox(bbox[8])

	end
end

if area.style == "VM2" then
	if area.dval and area.dval.lat and area.dval.lon then
		bbox = { { area.dval.lat / 100000, area.dval.lon / 100000 } }
	end
end
