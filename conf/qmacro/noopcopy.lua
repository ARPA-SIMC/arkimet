-- Pass through
ds = qmacro:dataset(data)

function queryData(q, cons)
	mds = {}
	ds:queryData(q, function(md)
		m = md:copy()
		table.insert(mds, m)
		return true
	end)
	for idx, m in pairs(mds) do
		if not cons(m) then break end
	end
end

function querySummary(q, sum)
	s = sum:copy()
	ds:querySummary(q, s)
	s:output(sum)
end


-- count1 = 0
-- ds:queryData({matcher=""}, function (md)
-- 	count1 = count1 + 1
-- 	return true
-- end)
-- 
-- count2 = 0
-- ds:queryData({matcher="origin:GRIB1,200"}, function (md)
-- 	count2 = count2 + 1
-- 	return true
-- end)

-- ds:querySummary("reftime:>=today")
