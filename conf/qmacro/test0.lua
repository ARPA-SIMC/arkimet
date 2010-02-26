ds = qmacro:dataset("testds")

count1 = 0
ds:queryData({matcher=""}, function (md)
	count1 = count1 + 1
	return true
end)

count2 = 0
ds:queryData({matcher="origin:GRIB1,200"}, function (md)
	count2 = count2 + 1
	return true
end)

-- ds:querySummary("reftime:>=today")
