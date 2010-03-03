-- Build the merged query
-- linepat = "^ds:(.-)%.%s+d:(.-)%.%s+t:(.-)%.%s+s:(.-)%.%s+l:(.-)%.%s+v:(.-)%.$"
linepat =    "^ds:(.-)%.%s+d:(.-)%.%s+t:(.-)%.%s+s:(.-)%.%s+l:(.-)%.%s+v:(.-)%.%s*"

Set = {}

function Set:new(o)
	o = o or {}
	setmetatable(o, self)
	self.__index = self
	return o
end

function Set:parse(s)
	set = Set:new()
	for name in s:gmatch("%s*([^,]+)%s*") do
		set[name:gsub("/",",")] = true
	end
	return set
end

function Set:addset(set)
	for k, v in pairs(set) do
		self[k] = true
	end
end

function Set:orexpr()
	res = ""
	first = true
	for k, v in pairs(self) do
		if first then
			res = k
			first = false
		else
			res = res .. " or " .. k
		end
	end
	return res
end

function buildmatcher(s, l, v)
	query = {}
	table.insert(query, "timerange:" .. s:orexpr())
	table.insert(query, "level:" .. l:orexpr())
	table.insert(query, "product:" .. v:orexpr())
	return arki.matcher.new(table.concat(query, "; "))
end


DSInfo = {}

function DSInfo:new(o)
	o = o or {}
	setmetatable(o, self)
	self.__index = self
	return o
end

function DSInfo:init(dsname)
	local ds = qmacro:dataset(dsname)
	if ds == nil then error("Cannot access dataset " .. dsname) end
	self["dataset"] = ds
	local gq = arki.gridquery.new(ds)
	self["gq"] = gq
end


-- Cache of summary information
dsinfo = {}

-- Parse input
all_ds = Set:new()
all_s = Set:new()
all_l = Set:new()
all_v = Set:new()
for line in query:gmatch("[^\r\n]+") do
	ds, d, t, s, l, v = line:match(linepat)
	if ds ~= nil then
		ds = Set:parse(ds)
		d = Set:parse(d)
		s = Set:parse(s)
		l = Set:parse(l)
		v = Set:parse(v)

		for name, _ in pairs(ds) do
			info = dsinfo[name]
			if dsinfo[name] == nil then
				info = DSInfo:new()
				info:init(name)
				dsinfo[name] = info
			end
			q = buildmatcher(s, l, v)
			-- print("ADD", name, q)
			info.gq:add(q)

			-- Add times
			for k, _ in pairs(d) do
				if k == "@" then
					-- Expand '@' in yesterday
					yesterday = os.time() - (3600*24)
					k = os.date("%Y-%m-%d 00:00:00", yesterday)
				end
				info.gq:addtime(k)
			end
		end

		all_ds:addset(ds)
		all_s:addset(s)
		all_l:addset(l)
		all_v:addset(v)
	else
		print("line not parsed:", line)
	end
end

-- Consolidate the query grids
for name, info in pairs(dsinfo) do
	info.gq:consolidate()
	-- print ("MQ", name, info.gq:mergedquery())
end

function queryData(q, cons)
	for name, info in pairs(dsinfo) do
		-- Build the merged query
		query = info.gq:mergedquery()
		-- print (query)
		-- print (query:expanded())

		-- Query dataset storing results
		mds = {}
		info.dataset:queryData({matcher=query}, function(md)
			if info.gq:checkandmark(md) then
				table.insert(mds, md:copy())
			end
			return true
		end)

		if info.gq:satisfied() then
			for idx, md in ipairs(mds) do
				cons(md)
			end
		else
			print("Master query: " .. tostring(query))
			print(info.gq:dump())
			error("Query cannot be satisfied on dataset " .. name)
		end
	end
end

function querySummary(q, sum)
	s = arki.summary.new()
	for name, info in pairs(dsinfo) do
		-- Build the merged query
		query = info.gq:mergedquery()
		-- print (query)
		-- print (query:expanded())

		-- Query dataset merging all summaries
		info.dataset:querySummary(query, s)
	end
	s:filter("", sum)
end
