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

MatchGrid = {}

function MatchGrid:new(o)
	o = o or {}
	setmetatable(o, self)
	self.__index = self
	return o
end

function MatchGrid:add(matcher)
	self[matcher] = false
end

function MatchGrid:reset()
	for k, v in pairs(self) do
		self[k] = false
	end
end

function MatchGrid:acquire(md)
	found = nil
	for k, v in pairs(self) do
		if k:match(md) then
			found = k
			break
		end
	end
	if found ~= nil then
		if self[found] then
			error("Result conflict for " .. found)
		else
			self[found] = md:copy()
		end
	end
end

function MatchGrid:satisfied()
	for k, v in pairs(self) do
		if not v then return false end
	end
	return true
end

function MatchGrid:feed(cons)
	for k, v in pairs(self) do
		if v then
			if not cons(v) then break end
		end
	end
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
	self["dataset"] = ds
	local s = arki.summary.new()
	ds:querySummary("", s)
	self["summary"] = s
end


-- Cache of summary information
dsinfo = {}

-- Parse input
all_ds = Set:new()
all_s = Set:new()
all_l = Set:new()
all_v = Set:new()
all_matchers = MatchGrid:new()
for line in query:gmatch("[^\r\n]+") do
	ds, d, t, s, l, v = line:match(linepat)
	if ds ~= nil then
		ds = Set:parse(ds)

		for name, _ in pairs(ds) do
			info = dsinfo[name]
			if dsinfo[name] == nil then
				info = DSInfo:new()
				info:init(name)
				dsinfo[name] = info
			end
		end

		s = Set:parse(s)
		l = Set:parse(l)
		v = Set:parse(v)
		all_ds:addset(ds)
		all_s:addset(s)
		all_l:addset(l)
		all_v:addset(v)
		all_matchers:add(buildmatcher(s, l, v))
	else
		print("line not parsed:", line)
	end
end

-- Build the merged query
query = buildmatcher(all_s, all_l, all_v)
-- print (query)
-- print (query:expanded())

function queryData(q, cons)
	for k, v in pairs(all_ds) do
		-- Query dataset storing results
		ds = qmacro:dataset(k)
		all_matchers:reset()
		if ds ~= nil then
			ds:queryData({matcher=query}, function(md)
				all_matchers:acquire(md)
				return true
			end)

			-- If results are good, output them and stop here
			if all_matchers:satisfied() then
				all_matchers:feed(cons)
				break
			end
		end
	end
end

function querySummary(q, sum)
	for k, v in pairs(all_ds) do
		-- Query dataset storing results
		ds = qmacro:dataset(k)
		if ds ~= nil then
			s = arki.summary.new()
			ds:querySummary(query, s)

			if s:count() > 0 then
				s:filter("", sum)
				break
			end
		end
	end
end
