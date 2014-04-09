-- Build the merged query
linepat = "^ds:(.-)%.%s+d:(.-)%.%s+t:(.-)%.%s+s:(.-)%.%s+l:(.-)%.%s+v:(.-)%.%s*$"

dataset_aliases = {
	LMSMR = { "lmsmr4x56", "lmsmr4x55", "lmsmr4x57" }
}

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


QueryChunk = {}

function QueryChunk:new(o)
	o = o or {}
	setmetatable(o, self)
	self.__index = self
	return o
end

function QueryChunk:init(dsname, d, t)
	self["dsname"] = dsname
	self["date"] = d
	self["time"] = t
	self["queries"] = {}
	-- Parse date
	if d:match("^%s*[Tt]%s*$") then
		d = os.date("%Y-%m-%d")
	elseif d:match("^%s*@%s*[+-]?%s*%d*$") then
		if args == nil then error("If using @, please invoke as --qmacro=\"expa YYYY-MM-DD\"") end
		pm, val = d:match("^%s*@%s*([+-])%s*(%d+)%s*$")
		d = args
		if pm ~= nil then
			rt = os.date("*t")
			rt["year"], rt["month"], rt["day"] = d:match("^(%d%d%d%d)-(%d%d)-(%d%d)$")
			if rt["year"] == nil then error("If using @, please invoke as --qmacro=\"expa YYYY-MM-DD\"") end
			if pm == "+" then
				ts = os.time(rt) + 3600 * 24 * val
			else
				ts = os.time(rt) - 3600 * 24 * val
			end
			d = os.date("%Y-%m-%d", ts)
		end
	elseif d:match("^%d+-%d+-%d+$") then
		-- d is already as we want it
	else
		pm, val = d:match("^%s*[Tt]%s*([+-])%s*(%d+)%s*$")
		if pm == nil then error("Expected 'T+n' or 'T-n'") end
		if pm == "+" then
			ts = os.time() + 3600 * 24 * val
		else
			ts = os.time() - 3600 * 24 * val
		end
		d = os.date("%Y-%m-%d", ts)
	end

	-- Parse time
	h, m = t:match("(%d%d)(%d%d)")
	if h == nil then error("Time expected in form HHMM") end

	self.timestr = d .. string.format(" %02d:%02d:00", h, m)
end

function QueryChunk:addquery(q)
	table.insert(self.queries, q)
end

function QueryChunk:datasets()
	local res = dataset_aliases[self.dsname]
	if res ~= nil then return res end
	return { self.dsname }
end

-- Feed all information we have into a GridQuery
function QueryChunk:setupgq(gq)
	-- Add queries
	for _, q in ipairs(self.queries) do
		gq:add(q)
	end

	-- Add time
	gq:addtime(self.timestr)

	-- Consolidate in the end
	gq:consolidate()

	if debug then
		io.stderr:write("Master query: ", tostring(gq:mergedquery()), "\n")
		io.stderr:write("Result space:\n")
		io.stderr:write(gq:dump(), "\n")
	end
end

function QueryChunk:checkData(cons)
	for idx, dsname in ipairs(self:datasets()) do
		if verbose then io.stderr:write("Trying query on ", dsname, "\n") end

		local ds = qmacro:dataset(dsname)
		if ds == nil then error("Cannot access dataset " .. dsname) end
		local gq = arki.gridquery.new(ds)

		-- Prepare the GridQuery
		self:setupgq(gq)

		if gq:expecteditems() == 0 then
			if verbose then
                                io.stderr:write("No results expected from ", dsname, "\n")
                        end
		else
			-- Build the merged query
			query = gq:mergedquery()
			-- print (query)
			-- print (query:expanded())
		
			-- Query dataset storing results
			self.mds = {}

			ds:queryData({matcher=query}, function(md)
				if gq:checkandmark(md) then
					table.insert(self.mds, md:copy())
				end
				return true
			end)

			
			if #self.mds == 0 then
				if verbose then
					io.stderr:write("No results from ", dsname, "\n")
				end
			elseif gq:satisfied() then
				if verbose then io.stderr:write("Satisfied query on ", dsname, "\n") end
				return
			elseif verbose then
				io.stderr:write("Unsatisfied query on ", dsname, "\n")
				io.stderr:write("Master query: ", tostring(query), "\n")
				io.stderr:write("Result space:\n")
				io.stderr:write(gq:dump(), "\n")
			end
		end
	end

	error("Query cannot be satisfied")
end

function QueryChunk:outputData(cons)
	for idx, md in ipairs(self.mds) do
		cons(md)
	end
end

function QueryChunk:querySummary(sum)
	for idx, dsname in ipairs(self:datasets()) do
		if verbose then io.stderr:write("Getting summary from ", dsname, "\n") end

		local ds = qmacro:dataset(dsname)
		if ds == nil then error("Cannot access dataset " .. dsname) end
		local gq = arki.gridquery.new(ds)

		-- Prepare the GridQuery
		self:setupgq(gq)

		-- Build the merged query
		query = gq:mergedquery()

		-- Add the summary to sum
		ds:querySummary(query, sum)
	end
end


-- Cache of summary information
chunks = {}

-- Parse input
for line in query:gmatch("[^\r\n]+") do
	ds, d, t, s, l, v = line:match(linepat)
	if ds ~= nil then
		s = Set:parse(s)
		l = Set:parse(l)
		v = Set:parse(v)
		q = buildmatcher(s, l, v)

		dstag = ds .. "/" .. d .. "/" .. t
		info = chunks[dstag]
		if chunks[dstag] == nil then
			info = QueryChunk:new()
			info:init(ds, d, t)
			chunks[dstag] = info
		end
		-- print("ADD", name, q)
		info:addquery(q)
	else
		error("line not parsed: " .. line)
	end
end

function queryData(q, cons)
	for _, info in pairs(chunks) do
		info:checkData(cons)
	end
	for _, info in pairs(chunks) do
		info:outputData(cons)
	end
end

function querySummary(q, sum)
	for _, info in pairs(chunks) do
		info:querySummary(sum)
	end
end
