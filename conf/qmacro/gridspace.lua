-- Syntax of input:
--
-- dataset: dsname
-- add: matcher
-- addtime: time
-- addtimes: from to step
-- addfilter: matcher
--
-- "dataset:" lines must appear before any other line
--
-- Each line can be provided more than once. If multiple datasets are given,
-- they are queried one after the other until one satisfies the request

dataset_aliases = {
	LMSMR = { "lmsmr4x46", "lmsmr4x45", "lmsmr4x47" }
}

datasets_tried = {}

function makedsgq()
	local name = nil
	local ds = nil
	local gq = nil

	-- These handlers will be called only if ds and gq are non-nil
	local handlers = {
		add = function(val)
			gq:add(val)
		end,
		addtime = function(val)
			gq:addtime(val)
	-- -- Parse date
	-- if d:match("^%s*[Tt]%s*$") then
	-- 	d = os.date("%Y-%m-%d")
	-- elseif d:match("^%s*@%s*$") then
	-- 	d = os.getenv("DAY")
	-- 	if d == nil then error("If using @, set $DAY to YYYY-MM-DD") end
	-- elseif d:match("^%d+-%d+-%d+$") then
	-- 	-- d is already as we want it
	-- else
	-- 	pm, val = d:match("^%s*[Tt]%s*([+-])%s*(%d+)%s*$")
	-- 	if pm == nil then error("Expected 'T+n' or 'T-n'") end
	-- 	if pm == "+" then
	-- 		ts = os.time() + 3600 * 24 * val
	-- 	else
	-- 		ts = os.time() - 3600 * 24 * val
	-- 	end
	-- 	d = os.date("%Y-%m-%d", ts)
	-- end

	-- -- Parse time
	-- h, m = t:match("(%d%d)(%d%d)")
	-- if h == nil then error("Time expected in form HHMM") end

	-- self.timestr = d .. string.format(" %02d:%02d:00", h, m)
		end,
		addtimes = function(val)
			from, to, step = val:match("(%d+-%d+-%d+ %d+:%d+:%d+)%s+(%d+-%d+-%d+ %d+:%d+:%d+)%s+(%d+)")
			if from == nil then error("Invalid addtimes value; expected 'TS_FROM TS_TO STEP_SECS'") end

			gq:addtimes(from, to, step)
		end,
		addfilter = function(val)
			gq:addfilter(val)
		end
	}

	-- Parse input
	for line in query:gmatch("[^\r\n]+") do
		if line:match("^%s+$") then 
			-- Skip empty lines
		else
			key, val = line:match("(%w+):%s*(.+)")
			if key == nil then
				error("line not parsed:", line)
			elseif key == "dataset" then
				if gq == nil and datasets_tried[val] == nil then
					-- Try this dataset
					datasets_tried[val] = true
					ds = qmacro:dataset(val)
					if ds == nil then error("Dataset " .. val .. " does not exist") end
					gq = arki.gridquery.new(ds)
					name = val
				end
			else
				if gq == nil then
					-- No more datasets left to try
					return nil, nil, nil
				end
				handler = handlers[key]
				if handler == nil then error("unknown command: " .. key) end
				handler(val)
			end
		end
	end

	if gq ~= nil then
		-- Consolidate in the end
		gq:consolidate()
	end

	if debug then
		io.stderr:write("Gridspace ready for ", name, "\n")
		io.stderr:write("Master query: ", tostring(gq:mergedquery()), "\n")
		io.stderr:write("Result space:\n")
		io.stderr:write(gq:dump(), "\n")
	end

	return name, ds, gq
end

function queryData(q, cons)
	datasets_tried = {}
	while true do
		local dsname, ds, gq = makedsgq()
		if dsname == nil then break end

		if verbose then io.stderr:write("Trying query on ", dsname, "\n") end

		if gq:expecteditems() == 0 then
			if verbose then
                                io.stderr:write("No results expected from ", dsname, "\n")
                        end
		else
			-- Build the merged query
			local query = gq:mergedquery()
			-- print (query)
			-- print (query:expanded())
		
			-- Query dataset storing results
			mds = {}
			ds:queryData({matcher=query}, function(md)
				if gq:checkandmark(md) then
					table.insert(mds, md:copy())
				end
				return true
			end)

			
			if #mds == 0 then
				if verbose then
					io.stderr:write("No results from ", dsname, "\n")
				end
			elseif gq:satisfied() then
				if verbose then io.stderr:write("Satisfied query on ", dsname, "\n") end
				for idx, md in ipairs(mds) do
					cons(md)
				end
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

function querySummary(q, sum)
	datasets_tried = {}
	local tried = 0
	while true do
		local dsname, ds, gq = makedsgq()
		if dsname == nil then break end
		if verbose then io.stderr:write("Getting summary from ", dsname, "\n") end
		-- Build the merged query
		local query = gq:mergedquery()

		-- Add the summary to sum
		ds:querySummary(query, sum)

		tried = tried + 1
	end
	if tried == 0 then error("Query cannot be satisfied") end
end
