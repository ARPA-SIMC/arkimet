-- Build the merged query
-- linepat = "^ds:(.-)%.%s+d:(.-)%.%s+t:(.-)%.%s+s:(.-)%.%s+l:(.-)%.%s+v:(.-)%.$"
linepat =    "^ds:(.-)%.%s+d:(.-)%.%s+t:(.-)%.%s+s:(.-)%.%s+l:(.-)%.%s+v:(.-)%.%s*"

function addtoset(s, set)
	for name in s:gmatch("%s*([^,]+)%s*") do
		set[name] = 1
	end
end

function settoors(set)
	res = ""
	first = true
	for k, v in pairs(set) do
		if first then
			res = k
			first = false
		else
			res = res .. " or " .. k
		end
	end
	return res
end

-- Parse input
set_ds = {}
set_s = {}
set_l = {}
set_v = {}
for line in data:gmatch("[^\r\n]+") do
	ds, d, t, s, l, v = line:match(linepat)
	if ds ~= nil then
		addtoset(ds, set_ds)
		addtoset(s, set_s)
		addtoset(l, set_l)
		addtoset(v, set_v)
	else
		print("line not parsed:", line)
	end
end

-- Build the merged query
query = {}
table.insert(query, "timerange:" .. settoors(set_s))
table.insert(query, "level:" .. settoors(set_l))
table.insert(query, "product:" .. settoors(set_v))
query = table.concat(query, "; ")
-- print (query)

function queryData(q, cons)
	for k, v in pairs(set_ds) do
		-- Query dataset storing results
		ds = qmacro:dataset(k)
		if ds ~= nil then
			mds = {}
			ds:queryData(query, function(md)
				table.insert(mds, md:copy())
				return true
			end)

			-- If results are good, output them and stop here
			if #mds > 0 then
				for idx, m in pairs(mds) do
					if not cons(m) then break end
				end
				break
			end
		end
	end
end

function querySummary(q, sum)
	for k, v in pairs(set_ds) do
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
