--[[
Read a grib table.

edition is the GRIB edition: 1 or 2

table is the table name, for example "0.0"

Returns a table where the index maps to a couple { abbreviation, description },
or nil if the file had no such entry.

For convenience, the table has also two functions, 'abbr' and 'desc', that
return the abbreviation or the description, falling back on returning the table
index if they are not available.

For example:

origins = read_grib_table(1, "0")
print(origins:abbr(98))  -- Prints 'ecmf'
print(origins:desc(98))  -- Prints 'European Center for Medium-Range Weather Forecasts'
print(origins:abbr(999)) -- Prints '999'
print(origins:desc(999)) -- Prints '999'
--]]

function read_grib_table(edition, table)
  -- Initial table, with extra accessor methods
  res = {
    abbr = function(self, val)
             if self[val] == nil then
               return val
             else
               return self[val][1]
             end
           end,
    desc = function(self, val)
             if self[val] == nil then
               return val
             else
               return self[val][2]
             end
           end,
  }
  for dir in string.gmatch(GRIBAPI_DEF_DIR, "[^:]+")
  do
    valid = pcall(function()
    -- Build the file name
    file = dir .. "/grib" .. edition .. "/" .. table .. ".table"
    for line in io.lines(file) do
        idx, abbr, desc = string.match(line, "^%s*(%d+)%s+([^%s]+)%s+(.-)%s*$")
        if idx ~= nil and res[tonumber(idx)] == nil then
            res[tonumber(idx)] = { abbr, desc }
        end
    end
    end)
  end

  return res
end


function get_grib2_table_prefix(centre, table_version, local_table_version)
    local default_table_version = 4

    if table_version == nil or table_version == 255 then
        table_version = default_table_version
    end

    if local_table_version ~= nil and local_table_version ~= 255 and local_table_version ~= 0 then
        centres = read_grib_table(1, "0")
        if centres[centre] ~= nil then
            return string.format('tables/local/%s/%d/',
                centres:abbr(centre), local_table_version)
        end
    end

    return string.format('tables/%d/', table_version)
end