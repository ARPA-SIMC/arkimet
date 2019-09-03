--- Matching run information

local sample = arki.metadata.new()

-- MINUTE

--[[

Syntax: run:MINUTE,hour:min

min can be ommitted: if omitted, 00 is assumed.

hour:min can be completely omitted: in that case any MINUTE run will match.
]]

sample:set(arki_run.minute(12))

ensure_matches(sample, "run:MINUTE")
ensure_matches(sample, "run:MINUTE,12")
ensure_matches(sample, "run:MINUTE,12:00")

ensure_not_matches(sample, "run:MINUTE,13")
ensure_not_matches(sample, "run:MINUTE,12:30")


sample:set(arki_run.minute(12, 30))

ensure_matches(sample, "run:MINUTE,12:30")
ensure_not_matches(sample, "run:MINUTE,12")
