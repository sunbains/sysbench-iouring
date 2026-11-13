sysbench.histogram = {}

local histogram = {}

function histogram:update(value)
   sb_histogram_update(self.__ud, value)
end

function histogram:print()
   sb_histogram_print(self.__ud)
end

histogram.__index = histogram

function sysbench.histogram.new(size, range_min, range_max)
   local ud = sb_histogram_new(size, range_min, range_max)
   return setmetatable({__ud = ud}, histogram)
end
