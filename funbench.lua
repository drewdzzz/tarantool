local clock = require('clock')
if arg[1] == nil then
    error('Module name is expected')
end
local fun = require(arg[1])

local t = {}

local N = 1e6

math.randomseed(os.time())
for i = 1, N do
    table.insert(t, math.random(50))
end
table.insert(t, 100)
for i = 1, N do
    table.insert(t, math.random(100))
end

local acc = 0
local function bench()
    for _ = 1, 300 do
        local factor = math.random(10)
        for _, i in fun.iter(t):drop_while(function(x) return x <= 50 end) do
            acc = acc + i * factor
        end
    end
end

local elapsed = clock.bench(bench)[1]

print(elapsed)