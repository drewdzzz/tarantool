local clock = require('clock')
local fun = require('fun')

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
	-- Methods:
	-- drop_while - original version
	-- drop_while_stateful - my version for stateful iterators
	-- drop_while_stateful2 - version for stateful iterators developed by Alexander Turenko
        for _, i in fun.iter(t):drop_while(function(x) return x <= 50 end) do
            acc = acc + i * factor
        end
    end
end

local elapsed = clock.bench(bench)[1]

print(elapsed)