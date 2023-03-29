local server = require('luatest.server')
local t = require('luatest')

local g = t.group()

g.before_all(function(cg)
    cg.server = server:new({alias = 'master'})
    cg.server:start()
end)

g.after_all(function(cg)
    cg.server:stop()
end)

-- seed 777, iter_num 30, eps 3, delta 1 - segfault
-- seed 1680250185, iter_num 100, eps 3, delta 1 - find_apporx_pos failed.
-- seed 1680252743, iter_num 100, eps 3, delta 1 - just did not find.
--
g.test_random_1e2 = function(cg)
    cg.server:exec(function()
        -- local seed = os.time()
        -- print("Seed: ", seed)
        -- math.randomseed(seed)
        math.randomseed(1680252743)
        local iter_num = 100
        local max_num = 10 * iter_num

        local data = {}
        local used = {}
        used[0] = true

        for i = 1, iter_num do
        	local num = 0
        	while used[num] do
        		num = math.random(1, max_num)
	        end
	        used[num] = true
	        table.insert(data, num)
        end
        local s = box.schema.space.create('s')
        s:create_index('pk')
        local sk = s:create_index('sk', {type = 'pgdm'})

        for i = 1, iter_num do
	        s:replace{data[i]}
        end
        for i = 1, max_num do
            local found = sk:get{i} and true
            if found ~= used[i] then   
                box.space.s.index.sk:count()
            end
            t.assert_equals(found, used[i], "Failed on " .. i .. " iter")
        end
    end)
end
