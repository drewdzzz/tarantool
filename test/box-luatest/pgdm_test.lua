local server = require('luatest.server')
local t = require('luatest')

local g = t.group()

g.before_all(function(cg)
    cg.server = server:new({alias = 'master', box_cfg = {memtx_memory = 1024 * 1024 * 1024}})
    cg.server:start()
end)

g.after_all(function(cg)
    cg.server:stop()
end)

g.test_random_1e5 = function(cg)
    cg.server:exec(function()
        local seed = os.time()
        math.randomseed(seed)
        local iter_num = 5 * 1e5
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
	local found_num = 0
	for i = 1, max_num do
		local found = sk:get{i} and true
		if found then found_num = found_num + 1 end
	end
	print(found_num)
        for i = 1, max_num do
            local found = sk:get{i} and true
            if found ~= used[i] then   
                box.space.s.index.sk:count()
            end
            t.assert_equals(found, used[i], "Failed on " .. i .. " iter")
        end
    end)
end
