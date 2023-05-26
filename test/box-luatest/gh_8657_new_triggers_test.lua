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

g.after_each(function(cg)
    cg.server:exec(function()
        local trigger = require('trigger')
        if box.space.test then box.space.test:drop() end
        -- Delete all registered triggers
        local trigger_info = trigger.info()
        for event, trigger_list in pairs(trigger_info) do
            for trigger_name, _ in pairs(trigger_list) do
                trigger.del(event, trigger_name)
            end
        end
    end)
end)

g.test_on_replace = function(cg)
    cg.server:exec(function()
        local trigger = require('trigger')
        local state = {}
        local handlers = {}
        for i = 1, 4 do
            table.insert(handlers, function(old_tuple, new_tuple)
                table.insert(state, new_tuple[i])
            end)
        end
        local space_id = 743
        local event_by_id = string.format('box.space[%d].on_replace', space_id)
        local event_by_name = 'box.space.test.on_replace'
        trigger.set(event_by_id, tostring(2), handlers[2])
        trigger.set(event_by_name, tostring(4), handlers[4])
        local s = box.schema.create_space('test', {id = space_id})
        s:create_index('pk')
        t.assert_equals(state, {})
        s:replace{0, 5, 10, 15}
        t.assert_equals(state, {5, 15}) 
        state = {}

        trigger.set(event_by_id, tostring(1), handlers[1])
        trigger.set(event_by_name, tostring(3), handlers[3])
        s:replace{1, 2, 3, 4}
        t.assert_equals(state, {1, 2, 3, 4}) 
        state = {}
        
        local new_tuple = {11, 12, 13, 14}
        trigger.call(event_by_id, nil, new_tuple)
        trigger.call(event_by_name, nil, new_tuple)
        t.assert_equals(state, new_tuple)
        state = {}
    end)
end

g.test_before_replace = function(cg)
    cg.server:exec(function()
        local trigger = require('trigger')
        local handlers = {}
        for i = 1, 4 do
            table.insert(handlers, function(old_tuple, new_tuple)
		local prev_field = new_tuple[i]
                return new_tuple:update{{'+', i + 1, prev_field}}
            end)
        end
        local space_id = 743
        local event_by_id = string.format('box.space[%d].before_replace', space_id)
        local event_by_name = 'box.space.test.before_replace'
        trigger.set(event_by_id, tostring(2), handlers[2])
        trigger.set(event_by_name, tostring(4), handlers[4])
        local s = box.schema.create_space('test', {id = space_id})
        s:create_index('pk')

        s:replace{1, 3, 7, 19, 289}
	t.assert_equals(s:get(1), {1, 3, 3 + 7, 19, 289 + 19})

        trigger.set(event_by_id, tostring(1), handlers[1])
        trigger.set(event_by_name, tostring(3), handlers[3])

        s:replace{2, 15, 30, 70, 193}
	t.assert_equals(s:get(2), {2, 17, 47, 117, 310})

    end)
end
