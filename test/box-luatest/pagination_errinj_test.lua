local server = require('test.luatest_helpers.server')
local t = require('luatest')
local tree_g = t.group('Tree index tests')

tree_g.before_all(function()
    tree_g.server = server:new{
        alias   = 'default',
    }
    tree_g.server:start()
end)

tree_g.after_all(function()
    tree_g.server:drop()
end)

tree_g.before_each(function()
    tree_g.server:exec(function()
        box.schema.space.create('s', {engine = 'memtx'})
    end)
end)

tree_g.after_each(function()
    tree_g.server:exec(function()
        box.space.s:drop()
    end)
end)


tree_g.test_tree_errinj = function()
    tree_g.server:exec(function()
        local t = require('luatest')
        local s = box.space.s
        s:create_index('pk')
        for i = 1, 10 do
            s:replace{i}
        end
        box.error.injection.set('ERRINJ_POSITION_PACK_ON_GC_ALLOC', true)
        t.assert_error_msg_contains("Failed to allocate",
                s.select, s, nil,
                {fullscan=true, after={1}, limit=2, fetch_pos=true})
        box.error.injection.set('ERRINJ_POSITION_PACK_ON_GC_ALLOC', false)

        box.error.injection.set('ERRINJ_ITERATOR_POSITION_FAIL', true)
        t.assert_error_msg_contains("Error injection 'iterator_position fail'",
                s.select, s, nil,
                {fullscan=true, after={1}, limit=2, fetch_pos=true})
        box.error.injection.set('ERRINJ_ITERATOR_POSITION_FAIL', false)
    end)
end
