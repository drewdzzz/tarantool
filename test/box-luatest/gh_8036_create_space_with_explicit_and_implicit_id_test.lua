local server = require('luatest.server')
local t = require('luatest')
local g = t.group()

g.before_all(function()
    g.server = server:new{
        alias   = 'default',
        box_cfg = {}
    }
    g.server:start()
end)

g.after_all(function()
    g.server:drop()
end)

g.before_each(function()
    g.server:exec(function()
        -- Manually reset max_id instead of restarting the server.
        box.space._schema:replace{'max_id', 511}
    end)
end)

g.after_each(function()
    g.server:exec(function()
        box.space.SPACE:drop()
        box.space.SPACE2:drop()
    end)
end)

g.test_create_space_with_explicit_and_implicit_id = function()
    g.server:exec(function()
        local t = require('luatest')
        box.schema.space.create('SPACE', {
            format = {{name = 'id', type = 'unsigned'}},
            id = 512
        })
        box.schema.space.create('SPACE2', {
            format = {{name = 'id', type = 'unsigned'}}
        })
        t.assert_not_equals(box.space.SPACE, nil)
        t.assert_equals(box.space.SPACE.id, 512)
        t.assert_not_equals(box.space.SPACE2, nil)
        t.assert_equals(box.space.SPACE2.id, 513)
    end)
end

g.test_create_space_with_explicit_and_implicit_id_sql = function()
    g.server:exec(function()
        local t = require('luatest')
        box.schema.space.create('SPACE', {
            format = {{name = 'id', type = 'unsigned'}},
            id = 512
        })
        box.execute("CREATE TABLE SPACE2(a INT PRIMARY KEY);")
        t.assert_not_equals(box.space.SPACE, nil)
        t.assert_equals(box.space.SPACE.id, 512)
        t.assert_not_equals(box.space.SPACE2, nil)
        t.assert_equals(box.space.SPACE2.id, 513)
    end)
end

g.test_create_space_with_explicit_and_implicit_id_after_update = function()
    g.server:exec(function()
        local t = require('luatest')
        box.schema.space.create('SPACE', {
        format = {{name = 'id', type = 'unsigned'}},
            id = 512ULL
        })
        -- Simulate that SPACE was created before this patch.
        box.space._schema:replace{'max_id', 511}
        box.schema.space.create('SPACE2', {
            format = {{name = 'id', type = 'unsigned'}}
        })
        t.assert_not_equals(box.space.SPACE, nil)
        t.assert_equals(box.space.SPACE.id, 512)
        t.assert_not_equals(box.space.SPACE2, nil)
        t.assert_equals(box.space.SPACE2.id, 513)
    end)
end

g.test_create_space_with_explicit_and_implicit_id_no_max_id = function()
    g.server:exec(function()
        local t = require('luatest')
        box.space._schema:delete('max_id')
        box.schema.space.create('SPACE', {
            format = {{name = 'id', type = 'unsigned'}},
            id = 512
        })
        box.schema.space.create('SPACE2', {
            format = {{name = 'id', type = 'unsigned'}}
        })
        t.assert_not_equals(box.space.SPACE, nil)
        t.assert_equals(box.space.SPACE.id, 512)
        t.assert_not_equals(box.space.SPACE2, nil)
        t.assert_equals(box.space.SPACE2.id, 513)
    end)
end

g.test_create_space_with_explicit_and_implicit_id_space_id_overflow = function()
    g.server:exec(function()
        local t = require('luatest')
        box.schema.space.create('SPACE', {
        format = {{name = 'id', type = 'unsigned'}},
            id = 0x7fffffff
        })
        -- Simulate that SPACE was created before this patch.
        box.space._schema:replace{'max_id', 0x7fffffff}
        box.schema.space.create('SPACE2', {
            format = {{name = 'id', type = 'unsigned'}}
        })
        t.assert_not_equals(box.space.SPACE, nil)
        t.assert_equals(box.space.SPACE.id, 0x7fffffff)
        t.assert_not_equals(box.space.SPACE2, nil)
        t.assert_equals(box.space.SPACE2.id, 512)
        box.space.SPACE2:drop()
        box.space._schema:replace{'max_id', 0x7fffffff - 1}
        box.schema.space.create('SPACE2', {
            format = {{name = 'id', type = 'unsigned'}}
        })
        t.assert_not_equals(box.space.SPACE2, nil)
        t.assert_equals(box.space.SPACE2.id, 512)
    end)
end

g.test_create_space_no_maxid = function()
    g.server:exec(function()
        local t = require('luatest')
        box.space._schema:delete('max_id')
        box.schema.space.create('SPACE', {
            format = {{name = 'id', type = 'unsigned'}}
        })
        t.assert_not_equals(box.space.SPACE, nil)
        t.assert_equals(box.space.SPACE.id, 512)
        box.space._schema:delete('max_id')
        box.schema.space.create('SPACE2', {
            format = {{name = 'id', type = 'unsigned'}}
        })
        t.assert_not_equals(box.space.SPACE2, nil)
        t.assert_equals(box.space.SPACE2.id, 513)
    end)
end
