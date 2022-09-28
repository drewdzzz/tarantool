local server = require('test.luatest_helpers.server')
local t = require('luatest')
local tree_g = t.group('Tree index tests',
    t.helpers.matrix{engine={'memtx'}, disable_ffi={true, false}})

tree_g.before_all(function(cg)
    cg.server = server:new{
        alias   = 'default',
    }
    cg.server:start()
    cg.server:exec(function()
        require('jit').off()
    end)
    cg.server:exec(function(disable_ffi)
        local ffi = require('ffi')
        ffi.cdef("extern void box_read_ffi_disable(void);")
        if disable_ffi then
            ffi.C.box_read_ffi_disable()
        end
    end, {cg.params.disable_ffi})

end)

tree_g.after_all(function(cg)
    cg.server:drop()
end)

tree_g.before_each(function(cg)
    cg.server:exec(function(engine)
        box.schema.space.create('s', {engine = engine})
    end, {cg.params.engine})
end)

tree_g.after_each(function(cg)
    cg.server:exec(function()
        box.space.s:drop()
    end)
end)

tree_g.test_tree_pagination = function(cg)
    cg.server:exec(function()
        local t = require('luatest')
        local s = box.space.s
        s:create_index("pk", {type = "tree"})
        s:create_index("sk", {parts = {2}, type = "tree", unique=false})
        s:create_index("sk2", {parts = {3}, type = "tree", unique=false})
        s:create_index("sk_unique", {parts = {4}, type = "tree", unique=true})

        -- Fetch position in empty space
        local indexes = {s.index.pk, s.index.sk, s.index.sk2, s.index.sk_unique}
        for _, index in pairs(indexes) do
            local tuples, pos = index:select(nil,
                    {limit=2, fullscan=true, fetch_pos=true})
            t.assert_equals(tuples, {})
            t.assert(pos == nil)
        end

        for i = 1, 11 do
            s:replace{i, 1, i, i}
        end

        local tuples1
        local tuples2
        local tuples_offset
        local pos
        local last_tuple
        local test_indexes = {s.index.pk, s.index.sk2, s.index.sk_unique}

        -- Test fullscan pagination
        for _, index in pairs(test_indexes) do
            pos = ""
            last_tuple = box.NULL
            for i = 0, 5 do
                tuples1, pos = index:select(nil,
                        {limit=2, fullscan=true, fetch_pos=true, after=pos})
                tuples2 = index:select(nil,
                        {limit=2, fullscan=true, after=last_tuple})
                last_tuple = tuples2[#tuples2]
                tuples_offset = index:select(nil,
                        {limit=2, fullscan=true, offset=i*2})
                t.assert_equals(tuples1, tuples_offset)
                t.assert_equals(tuples2, tuples_offset)
            end
            tuples1, pos = index:select(nil,
                    {limit=2, fullscan=true, fetch_pos=true, after=pos})
            t.assert_equals(tuples1, {})
            t.assert(pos == nil)
        end

        -- Test pagination on range iterators
        local key_iter = {
            ['GE'] = 3,
            ['GT'] = 2,
            ['LE'] = 9,
            ['LT'] = 10,
        }
        for _, index in pairs(test_indexes) do
            for iter, key in pairs(key_iter) do
                pos = ""
                last_tuple = box.NULL
                for i = 0, 4 do
                    tuples1, pos = index:select(key,
                            {limit=2, iterator=iter, fetch_pos=true, after=pos})
                    tuples2 = index:select(key,
                            {limit=2, iterator=iter, after=last_tuple})
                    last_tuple = tuples2[#tuples2]
                    tuples_offset = index:select(key,
                            {limit=2, iterator=iter, offset=i*2})
                    t.assert_equals(tuples1, tuples_offset)
                    t.assert_equals(tuples2, tuples_offset)
                end
                tuples1, pos = index:select(key,
                        {limit=2, iterator=iter, fetch_pos=true, after=pos})
                t.assert_equals(tuples1, {})
                t.assert(pos == nil)
            end
        end

        -- Test pagination on equality iterators
        s:replace{0, 0, 0, 0}
        s:replace{12, 2, 12, 12}
        for _, iter in pairs({'EQ', 'REQ'}) do
            pos = ""
            last_tuple = box.NULL
            for i = 0, 5 do
                tuples1, pos = s.index.sk:select(1,
                        {limit=2, iterator=iter, fetch_pos=true, after=pos})
                tuples2 = s.index.sk:select(1,
                        {limit=2, iterator=iter, after=last_tuple})
                last_tuple = tuples2[#tuples2]
                tuples_offset = s.index.sk:select(1,
                        {limit=2, iterator=iter, offset=i*2})
                t.assert_equals(tuples1, tuples_offset)
                t.assert_equals(tuples2, tuples_offset)
            end
            tuples1, pos = s.index.sk:select(1,
                    {iterator=iter, limit=2, fetch_pos=true, after=pos})
            t.assert_equals(tuples1, {})
            t.assert(pos == nil)
        end
    end)
end

tree_g.test_tree_multikey_pagination = function(cg)
    cg.server:exec(function()
        local t = require('luatest')
        local s = box.space.s
        s:create_index("pk", {type = "tree"})
        local sk = s:create_index("sk",
                {parts = {{field = 2, type = 'uint', path = '[*]'}},
                 type = "tree", unique=false})

        -- Fetch position in empty space
        local tuples, pos = sk:select(nil,
                {limit=2, fullscan=true, fetch_pos=true})
        t.assert_equals(tuples, {})
        t.assert(pos == nil)

        for i = 1, 3 do
            s:replace{i, {1, 2, 3}}
        end

        local tuples
        local tuples_offset
        local pos = ""

        -- Test fullscan pagination
        for i = 0, 4 do
            tuples, pos = sk:select(nil,
                    {limit=2, fullscan=true, fetch_pos=true, after=pos})
            tuples_offset = sk:select(nil,
                    {limit=2, fullscan=true, offset=i*2})
            t.assert_equals(tuples, tuples_offset)
        end
        tuples, pos = sk:select(nil,
                {limit=2, fullscan=true, fetch_pos=true, after=pos})
        t.assert_equals(tuples, {})
        t.assert(pos == nil)

        -- Test pagination on range iterators
        local key_iter = {
            ['GE'] = 2,
            ['GT'] = 1,
            ['LE'] = 2,
            ['LT'] = 3,
        }
        for iter, key in pairs(key_iter) do
            pos = ""
            for i = 0, 2 do
                tuples, pos = sk:select(key,
                        {limit=2, iterator=iter, fetch_pos=true, after=pos})
                tuples_offset = sk:select(key,
                        {limit=2, iterator=iter, offset=i*2})
                t.assert_equals(tuples, tuples_offset)
            end
            tuples, pos = sk:select(key,
                    {limit=2, iterator=iter, fetch_pos=true, after=pos})
            t.assert_equals(tuples, {})
            t.assert(pos == nil)
        end

        -- Test pagination on equality iterators
        for _, iter in pairs({'EQ', 'REQ'}) do
            pos = ""
            for i = 0, 1 do
                tuples, pos = sk:select(2,
                        {limit=2, iterator=iter, fetch_pos=true, after=pos})
                tuples_offset = sk:select(2,
                        {limit=2, iterator=iter, offset=i*2})
                t.assert_equals(tuples, tuples_offset)
            end
            tuples, pos = sk:select(2,
                    {iterator=iter, limit=2, fetch_pos=true, after=pos})
            t.assert_equals(tuples, {})
            t.assert(pos == nil)
        end

        -- Test that after with tuple in multikey index returns an error
        local tuple = s.index.pk:get(1)
        t.assert_error_msg_contains(
                "Multikey index does not support position by tuple",
                sk.select, sk, nil, {fullscan=true, after=tuple})
    end)
end

tree_g.test_nullable_indexes = function(cg)
    cg.server:exec(function()
        local t = require('luatest')
        local s = box.space.s
        s:create_index("pk", {type = "tree"})
        s:create_index("sk1",
           {parts = {{field = 2, type = "uint", is_nullable=true}},
            type = "tree", unique=false})
        s:create_index("sk2",
            {parts = {{field = 3, type = "uint", is_nullable=true}},
            type = "tree", unique=true})
        s:create_index("sk3",
            {parts = {{field = 4, type = "uint", exclude_null=true}},
            type = "tree", unique=false})
        s:create_index("sk4",
            {parts = {{field = 5, type = "uint", exclude_null=true}},
            type = "tree", unique=true})

        -- Fetch position in empty space
        local indexes = {s.index.sk1, s.index.sk2, s.index.sk3, s.index.sk4}
        for _, index in pairs(indexes) do
            local tuples, pos = index:select(nil,
                    {limit=2, fullscan=true, fetch_pos=true})
            t.assert_equals(tuples, {})
            t.assert(pos == nil)
        end

        for i = 1, 13 do
            if i % 3 == 0 then
                s:replace{i}
            else
                s:replace{i, i, i, i, i}
            end
        end

        local tuples1
        local tuples2
        local tuples_offset
        local pos
        local last_tuple

        local test_indexes = {s.index.sk1, s.index.sk2}

        for _, index in pairs(test_indexes) do
            pos = ""
            last_tuple = box.NULL
            for i = 0, 6 do
                tuples1, pos = index:select(nil,
                        {limit=2, fullscan=true, fetch_pos=true, after=pos})
                tuples2 = index:select(nil,
                        {limit=2, fullscan=true, after=last_tuple})
                last_tuple = tuples2[#tuples2]
                tuples_offset = index:select(nil,
                        {limit=2, fullscan=true, offset=i*2})
                t.assert_equals(tuples1, tuples_offset)
                t.assert_equals(tuples2, tuples_offset)
            end
            tuples1, pos = index:select(nil,
                    {limit=2, fullscan=true, fetch_pos=true, after=pos})
            t.assert_equals(tuples1, {})
            t.assert(pos == nil)
        end

        test_indexes = {s.index.sk3, s.index.sk4}
        for _, index in pairs(test_indexes) do
            pos = ""
            last_tuple = box.NULL
            for i = 0, 4 do
                tuples1, pos = index:select(nil,
                        {limit=2, fullscan=true, fetch_pos=true, after=pos})
                tuples2 = index:select(nil,
                        {limit=2, fullscan=true, after=last_tuple})
                last_tuple = tuples2[#tuples2]
                tuples_offset = index:select(nil,
                        {limit=2, fullscan=true, offset=i*2})
                t.assert_equals(tuples1, tuples_offset)
                t.assert_equals(tuples2, tuples_offset)
            end
            tuples1, pos = index:select(nil,
                    {limit=2, fullscan=true, fetch_pos=true, after=pos})
            t.assert_equals(tuples1, {})
            t.assert(pos == nil)
        end
    end)
end

-- Check if nil is returned if no tuples were selected.
tree_g.test_no_tuples_satisfying_filters = function(cg)
    cg.server:exec(function()
        local t = require('luatest')
        local s = box.space.s
        s:create_index("pk", {type = "tree"})
        s:create_index("sk",
                {parts =
                 {{field = 2, type = 'uint', path = '[*]'}},
                 type = "tree",
                 unique=false})

        local tuples
        local pos

        s:replace{1, {1, 2}}

        tuples, pos = s:select(3, {limit=1, iterator='GE', fetch_pos=true})
        t.assert_equals(tuples, {})
        t.assert(pos == nil)
        s:replace{2, {1, 2}}
        s:replace{3, {1, 2}}
        s:replace{4, {1, 2}}
        tuples = s:select(3, {limit=1, iterator='GE', after=pos})
        t.assert_equals(tuples[1], {3, {1, 2}})

        tuples, pos = s.index.sk:select(4,
                {limit=1, iterator='GE', fetch_pos=true})
        t.assert_equals(tuples, {})
        t.assert(pos == nil)
    end)
end

tree_g.test_invalid_positions = function(cg)
    cg.server:exec(function()
        local t = require('luatest')
        local s = box.space.s
        s:create_index('pk', {type = 'tree'})
        s:create_index('sk', {
                parts = {{field = 2, type = 'string'}},
                type = 'tree', unique=true})
        s:replace{1, 'Zero'}

        local tuples
        local pos = {1, 2}
        local flag, err = pcall(function()
            s:select(1, {limit=1, iterator='GE', after=pos})
        end)
        t.assert_equals(flag, false)
        t.assert_equals(err.code, box.error.FIELD_TYPE)

        pos = "abcd"
        flag, err = pcall(function()
            s:select(1, {limit=1, iterator='GE', after=pos})
        end)
        t.assert_equals(flag, false)
        t.assert_equals(err.code, box.error.ITERATOR_POSITION)

        tuples, pos = s:select(nil, {fullscan=true, limit=1, fetch_pos=true})
        t.assert_equals(#tuples, 1)
        t.assert(#pos > 0)
        flag, err = pcall(function()
            s.index.sk:select(nil, {fullscan=true, limit=1, after=pos})
        end)
        t.assert_equals(flag, false)
        t.assert_equals(err.code, box.error.EXACT_MATCH)
    end)
end

tree_g.test_tree_pagination_no_duplicates = function(cg)
    cg.server:exec(function()
        local t = require('luatest')
        local s = box.space.s
        s:create_index("pk", {type = "tree"})

        for i = 1, 4 do
            s:replace{i}
        end
        for i = 6, 10 do
            s:replace{i}
        end

        local tuples, pos = s:select(1,
                {iterator='GE', fetch_pos=true, limit=5})
        t.assert_equals(#tuples, 5)
        s:replace{5}
        tuples = s:select(1, {iterator='GE', after=pos})
        t.assert_equals(#tuples, 4)
        t.assert_equals(tuples, s:select(7, {iterator='GE'}))

        tuples, pos = s:select(1, {iterator='GE', fetch_pos=true})
        t.assert_equals(#tuples, 10)
        s:delete(10)
        local last_pos = pos
        tuples, pos = s:select(1, {iterator='GE', fetch_pos=true, after=pos})
        t.assert_equals(tuples, {})
        t.assert(pos == nil)
        s:replace{10}
        s:replace{11}
        pos = last_pos
        tuples = s:select(1, {iterator='GE', after=pos})
        t.assert_equals(#tuples, 1)
        t.assert_equals(tuples[1], {11})
    end)
end

tree_g.test_tuple_pos_simple = function(cg)
    cg.server:exec(function()
        local t = require('luatest')
        local s = box.space.s
        s:create_index("pk", {type = "tree"})
        s:create_index("sk", {
                parts = {{field = 2, type = 'uint', path = '[*]'}},
                type = "tree", unique=false})

        local tuples
        local pos = ""
        local last_pos

        for i = 1, 10 do
            s:replace{i, {1, 2}}
        end

        for i = 0, 4 do
            tuples, last_pos = s:select(nil,
                    {limit=2, fullscan=true, fetch_pos=true, after=pos})
            t.assert_equals(tuples[1][1], i * 2 + 1)
            t.assert_equals(tuples[2][1], i * 2 + 2)
            t.assert_equals(tuples[3], nil)
            pos = s.index.pk:tuple_pos(tuples[2])
            t.assert(pos == last_pos)
        end
        tuples, pos = s:select(nil,
                {limit=2, fullscan=true, fetch_pos=true, after=pos})
        t.assert_equals(tuples, {})
        t.assert(pos == nil)

        tuples = s:select(1)
        t.assert_error_msg_contains(
                "Multikey index does not support position by tuple",
                s.index.sk.tuple_pos, s.index.sk, tuples[1])
    end)
end

tree_g.test_tuple_pos_corner_cases = function(cg)
    cg.server:exec(function()
        local t = require('luatest')
        local s = box.space.s
        s:create_index("pk", {type = "tree"})
        local sk1 = s:create_index("sk1", {parts = {{field = 2, type = 'uint'}},
                                           type = "tree", unique=true})
        local sk2 = s:create_index("sk2", {parts = {{field = 3, type = 'uint'}},
                                           type = "tree", unique=false})
        for i = 0, 9 do
            s:replace{i, i, math.floor(i / 2)}
        end

        local pos
        local tuple
        local tuples1
        local tuples2
        for i = 0, 4 do
            tuple = {i * 2, i * 2, i}
            pos = sk1:tuple_pos(tuple)
            tuples1 = sk1:select(nil, {after=pos})
            pos = sk2:tuple_pos(tuple)
            tuples2 = sk2:select(nil, {after=pos})
            t.assert_equals(tuples1, tuples2)
        end
    end)
end

tree_g.test_tuple_pos_invalid_tuple = function(cg)
    cg.server:exec(function()
        local t = require('luatest')
        local s = box.space.s
        s:create_index("pk", {type = "tree", parts={{field=1, type='uint'}}})

        local tuples
        local pos

        for i = 1, 10 do
            s:replace{i, 0}
        end

        t.assert_error_msg_contains("Usage index:tuple_pos(tuple)",
                s.index.pk.tuple_pos, s.index.pk)
        pos = s.index.pk:tuple_pos({1, 0})
        tuples = s:select(nil, {fullscan=true, after=pos, limit=1})
        t.assert_equals(tuples[1], {2, 0})
        -- test with invalid tuple
        t.assert_error_msg_contains(
                "Tuple field 1 type does not match one required by operation",
                s.index.pk.tuple_pos, s.index.pk, {'a'})
    end)
end

tree_g.test_runtime_memory_leak = function(cg)
    cg.server:exec(function()
        local fiber = require('fiber')
        fiber.set_max_slice(fiber.self(), 8)
        local t = require('luatest')
        local s = box.space.s
        local pk = s:create_index("pk")
        local tuple = s:replace{0, 0}
        local runtime_used = box.runtime.info().used
        local delta = 100

        for _ = 1, 1e4 do
            pk:tuple_pos({0, 0})
            pk:tuple_pos(tuple)
        end
        t.assert_le(box.runtime.info().used, runtime_used + delta)

        local pos = pk:tuple_pos({0, 0})
        for _ = 1, 1e3 do
            s:select(0, {after={0, 0}})
            s:select(0, {after=tuple})
            s:select(0, {after=pos})
        end
        t.assert_le(box.runtime.info().used, runtime_used + delta)
    end)
end

-- Tests for memtx tree features, such as functional index
local func_g = t.group('Memtx tree func index tests')

func_g.before_all(function()
    func_g.server = server:new{
        alias   = 'default',
    }
    func_g.server:start()
    func_g.server:exec(function()
        require('jit').off()
    end)
end)

func_g.after_all(function()
    func_g.server:drop()
end)

func_g.before_each(function()
    func_g.server:exec(function()
        box.schema.space.create('s', {engine = 'memtx'})
    end)
end)

func_g.after_each(function()
    func_g.server:exec(function()
        box.space.s:drop()
        box.schema.func.drop('func')
    end)
end)

func_g.test_func_index_pagination = function()
    func_g.server:exec(function()
        local t = require('luatest')
        local s = box.space.s
        s:create_index('pk',{parts={{field = 1, type = 'uint'}}})
        local lua_code = [[function(tuple) return {tuple[2]} end]]
        box.schema.func.create('func',
                {body = lua_code,
                 is_deterministic = true,
                 is_sandboxed = true})
        local sk = s:create_index('func', {
                unique = false, func = 'func',
                parts = {{field = 1, type = 'uint'}}})

        -- Fetch position in empty space
        local tuples, pos = sk:select(nil,
                {limit=2, fullscan=true, fetch_pos=true})
        t.assert_equals(tuples, {})
        t.assert(pos == nil)

        for i = 1, 6 do
            s:replace{i, i}
        end
        for i = 7, 10 do
            s:replace{i, 6}
        end
        s:replace{11, 7}

        local tuples_offset

        -- Test fullscan pagination
        for i = 0, 5 do
            tuples, pos = sk:select(nil,
                    {limit=2, fullscan=true, fetch_pos=true, after=pos})
            tuples_offset = sk:select(nil,
                    {limit=2, fullscan=true, offset=i*2})
            t.assert_equals(tuples, tuples_offset)
        end
        tuples, pos = sk:select(nil,
                {limit=2, fullscan=true, fetch_pos=true, after=pos})
        t.assert_equals(tuples, {})
        t.assert(pos == nil)

        -- Test pagination on range iterators
        local key_iter = {
            ['GE'] = 6,
            ['GT'] = 5,
            ['LE'] = 5,
            ['LT'] = 6,
        }
        for iter, key in pairs(key_iter) do
            pos = ""
            for i = 0, 2 do
                tuples, pos = sk:select(key,
                        {limit=2, iterator=iter, fetch_pos=true, after=pos})
                tuples_offset = sk:select(key,
                        {limit=2, iterator=iter, offset=i*2})
                t.assert_equals(tuples, tuples_offset)
            end
            tuples, pos = sk:select(key,
                    {limit=2, iterator=iter, fetch_pos=true, after=pos})
            t.assert_equals(tuples, {})
            t.assert(pos == nil)
        end

        -- Test pagination on equality iterators
        for _, iter in pairs({'EQ', 'REQ'}) do
            pos = ""
            for i = 0, 2 do
                tuples, pos = sk:select(6,
                        {limit=2, iterator=iter, fetch_pos=true, after=pos})
                tuples_offset = sk:select(6,
                        {limit=2, iterator=iter, offset=i*2})
                t.assert_equals(tuples, tuples_offset)
            end
            tuples, pos = sk:select(6,
                    {iterator=iter, limit=2, fetch_pos=true, after=pos})
            t.assert_equals(tuples, {})
            t.assert(pos == nil)
        end
        --[[ Test that after with tuple and tuple_pos in
             functional index returns an error ]]--
        local tuple = s.index.pk:get(1)
        t.assert_error_msg_contains(
                "Functional index does not support position by tuple",
                sk.select, sk, nil, {fullscan=true, after=tuple})
        t.assert_error_msg_contains(
                "Functional index does not support position by tuple",
                sk.tuple_pos, sk, tuple)
    end)
end

func_g.test_func_multikey_index_pagination = function()
    func_g.server:exec(function()
        local t = require('luatest')
        local s = box.space.s
        s:create_index('pk', {parts = {{field = 1, type = 'uint'}}})
        local lua_code = [[function(tuple)
               local ret = {}
               for i = tuple[2], tuple[2] + 2 do
                 table.insert(ret, {i})
               end
               return ret
             end]]
        box.schema.func.create('func',
                {body = lua_code,
                 is_deterministic = true,
                 is_sandboxed = true,
                 is_multikey = true})
        local sk = s:create_index('sk', {
                unique = false, func = 'func',
                parts = {{field = 1, type = 'uint'}}})

        -- Fetch position in empty space
        local tuples, pos = sk:select(nil,
                {limit=2, fullscan=true, fetch_pos=true})
        t.assert_equals(tuples, {})
        t.assert(pos == nil)

        for i = 1, 3 do
            s:replace{i, i}
        end

        local tuples
        local tuples_offset
        local pos = ""

        -- Test fullscan pagination
        for i = 0, 4 do
            tuples, pos = sk:select(nil,
                    {limit=2, fullscan=true, fetch_pos=true, after=pos})
            tuples_offset = sk:select(nil,
                    {limit=2, fullscan=true, offset=i*2})
            t.assert_equals(tuples, tuples_offset)
        end
        tuples, pos = sk:select(nil,
                {limit=2, fullscan=true, fetch_pos=true, after=pos})
        t.assert_equals(tuples, {})
        t.assert(pos == nil)

        -- Test pagination on range iterators
        local key_iter = {
            ['GE'] = 2,
            ['GT'] = 1,
            ['LE'] = 4,
            ['LT'] = 5,
        }
        for iter, key in pairs(key_iter) do
            pos = ""
            for i = 0, 3 do
                tuples, pos = sk:select(key,
                        {limit=2, iterator=iter, fetch_pos=true, after=pos})
                tuples_offset = sk:select(key,
                        {limit=2, iterator=iter, offset=i*2})
                t.assert_equals(tuples, tuples_offset)
            end
            tuples, pos = sk:select(key,
                    {limit=2, iterator=iter, fetch_pos=true, after=pos})
            t.assert_equals(tuples, {})
            t.assert(pos == nil)
        end

        -- Test pagination on equality iterators
        for _, iter in pairs({'EQ', 'REQ'}) do
            pos = ""
            for i = 0, 1 do
                tuples, pos = sk:select(3,
                        {limit=2, iterator=iter, fetch_pos=true, after=pos})
                tuples_offset = sk:select(3,
                        {limit=2, iterator=iter, offset=i*2})
                t.assert_equals(tuples, tuples_offset)
            end
            tuples, pos = sk:select(3,
                    {iterator=iter, limit=2, fetch_pos=true, after=pos})
            t.assert_equals(tuples, {})
            t.assert(pos == nil)
        end

        --[[ Test that after with tuple and tuple_pos in
             functional multikey index returns an error ]]--
        local tuple = s.index.pk:get(1)
        t.assert_error_msg_contains(
                "Functional index does not support position by tuple",
                sk.select, sk, nil, {fullscan=true, after=tuple})
        t.assert_error_msg_contains(
                "Functional index does not support position by tuple",
                sk.tuple_pos, sk, tuple)
    end)
end

local no_sup = t.group('Unsupported pagination', {
            {engine = 'memtx', type = 'hash'},
            {engine = 'memtx', type = 'bitset'},
            {engine = 'memtx', type = 'rtree'},
})

no_sup.before_all(function(cg)
    cg.server = server:new{
        alias   = 'default',
    }
    cg.server:start()
    cg.server:exec(function()
        require('jit').off()
    end)
end)

no_sup.after_all(function(cg)
    cg.server:drop()
end)

no_sup.before_each(function(cg)
    cg.server:exec(function(engine, type)
        local s = box.schema.space.create('s', {engine=engine})
        s:create_index('pk')
        s:create_index('sk', {type=type})

    end, {cg.params.engine, cg.params.type})
end)

no_sup.after_each(function(cg)
    cg.server:exec(function()
        box.space.s:drop()
    end)
end)

no_sup.test_unsupported_pagination = function(cg)
    cg.server:exec(function(index_type)
        local t = require('luatest')
        t.assert_error_msg_contains('does not support pagination',
                box.space.s.index.sk.select, box.space.s.index.sk,
                nil, {fullscan=true, fetch_pos=true})
        local tuple = {0, 0}
        if index_type == 'rtree' then
            tuple = {0, {0, 0}}
        end
        t.assert_error_msg_contains('does not support pagination',
                box.space.s.index.sk.select, box.space.s.index.sk,
                nil, {fullscan=true, after=tuple})
        -- tuple_pos works everywhere instead of func and multikey indexes
        local pos = box.space.s.index.sk:tuple_pos(tuple)
        t.assert_error_msg_contains('does not support pagination',
                box.space.s.index.sk.select, box.space.s.index.sk,
                nil, {fullscan=true, after=pos})
    end, {cg.params.type})
end
