local server = require('test.luatest_helpers.server')
local t = require('luatest')
local tree_g = t.group('Tree index tests', t.helpers.matrix{engine={'memtx', 'vinyl'}})

tree_g.before_all(function(cg)
    cg.server = server:new{
        alias   = 'default',
    }
    cg.server:start()
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

        -- Fetch position in empty space
        local tuples, pos = s:select(nil,
                {limit=2, fullscan=true, fetch_pos=true})
        t.assert_equals(tuples, {})
        t.assert_equals(pos, "")

        for i = 1, 11 do
            s:replace{i, 1}
        end

        local tuples1
        local tuples2
        local tuples_offset
        local pos = ""
        local last_tuple = box.NULL
        local last_pos

        -- Test fullscan pagination
        for i = 0, 5 do
            tuples1, pos = s:select(nil,
                    {limit=2, fullscan=true, fetch_pos=true, after=pos})
            tuples2 = s:select(nil,
                    {limit=2, fullscan=true, after=last_tuple})
            last_tuple = tuples2[2]
            tuples_offset = s:select(nil,
                    {limit=2, fullscan=true, offset=i*2})
            t.assert_equals(tuples1, tuples_offset)
            t.assert_equals(tuples2, tuples_offset)
        end
        tuples1, last_pos = s:select(nil,
                {limit=2, fullscan=true, fetch_pos=true, after=pos})
        t.assert_equals(tuples1, {})
        t.assert_equals(last_pos, pos)

        -- Test pagination on range iterators
        local key_iter = {
            ['GE'] = 3,
            ['GT'] = 2,
            ['LE'] = 9,
            ['LT'] = 10,
        }
        for iter, key in pairs(key_iter) do
            pos = ""
            last_tuple = box.NULL
            for i = 0, 4 do
                tuples1, pos = s:select(key,
                        {limit=2, iterator=iter, fetch_pos=true, after=pos})
                tuples2 = s:select(key,
                        {limit=2, iterator=iter, after=last_tuple})
                last_tuple = tuples2[2]
                tuples_offset = s:select(key,
                        {limit=2, iterator=iter, offset=i*2})
                t.assert_equals(tuples1, tuples_offset)
                t.assert_equals(tuples2, tuples_offset)
            end
            tuples1, last_pos = s:select(key,
                    {limit=2, iterator=iter, fetch_pos=true, after=pos})
            t.assert_equals(tuples1, {})
            t.assert_equals(last_pos, pos)
        end

        -- Test pagination on equality iterators
        s:replace{0, 0}
        s:replace{12, 2}
        for _, iter in pairs({'EQ', 'REQ'}) do
            pos = ""
            last_tuple = box.NULL
            for i = 0, 5 do
                tuples1, pos = s.index.sk:select(1,
                        {limit=2, iterator=iter, fetch_pos=true, after=pos})
                tuples2 = s.index.sk:select(1,
                        {limit=2, iterator=iter, after=last_tuple})
                last_tuple = tuples2[2]
                tuples_offset = s.index.sk:select(1,
                        {limit=2, iterator=iter, offset=i*2})
                t.assert_equals(tuples1, tuples_offset)
                t.assert_equals(tuples2, tuples_offset)
            end
            tuples, last_pos = s.index.sk:select(1,
                    {iterator=iter, limit=2, fetch_pos=true, after=pos})
            t.assert_equals(tuples, {})
            t.assert_equals(last_pos, pos)
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
        t.assert_equals(pos, "")

        for i = 1, 3 do
            s:replace{i, {1, 2, 3}}
        end

        local tuples
        local tuples_offset
        local pos = ""
        local last_pos

        -- Test fullscan pagination
        for i = 0, 4 do
            tuples, pos = sk:select(nil,
                    {limit=2, fullscan=true, fetch_pos=true, after=pos})
            tuples_offset = sk:select(nil,
                    {limit=2, fullscan=true, offset=i*2})
            t.assert_equals(tuples, tuples_offset)
        end
        tuples, last_pos = sk:select(nil,
                {limit=2, fullscan=true, fetch_pos=true, after=pos})
        t.assert_equals(tuples, {})
        t.assert_equals(last_pos, pos)

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
            tuples, last_pos = sk:select(key,
                    {limit=2, iterator=iter, fetch_pos=true, after=pos})
            t.assert_equals(tuples, {})
            t.assert_equals(last_pos, pos)
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
            tuples, last_pos = sk:select(2,
                    {iterator=iter, limit=2, fetch_pos=true, after=pos})
            t.assert_equals(tuples, {})
            t.assert_equals(last_pos, pos)
        end

        -- Test that after with tuple in multikey index returns an error
        local tuple = s.index.pk:get(1)
        t.assert_error_msg_contains(
                "Multikey index does not support position by tuple",
                sk.select, sk, nil, {fullscan=true, after=tuple})
    end)
end

--[[ We must return an empty position if there are no tuples
satisfying the filters. ]]--
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
        t.assert_equals(#tuples, 0)
        t.assert_equals(pos, "")
        s:replace{2, {1, 2}}
        s:replace{3, {1, 2}}
        s:replace{4, {1, 2}}
        tuples = s:select(3, {limit=1, iterator='GE', after=pos})
        t.assert_equals(tuples[1], {3, {1, 2}})

        tuples, pos = s.index.sk:select(4, {limit=1, iterator='GE', fetch_pos=true})
        t.assert_equals(#tuples, 0)
        t.assert_equals(pos, "")
        s:replace{5, {3, 4}}
        tuples = s.index.sk:select(4, {limit=2, iterator='GE', after=pos})
        t.assert_equals(#tuples, 1)
        t.assert_equals(tuples[1], {5, {3, 4}})
    end)
end

tree_g.test_invalid_positions = function(cg)
    cg.server:exec(function()
        local t = require('luatest')
        local s = box.space.s
        s:create_index('pk', {type = 'tree'})
        s:create_index('sk',
                {parts = {{field = 2, type = 'string'}}, type = 'tree'})
        s:replace{1, 'Zero'}

        local tuples
        local pos = {1, 2}
        local flag, err = pcall(function()
            s:select(1, {limit=1, iterator='GE', after=pos})
        end)
        print(err)
        t.assert_equals(flag, false)
        t.assert_equals(err.code, box.error.FIELD_TYPE)

        pos = "abcd"
        flag, err = pcall(function()
            s:select(1, {limit=1, iterator='GE', after=pos})
        end)
        t.assert_equals(flag, false)
        t.assert_equals(err.code, box.error.INVALID_POSITION)

        tuples, pos = s:select(nil, {fullscan=true, limit=1, fetch_pos=true})
        t.assert_equals(#tuples, 1)
        t.assert(#pos > 0)
        flag, err = pcall(function()
            s.index.sk:select(nil, {fullscan=true, limit=1, after=pos})
        end)
        t.assert_equals(flag, false)
        t.assert_equals(err.code, box.error.INVALID_POSITION)
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

        local tuples, pos = s:select(1, {iterator='GE', fetch_pos=true, limit=5})
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
        t.assert_equals(last_pos, pos)
        s:replace{10}
        s:replace{11}
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
        s:create_index("sk", {parts = {{field = 2, type = 'uint', path = '[*]'}}, type = "tree", unique=false})

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
            t.assert_equals(pos, last_pos)
        end
        tuples, last_pos = s:select(nil,
                {limit=2, fullscan=true, fetch_pos=true, after=pos})
        t.assert_equals(tuples, {})
        t.assert_equals(last_pos, pos)

        tuples = s:select(1)
        t.assert_error_msg_contains(
                "Multikey index does not support position by tuple",
                s.index.sk.tuple_pos, s.index.sk, tuples[1])
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

-- Tests for memtx tree features, such as functional index
local func_g = t.group('Memtx tree func index tests')

func_g.before_all(function()
    func_g.server = server:new{
        alias   = 'default',
    }
    func_g.server:start()
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
        local sk = s:create_index('func', {unique = false,
                                            func = 'func',
                                            parts = {{field = 1, type = 'uint'}}})

        -- Fetch position in empty space
        local tuples, pos = sk:select(nil,
                {limit=2, fullscan=true, fetch_pos=true})
        t.assert_equals(tuples, {})
        t.assert_equals(pos, "")

        for i = 1, 6 do
            s:replace{i, i}
        end
        for i = 7, 10 do
            s:replace{i, 6}
        end
        s:replace{11, 7}

        local tuples_offset
        local last_pos

        -- Test fullscan pagination
        for i = 0, 5 do
            tuples, pos = sk:select(nil,
                    {limit=2, fullscan=true, fetch_pos=true, after=pos})
            tuples_offset = sk:select(nil,
                    {limit=2, fullscan=true, offset=i*2})
            t.assert_equals(tuples, tuples_offset)
        end
        tuples, last_pos = sk:select(nil,
                {limit=2, fullscan=true, fetch_pos=true, after=pos})
        t.assert_equals(tuples, {})
        t.assert_equals(last_pos, pos)

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
            tuples, last_pos = sk:select(key,
                    {limit=2, iterator=iter, fetch_pos=true, after=pos})
            t.assert_equals(tuples, {})
            t.assert_equals(last_pos, pos)
        end

        -- Test pagination on equality iterators
        for _, iter in pairs({'EQ', 'REQ'}) do
            pos = ""
            for i = 0, 1 do
                tuples, pos = sk:select(7,
                        {limit=2, iterator=iter, fetch_pos=true, after=pos})
                tuples_offset = sk:select(7,
                        {limit=2, iterator=iter, offset=i*2})
                t.assert_equals(tuples, tuples_offset)
            end
            tuples, last_pos = sk:select(7,
                    {iterator=iter, limit=2, fetch_pos=true, after=pos})
            t.assert_equals(tuples, {})
            t.assert_equals(last_pos, pos)
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
        local sk = s:create_index('sk', {unique = false,
                                         func = 'func',
                                         parts = {{field = 1, type = 'uint'}}})

        -- Fetch position in empty space
        local tuples, pos = sk:select(nil,
                {limit=2, fullscan=true, fetch_pos=true})
        t.assert_equals(tuples, {})
        t.assert_equals(pos, "")

        for i = 1, 3 do
            s:replace{i, i}
        end

        local tuples
        local tuples_offset
        local pos = ""
        local last_pos

        -- Test fullscan pagination
        for i = 0, 4 do
            tuples, pos = sk:select(nil,
                    {limit=2, fullscan=true, fetch_pos=true, after=pos})
            tuples_offset = sk:select(nil,
                    {limit=2, fullscan=true, offset=i*2})
            t.assert_equals(tuples, tuples_offset)
        end
        tuples, last_pos = sk:select(nil,
                {limit=2, fullscan=true, fetch_pos=true, after=pos})
        t.assert_equals(tuples, {})
        t.assert_equals(last_pos, pos)

        -- Test pagination on range iterators
        local key_iter = {
            ['GE'] = 2,
            ['GT'] = 1,
            ['LE'] = 3,
            ['LT'] = 4,
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
            tuples, last_pos = sk:select(key,
                    {limit=2, iterator=iter, fetch_pos=true, after=pos})
            t.assert_equals(tuples, {})
            t.assert_equals(last_pos, pos)
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
            tuples, last_pos = sk:select(3,
                    {iterator=iter, limit=2, fetch_pos=true, after=pos})
            t.assert_equals(tuples, {})
            t.assert_equals(last_pos, pos)
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
