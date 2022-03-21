local server = require('test.luatest_helpers.server')
local t = require('luatest')
local g = t.group()


g.before_test('test_limit_iteration', function()
    g.server = server:new({alias = 'default'})
    g.server:start()
end)

g.before_test('test_limit_replace', function()
    g.server = server:new({
        alias = 'default',
        -- Disable WAL so that fiber does not yield on replace.
        box_cfg = {wal_mode = 'None'}
    })
    g.server:start()
end)

g.after_each(function()
    g.server:drop()
end)

g.test_limit_iteration = function()
    g.server:exec(function()
        local fiber = require('fiber')
        local check_fiber_slice = require('test.box-luatest.fiber_slice_checker')

        local s = box.schema.create_space('tester')
        s:create_index('pk')
        for i = 1, 1000 do
            s:insert{i}
        end
        local timeout = 0.3
        -- delta is quite bit to pass this test in debug mode
        local delta = 0.2
        fiber.set_default_slice(timeout)

        local function endless_select_func()
            while true do
                s:select{}
            end
        end
        check_fiber_slice(endless_select_func, timeout, delta)

        local function endless_get_func()
            while true do
                for i = 1, 1000 do
                    s:get(i)
                end
            end
        end
        check_fiber_slice(endless_get_func, timeout, delta)

        local function endless_pairs_func()
            while true do
                -- luacheck: ignore a
                local a = 0
                for _, v in s:pairs() do
                    a = v
                end
            end
        end
        check_fiber_slice(endless_pairs_func, timeout, delta)
    end)
end

g.test_limit_replace = function()
    g.server:exec(function()
        local fiber = require('fiber')
        local check_fiber_slice = require('test.box-luatest.fiber_slice_checker')

        local s = box.schema.create_space('tester')
        s:create_index('pk')
        for i = 1, 1000 do
            s:insert{i}
        end
        local timeout = 0.3
        -- delta is quite bit to pass this test in debug mode
        local delta = 0.2
        fiber.set_default_slice(timeout)

        local function endless_replace_func()
            while true do
                for i = 1, 1000 do
                    s:replace{i}
                end
            end
        end
        check_fiber_slice(endless_replace_func, timeout, delta)
    end)
end

g.before_test('test_limit_on_sigurg', function()
    g.server = server:new({
        alias = 'default',
    })
    g.server:start()
end)

g.test_limit_on_sigurg = function()
    local pid = g.server:eval('return box.info.pid')
    local os = require('os')
    local clock = require('clock')

    g.server:exec(function()
        local fiber = require('fiber')

        local s = box.schema.create_space('tester')
        s:create_index('pk')
        for i = 1, 1000 do
            s:insert{i}
        end
        local timeout = 10
        fiber.self():set_slice(timeout)
    end)

    local start_time = clock.monotonic()
    local future = g.server.net_box:eval('while true do box.space.tester:select{} end', {}, {is_async=true})
    -- Wait while fiber will be waken up.
    future:wait_result(2)
    -- Send SIGURG
    os.execute('kill -URG ' .. tonumber(pid))
    future:wait_result(8)
    local end_time = clock.monotonic()
    -- Must end before deadline is up.
    t.assert(end_time - start_time < 4)
end
