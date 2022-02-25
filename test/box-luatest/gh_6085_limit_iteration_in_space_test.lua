local server = require('test.luatest_helpers.server')
local t = require('luatest')
local g = t.group()

g.before_each(function()
    g.server = server:new({alias = 'default'})
    g.server:start()
end)

g.after_each(function()
    g.server:drop()
end)

g.test_deadline_timeout = function()
    g.server:exec(function()
        local t = require('luatest')
        local fiber = require('fiber')
        local clock = require('clock')

        local check_fiber_deadline = function(fiber_f, expected_timeout, delta, set_custom_timeout)
            local fib = fiber.new(fiber_f)
            fib:set_joinable(true)
            if set_custom_timeout then
                fib:set_deadline_timeout(expected_timeout)
            end
            local ret = clock.bench(fib.join, fib)
            local time_elapsed = ret[1]
            local err = ret[3]
            t.assert_almost_equals(time_elapsed, expected_timeout, delta)
            t.assert_equals(err:unpack().code, box.error.SPACE_ITERATION_TIMEOUT)
        end

        local s = box.schema.create_space('tester')
        s:create_index('pk')
        for i = 1, 1000 do
            s:insert{i}
        end
        local timeout = 0.1
        local delta = 0.05
        fiber.set_default_deadline_timeout(timeout)

        local function endless_select_func()
            while true do
                s:select{}
            end
        end
        check_fiber_deadline(endless_select_func, timeout, delta)

        local function endless_get_func()
            while true do
                for i = 1, 1000 do
                    s:get(i)
                end
            end
        end
        check_fiber_deadline(endless_get_func, timeout, delta)

        local function endless_random_func()
            while true do
                for i = 1, 1000 do
                    s:random(42)
                end
            end
        end
        check_fiber_deadline(endless_random_func, timeout, delta)

        local function endless_pairs_func()
            while true do
                local a = 0
                for _, v in s:pairs() do
                    a = v
                end
            end
        end
        check_fiber_deadline(endless_pairs_func, timeout, delta)

    end)
end
