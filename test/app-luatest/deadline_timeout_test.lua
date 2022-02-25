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
            local time_elapsed = clock.bench(fib.join, fib)[1]
            t.assert_almost_equals(time_elapsed, expected_timeout, delta)
        end

        local timeout = 0.5
        local delta = 0.05
        fiber.set_default_deadline_timeout(timeout)

        local function check_deadline_func()
            local a = 0
            while fiber.check_deadline() do
                a = a + 1
            end
        end
        -- Test fiber.check_deadline() method and deadline mechanism itself.
        check_fiber_deadline(check_deadline_func, timeout, delta)

        local function check_deadline_func_with_increase()
            fiber.increase_deadline_timeout(timeout)
            local a = 0
            while fiber.check_deadline() do
                a = a + 1
            end
        end
        -- Check deadlines with increased timeout.
        check_fiber_deadline(check_deadline_func_with_increase, timeout * 2, delta)

        -- Check deadlines with custom timeout.
        check_fiber_deadline(check_deadline_func, timeout / 2, delta, true)

        local function set_deadline_from_inside()
            fiber.self():set_deadline_timeout(timeout / 2)
            local a = 0
            while fiber.check_deadline() do
                a = a + 1
            end
        end
        -- Set custom timeout from inside the fiber.
        check_fiber_deadline(set_deadline_from_inside, timeout / 2, delta)

        -- Check that default deadline timeout have not been changed after all.
        check_fiber_deadline(check_deadline_func, timeout, delta)
    end)
end
