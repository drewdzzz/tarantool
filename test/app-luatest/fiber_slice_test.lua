local t = require('luatest')
local g = t.group()

g.test_fiber_slice = function()
    local fiber = require('fiber')
    local clock = require('clock')

    local check_fiber_slice = function(fiber_f, expected_timeout, delta, set_custom_timeout)
        local fib = fiber.new(fiber_f)
        fib:set_joinable(true)
        if set_custom_timeout then
            fib:set_slice(expected_timeout)
        end
        local overhead = clock.monotonic()
        overhead = clock.monotonic() - overhead
        local start_time = clock.monotonic()
        local _, err = fib:join()
        t.assert_equals(tostring(err), "fiber slice is exceeded")
        local time_elapsed = clock.monotonic() - start_time - overhead
        t.assert_almost_equals(time_elapsed, expected_timeout, delta)
    end

    local timeout = 0.3
    -- delta is quite bit to pass this test in debug mode
    local delta = 0.1
    fiber.set_default_slice(timeout)

    local function check_slice()
        while true do
            fiber.check_slice()
        end
    end
    -- Test fiber.check_slice() method and deadline mechanism itself.
    check_fiber_slice(check_slice, timeout, delta)

    local function check_extend()
        fiber.self():extend_slice(timeout / 2)
        while true do
            fiber.check_slice()
        end
    end
    -- Check fiber.extend_slice().
    check_fiber_slice(check_extend, timeout * 1.5, delta)

    local function check_extend_with_table()
        fiber.self():extend_slice({warn = timeout / 2, err = timeout / 2})
        while true do
            fiber.check_slice()
        end
    end
    -- Check fiber.extend_slice() with table argument.
    check_fiber_slice(check_extend_with_table, timeout * 1.5, delta)

    -- Check fiber with custom slice.
    check_fiber_slice(check_slice, timeout / 2, delta, true)

    local function set_slice_from_inside()
        fiber.self():set_slice({warn = timeout / 2, err = timeout / 2})
        while true do
            fiber.check_slice()
        end
    end
    -- Set custom slice from inside the fiber.
    -- Also check fiber.set_slice() with table argument.
    check_fiber_slice(set_slice_from_inside, timeout / 2, delta)

    -- Check that default deadline timeout have not been changed after all.
    check_fiber_slice(check_slice, timeout, delta)

    -- Check set_default_deadline method with table argument.
    fiber.set_default_slice({warn = timeout * 1.5, err = timeout * 1.5})
    check_fiber_slice(check_slice, timeout * 1.5, delta)

    local function default_inside_fiber()
        fiber.set_default_slice(timeout / 2)
        while true do
            fiber.check_slice()
        end
    end
    -- Check that setting default slice will change current slice if it is not custom.
    check_fiber_slice(default_inside_fiber, timeout / 2, delta)

    local function custom_against_default()
        fiber.self():set_slice(timeout / 2)
        fiber.set_default_slice(timeout * 2)
        while true do
            fiber.check_slice()
        end
    end
    -- Check that setting default slice will not change current slice if it is custom.
    check_fiber_slice(custom_against_default, timeout / 2, delta)
end
