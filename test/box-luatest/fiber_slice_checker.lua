local fiber = require('fiber')
local clock = require('clock')
local t = require('luatest')

local check_fiber_slice = function(_, fiber_f, expected_timeout, delta, set_custom_timeout)
    local fib = fiber.new(fiber_f)
    fib:set_joinable(true)
    if set_custom_timeout then
        fib:set_slice(expected_timeout)
    end
    local overhead = clock.monotonic()
    overhead = clock.monotonic() - overhead
    local start_time = clock.monotonic()
    local _, err = fib:join()
    local time_elapsed = clock.monotonic() - start_time - overhead
    t.assert_equals(tostring(err), "fiber slice is exceeded")
    t.assert_almost_equals(time_elapsed, expected_timeout, delta)
end

local mt = {__call = check_fiber_slice}
local fiber_slice_checker = setmetatable({}, mt)
return fiber_slice_checker
