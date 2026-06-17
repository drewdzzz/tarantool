local t = require('luatest')
local cbuilder = require('luatest.cbuilder')
local cluster = require('luatest.cluster')

local g = t.group()

g.after_each(function()
    if g.cluster ~= nil then
        g.cluster:drop()
        g.cluster = nil
    end
end)

-- Verify that the session.idle_timeout map is passed through to the
-- box.cfg.session_idle_timeout map unchanged.
g.test_session_idle_timeout = function()
    local config = cbuilder:new()
        :set_global_option('session.idle_timeout', {
            guest = 5,
            admin = 10,
        })
        :add_instance('i-001', {})
        :config()

    g.cluster = cluster:new(config)
    g.cluster:start()

    g.cluster['i-001']:exec(function(expected)
        t.assert_equals(box.cfg.session_idle_timeout, expected)
    end, {{guest = 5, admin = 10}})
end

-- Verify that removing the option on reconfiguration resets the
-- box.cfg value back to the default empty map (no idle timeouts).
g.test_session_idle_timeout_reset = function()
    local config = cbuilder:new()
        :set_global_option('session.idle_timeout', {guest = 5})
        :add_instance('i-001', {})
        :config()

    g.cluster = cluster:new(config)
    g.cluster:start()

    g.cluster['i-001']:exec(function()
        t.assert_equals(box.cfg.session_idle_timeout, {guest = 5})
    end)

    -- Remove the option, write and reload the new configuration.
    local config_2 = cbuilder:new(config)
        :set_global_option('session.idle_timeout', nil)
        :config()
    g.cluster:reload(config_2)

    g.cluster['i-001']:exec(function()
        t.assert_equals(box.cfg.session_idle_timeout, nil)
    end)
end
