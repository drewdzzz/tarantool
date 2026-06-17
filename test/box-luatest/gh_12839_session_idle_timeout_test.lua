local net = require('net.box')
local server = require('luatest.server')
local fiber = require('fiber')
local popen = require('popen')
local t = require('luatest')
local urilib = require('uri')

local tarantool = arg[-1]

-- Idle timeout (in seconds) used throughout the suite.
local TIMEOUT = 0.1
-- The replication test has to leave enough time for the replica to complete
-- the initial handshake.
local REPLICATION_TIMEOUT = 1
-- How long to wait while asserting that a connection stays open.
local ALIVE_WAIT = TIMEOUT * 4
local REPLICATION_ALIVE_WAIT = REPLICATION_TIMEOUT * 2

local g = t.group()

-- Wait (by polling) until an idle connection has been closed by the server.
local function assert_closed_by_idle(conn)
    t.helpers.retrying({timeout = TIMEOUT + 1, delay = 0.05}, function()
        t.assert_equals(conn.state, 'error',
                        'connection must be closed by idle timeout')
    end)
end

g.before_all(function(cg)
    cg.server = server:new({
        alias = 'master',
        box_cfg = {session_idle_timeout = {guest = TIMEOUT}},
    })
    cg.server:start()
    -- Create a user for re-auth test. DDL requires admin credentials.
    cg.server:exec(function()
        box.schema.user.create('bob', {if_not_exists = true})
        box.schema.user.passwd('bob', 'secret')
        box.schema.user.grant('bob', 'execute', 'universe',
                              nil, {if_not_exists = true})
        -- Register a slow function for the in-flight test.
        rawset(_G, 'slow_fn', function()
            require('fiber').sleep(0.5)
            return 'done'
        end)
        box.schema.func.create('slow_fn',
            {language = 'LUA', if_not_exists = true})
        box.schema.user.grant('guest', 'execute', 'function', 'slow_fn',
                              {if_not_exists = true})
    end)
end)

g.after_all(function(cg)
    cg.server:drop()
end)

g.test_idle_close = function(cg)
    local conn = net.connect(cg.server.net_box_uri)
    t.assert_equals(conn.state, 'active')
    conn:ping()
    assert_closed_by_idle(conn)
    conn:close()
end

g.test_activity_resets_timer = function(cg)
    local conn = net.connect(cg.server.net_box_uri)
    t.assert_equals(conn.state, 'active')
    -- Ping every TIMEOUT/3 for well beyond the idle timeout.
    local deadline = fiber.time() + ALIVE_WAIT
    while fiber.time() < deadline do
        conn:ping()
        fiber.sleep(TIMEOUT / 3)
    end
    t.assert_equals(conn.state, 'active',
                    'connection must stay alive while active')
    conn:close()
end

g.test_reauth_after_idle_close = function(cg)
    local conn = net.connect(cg.server.net_box_uri)
    t.assert_equals(conn.state, 'active')
    conn:ping()
    assert_closed_by_idle(conn)
    conn:close()

    -- Re-connect with explicit auth -- must succeed.
    local conn2 = net.connect(cg.server.net_box_uri,
                              {user = 'bob', password = 'secret'})
    t.assert_equals(conn2.state, 'active')
    t.assert(conn2:ping())
    conn2:close()
end

g.test_no_kill_during_request = function(cg)
    local conn = net.connect(cg.server.net_box_uri)
    t.assert_equals(conn.state, 'active')

    -- Start an async call that takes 1 s (> the idle timeout).
    -- request_count stays > 0 while TX is executing, so the timer re-arms.
    local fut = conn:call('slow_fn', {}, {is_async = true})

    -- Give iproto time to enqueue the message (request_count becomes 1).
    fiber.sleep(0.1)

    -- Wait past the idle timeout but before the call finishes.
    fiber.sleep(TIMEOUT * 2)

    -- Connection must still be alive because a request is in flight.
    t.assert_equals(conn.state, 'active',
                    'connection must not be closed while request is in flight')

    -- Collect the result.
    local result = fut:wait_result(10)
    t.assert_equals(result[1], 'done')
    conn:close()
end

g.test_idle_timeout_is_per_user = function(cg)
    -- Guest has a timeout, other users have none.
    -- Guest connection must be killed.
    local conn_guest = net.connect(cg.server.net_box_uri)
    t.assert_equals(conn_guest.state, 'active')
    conn_guest:ping()
    assert_closed_by_idle(conn_guest)
    conn_guest:close()

    -- Bob has no timeout configured, must stay alive.
    local conn_bob = net.connect(cg.server.net_box_uri,
                                 {user = 'bob', password = 'secret'})
    t.assert_equals(conn_bob.state, 'active')
    conn_bob:ping()
    fiber.sleep(ALIVE_WAIT)
    t.assert_equals(conn_bob.state, 'active',
                    'bob connection must not be closed (no timeout)')
    conn_bob:close()
end

-- Group for the "disabled" test: starts without any idle timeout.
g = t.group('session_idle_timeout_disabled')

g.before_all(function(cg)
    cg.server = server:new({
        alias = 'master_disabled',
        box_cfg = {session_idle_timeout = {}},
    })
    cg.server:start()
end)

g.after_all(function(cg)
    cg.server:drop()
end)

g.test_disabled_timeout = function(cg)
    local conn = net.connect(cg.server.net_box_uri)
    t.assert_equals(conn.state, 'active')
    fiber.sleep(ALIVE_WAIT)
    t.assert_equals(conn.state, 'active',
                    'connection must stay open when timeout is disabled')
    conn:close()
end

-- Each entry exercises one validation branch: a bad value paired with a
-- substring expected in the rejection error.
local INVALID_CONFIGS = {
    {value = 5,             msg = 'should be of type table'},
    {value = 'foo',         msg = 'should be of type table'},
    {value = {[1] = 5},     msg = 'user name must be a string'},
    {value = {guest = 'x'}, msg = 'timeout must be a number'},
    {value = {guest = -1},  msg = 'timeout must be >= 0'},
}

g.test_invalid_config_dynamic = function(cg)
    for _, case in ipairs(INVALID_CONFIGS) do
        local err = cg.server:exec(function(value)
            local ok, e = pcall(box.cfg, {session_idle_timeout = value})
            assert(not ok)
            return tostring(e)
        end, {case.value})
        t.assert_str_contains(err, case.msg)
    end
    -- The live config must remain unchanged after the failed updates.
    local value = cg.server:exec(function()
        return box.cfg.session_idle_timeout
    end)
    t.assert_equals(value, {})
end

g.test_invalid_config_startup = function()
    for _, case in ipairs(INVALID_CONFIGS) do
        -- Render the bad value as a Lua literal for the startup script.
        local literal
        if type(case.value) == 'table' then
            local k, v = next(case.value)
            if type(k) == 'string' then
                literal = ('{%s = %s}'):format(k,
                    type(v) == 'string' and ('%q'):format(v) or tostring(v))
            else
                literal = ('{[%s] = %s}'):format(tostring(k), tostring(v))
            end
        elseif type(case.value) == 'string' then
            literal = ('%q'):format(case.value)
        else
            literal = tostring(case.value)
        end
        local script = ('box.cfg{session_idle_timeout = %s}'):format(literal)
        local handle, err = popen.new({tarantool, '-e', script},
                                       {stdin = popen.opts.DEVNULL,
                                        stdout = popen.opts.DEVNULL,
                                        stderr = popen.opts.DEVNULL})
        t.assert(handle, err)
        -- Wait for process to exit.
        local status = handle:wait()
        t.assert_not_equals(status.state, 'running',
                            'server must have exited on bad config: ' .. script)
        t.assert_not_equals(status.exit_code, 0,
                            'server should exit with error: ' .. script)
        handle:close()
    end
end

g = t.group('session_idle_timeout_replication')

g.before_all(function(cg)
    cg.master = server:new({
        alias = 'master_timeout',
        box_cfg = {session_idle_timeout = {guest = REPLICATION_TIMEOUT}},
    })
    cg.master:start()
    cg.master:exec(function()
        -- Granting a universe privilege to oneself requires admin: the
        -- net.box session runs as guest, which does not own the universe.
        box.session.su('admin', function()
            box.schema.user.grant('guest', 'replication')
        end)
    end)
end)

g.after_all(function(cg)
    cg.master:drop()
end)

-- Test: a replication connection survives the idle timeout.
g.test_replication_connection_not_killed = function(cg)
    local uri = urilib.parse(cg.master.net_box_uri)
    local replica = server:new({
        alias = 'replica',
        box_cfg = {replication = uri.unix}
    })
    replica:start()
    t.helpers.retrying({timeout = REPLICATION_TIMEOUT, delay = 0.05}, function()
        local status = replica:exec(function()
            return box.info.replication[1].upstream.status
        end)
        t.assert_equals(status, 'follow')
    end)
    -- Wait longer than session_idle_timeout after the replication connection
    -- is established.
    fiber.sleep(REPLICATION_ALIVE_WAIT)
    -- Replica must still be connected to the master.
    local status = replica:exec(function()
        return box.info.replication[1].upstream.status
    end)
    t.assert_equals(status, 'follow',
                    'replication connection must survive idle timeout')
    replica:drop()
end

g = t.group('session_idle_timeout_with_user_ddl')

g.before_all(function(cg)
    cg.server = server:new()
    cg.server:start()
end)

g.after_all(function(cg)
    cg.server:drop()
end)

-- Checks that the timeout is keyed by user name, not by uid.
g.test_uid_reuse_does_not_inherit_timeout = function(cg)
    cg.server:exec(function(timeout)
        -- Configure a timeout for user 'timed'.
        box.schema.user.create('timed', {if_not_exists = true})
        box.schema.user.passwd('timed', 'pw')
        box.schema.user.grant('timed', 'execute', 'universe')
        box.cfg{session_idle_timeout = {timed = timeout}}

        -- Drop the user, freeing its uid.
        box.schema.user.drop('timed')

        -- Create a new user; it may reuse the freed uid.
        box.schema.user.create('fresh', {if_not_exists = true})
        box.schema.user.passwd('fresh', 'pw2')
        box.schema.user.grant('fresh', 'execute', 'universe')
    end, {TIMEOUT})

    local conn = net.connect(cg.server.net_box_uri,
                             {user = 'fresh', password = 'pw2'})
    t.assert_equals(conn.state, 'active')
    conn:ping()
    fiber.sleep(ALIVE_WAIT)
    -- 'fresh' is not in the config; the timeout is resolved by name at auth
    -- time, so reusing the dropped user's uid does not bring its timeout.
    t.assert_equals(conn.state, 'active',
                    'new user must not inherit the dropped user timeout')
    conn:close()

    -- Cleanup.
    cg.server:exec(function()
        box.cfg{session_idle_timeout = {}}
        box.schema.user.drop('fresh')
    end)
end

g.test_recreate_user_reapplies_timeout = function(cg)
    cg.server:exec(function(timeout)
        box.schema.user.create('revived', {if_not_exists = true})
        box.schema.user.passwd('revived', 'pw3')
        box.schema.user.grant('revived', 'execute', 'universe')
        box.cfg{session_idle_timeout = {revived = timeout}}

        -- Drop and re-create with the same name (new uid).
        box.schema.user.drop('revived')
        box.schema.user.create('revived', {if_not_exists = true})
        box.schema.user.passwd('revived', 'pw3')
        box.schema.user.grant('revived', 'execute', 'universe')
    end, {TIMEOUT})

    local conn = net.connect(cg.server.net_box_uri,
                             {user = 'revived', password = 'pw3'})
    t.assert_equals(conn.state, 'active')
    conn:ping()
    -- Timeout is keyed by name; re-created user must still be subject to it.
    assert_closed_by_idle(conn)
    conn:close()

    -- Cleanup.
    cg.server:exec(function()
        box.cfg{session_idle_timeout = {}}
        box.schema.user.drop('revived')
    end)
end

g.test_timeout_applied_to_later_created_user = function(cg)
    cg.server:exec(function(timeout)
        -- Configure a timeout for a user that does not exist yet. This must
        -- be accepted silently (no error, no warning).
        box.cfg{session_idle_timeout = {latecomer = timeout}}
        -- Now create the user.
        box.schema.user.create('latecomer', {if_not_exists = true})
        box.schema.user.passwd('latecomer', 'pw6')
        box.schema.user.grant('latecomer', 'execute', 'universe')
    end, {TIMEOUT})

    local conn = net.connect(cg.server.net_box_uri,
                             {user = 'latecomer', password = 'pw6'})
    t.assert_equals(conn.state, 'active')
    conn:ping()
    -- The timeout must now apply to the freshly created user.
    assert_closed_by_idle(conn)
    conn:close()

    cg.server:exec(function()
        box.cfg{session_idle_timeout = {}}
        box.schema.user.drop('latecomer')
    end)
end

g.test_rename_user_changes_timeout = function(cg)
    cg.server:exec(function(timeout)
        box.schema.user.create('before', {if_not_exists = true})
        box.schema.user.passwd('before', 'pw7')
        box.schema.user.grant('before', 'execute', 'universe')
        box.cfg{session_idle_timeout = {before = timeout}}
    end, {TIMEOUT})

    -- While named 'before', the user is subject to the timeout.
    local conn = net.connect(cg.server.net_box_uri,
                             {user = 'before', password = 'pw7'})
    t.assert_equals(conn.state, 'active')
    conn:ping()
    assert_closed_by_idle(conn)
    conn:close()

    -- Rename the user to 'after'; the config still keys 'before'.
    cg.server:exec(function()
        local uid = box.space._user.index.name:get('before')[1]
        box.space._user:update(uid, {{'=', 3, 'after'}})
    end)

    -- 'after' is not in the config, so the renamed user has no timeout.
    local conn2 = net.connect(cg.server.net_box_uri,
                              {user = 'after', password = 'pw7'})
    t.assert_equals(conn2.state, 'active')
    conn2:ping()
    fiber.sleep(ALIVE_WAIT)
    t.assert_equals(conn2.state, 'active',
                    'renamed user must not keep the old name timeout')
    conn2:close()

    -- Cleanup.
    cg.server:exec(function()
        box.cfg{session_idle_timeout = {}}
        box.schema.user.drop('after')
    end)
end

g.test_reauth_switches_timeout = function(cg)
    cg.server:exec(function(timeout)
        box.cfg{session_idle_timeout = {quick = timeout}}
        box.schema.user.create('quick', {if_not_exists = true})
        box.schema.user.passwd('quick', 'pw4')
        box.schema.user.grant('quick', 'execute', 'universe')

        box.schema.user.create('slow', {if_not_exists = true})
        box.schema.user.passwd('slow', 'pw5')
        box.schema.user.grant('slow', 'execute', 'universe')
    end, {TIMEOUT})

    -- 'quick' connection must be auto-closed after the idle timeout.
    local conn_quick = net.connect(cg.server.net_box_uri,
                                   {user = 'quick', password = 'pw4'})
    t.assert_equals(conn_quick.state, 'active')
    conn_quick:ping()
    assert_closed_by_idle(conn_quick)
    conn_quick:close()

    -- 'slow' connection must remain open (no timeout configured).
    local conn_slow = net.connect(cg.server.net_box_uri,
                                  {user = 'slow', password = 'pw5'})
    t.assert_equals(conn_slow.state, 'active')
    conn_slow:ping()
    fiber.sleep(ALIVE_WAIT)
    t.assert_equals(conn_slow.state, 'active',
                    'user with no timeout must not be auto-closed')
    conn_slow:close()

    -- Cleanup.
    cg.server:exec(function()
        box.cfg{session_idle_timeout = {}}
        box.schema.user.drop('quick')
        box.schema.user.drop('slow')
    end)
end
