local t = require('luatest')
local server = require('luatest.server')
local uuid = require('uuid')
local fio = require('fio')

local function xlog_to_lsn(xlog)
    local xlog_name = string.sub(xlog, 1, -6)
    return tonumber(xlog_name)
end

local function check_xlog(xdir, lsn)
    local files = fio.listdir(xdir)
    local xlogs = {}
    for _, file in ipairs(files) do
        if string.match(file, '.xlog') then
            table.insert(xlogs, file)
        end
    end
    table.sort(xlogs)
    t.assert_ge(#xlogs, 2)
    t.assert_le(xlog_to_lsn(xlogs[1]), lsn)
    t.assert_ge(xlog_to_lsn(xlogs[2]), lsn)
end

local g = t.group('Space _gc_consumers')

g.before_each(function(cg)
    cg.server = server:new({
        alias = 'server',
        box_cfg = {
            checkpoint_count = 1,
            wal_cleanup_delay = 0,
        },
    })
    cg.server:start()
end)

g.after_each(function(cg)
    cg.server:drop()
end)

g.test_basic = function(cg)
    cg.server:exec(function()
        local fiber = require('fiber')
        local s = box.schema.create_space('test')
        s:create_index('pk')
        for i = 1, 10 do
            box.snapshot()
            fiber.sleep(0)
            for j = 1, 100 do
                box.space.test:replace{(i - 1) * 10 + j, 0}
            end
        end

        -- Create new consumer
        box.space._gc_consumers:insert{
            box.info.uuid,
            {[1] = 1004, __serialize = 'map'},
            {__serialize = 'map'}
        }

        -- for i = 1, 10 do
        --     for j = 1, 100 do
        --         box.space.test:replace{(i - 1) * 10 + j, 1}
        --     end
        --     box.snapshot()
        --     fiber.sleep(0)
        -- end
    end)
    check_xlog(server.workdir, 1004)
    cg.server:restart()
    cg.server:exec(function()
        for i = 1, 10 do
            for j = 1, 100 do
                box.space.test:replace{(i - 1) * 10 + j, 2}
            end
            box.snapshot()
            fiber.sleep(0)
        end
    end)
    check_xlog(server.workdir, 1004)
end

-- g = t.group('Persistent gc')

-- g.before_all(function(cg)
--     cg.master = server:new({
--         alias = 'master',
--         box_cfg = {
--             replication_timeout = 0.1,
--             checkpoint_count = 1,
--             wal_cleanup_delay = 0,
--         },
--     })
--     cg.master:start()
-- end)

-- g.after_all(function(cg)
--     cg.master:drop()
-- end)

-- g.test_persistent_gc_basic = function(cg)
--     -- Fill master with initial data
--     cg.master:exec(function()
--         local fiber = require('fiber')
--         local s = box.schema.create_space('test')
--         s:create_index('pk')
--         for i = 1, 10 do
--             box.snapshot()
--             fiber.sleep(0)
--             for j = 1, 100 do
--                 box.space.test:replace{(i - 1) * 10 + j, 0}
--             end
--         end
--     end)

--     -- Connect replica
--     local replica_uuid = uuid.str()
--     local box_cfg = table.deepcopy(cg.master.box_cfg)
--     box_cfg.replication = {cg.master.net_box_uri}
--     box_cfg.instance_uuid = replica_uuid
--     local replica = server:new({
--         alias = 'replica',
--         box_cfg = box_cfg,
--     })
--     replica:start()
--     t.helpers.retrying({}, function()
--         local master_lsn = replica:exec(function()
--             return box.info.vclock[1]
--         end)
--         local expected_lsn = 1004
--         t.assert_equals(master_lsn, expected_lsn)
--     end)
--     cg.master:exec(function()
--         local consumers = box.space._gc_consumers:select{}
--         t.assert_equals(#consumers, 1)
--         t.assert_equals(consumers[1].vclock[1], 1004)
--     end)

--     -- Stop replica
--     replica:stop()

--     -- Restart master to check if gc is persisted
--     cg.master:restart()
--     cg.master:exec(function()
--         local fiber = require('fiber')
--         for i = 1, 10 do
--             for j = 1, 100 do
--                 box.space.test:replace{(i - 1) * 100 + j, 1}
--             end
--             box.snapshot()
--             fiber.sleep(0)
--         end
--         -- Consumer hasn't changed - replica is disabled
--         local consumers = box.space._gc_consumers:select{}
--         t.assert_equals(#consumers, 1)
--         t.assert_equals(consumers[1].vclock[1], 1004)
--     end)
--     check_xlog(cg.master.workdir, 1004)
-- end
