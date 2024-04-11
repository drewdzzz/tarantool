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
    table.sort(files)
    print(require('json').encode(files))
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

local function check_xlog_gt(xdir, lsn)
    local files = fio.listdir(xdir)
    table.sort(files)
    print(require('json').encode(files))
    local xlogs = {}
    for _, file in ipairs(files) do
        if string.match(file, '.xlog') then
            table.insert(xlogs, file)
        end
    end
    table.sort(xlogs)
    t.assert_ge(#xlogs, 1)
    t.assert_gt(xlog_to_lsn(xlogs[1]), lsn)
end

local function create_and_fill_space()
    local s = box.schema.create_space('test')
    s:create_index('pk')
    for i = 1, 10 do
        box.snapshot()
        for j = 1, 100 do
            box.space.test:replace{(i - 1) * 10 * j, 0}
        end
    end
    return box.info.vclock
end

local function load_space()
    local fiber = require('fiber')
    for i = 1, 10 do
        for j = 1, 100 do
            box.space.test:replace{(i - 1) * 100 * j, 1}
        end
        box.snapshot()
        fiber.sleep(0)
    end
    return box.info.vclock
end

local g = t.group('Space _gc_consumers')

g.before_each(function(cg)
    cg.master = server:new({
        alias = 'master',
        box_cfg = {
            wal_cleanup_delay = 0,
            memtx_use_mvcc_engine = true,
        },
    })
    cg.master:start()
end)

g.after_each(function(cg)
    cg.master:drop()
end)

local function gc_consumers_basic_test(cg, restart)
    cg.master:exec(create_and_fill_space)
    cg.master:exec(function()
        local uuid = require('uuid')
        local replica_uuid = uuid.str()
        box.space._cluster:insert{10, replica_uuid}
        t.assert_equals(box.info.vclock, {1004})
        box.space._gc_consumers:insert{replica_uuid, box.info.vclock}
    end)

    -- Restart master to check if gc is persisted
    if restart then
        cg.master:restart()
    end

    cg.master:exec(load_space)
    cg.master:exec(function()
        -- Check if consumer hasn't changed
        local consumers = box.space._gc_consumers:select{}
        t.assert_equals(#consumers, 1)
        t.assert_equals(consumers[1].vclock[1], 1004)
    end)
    check_xlog(cg.master.workdir, 1007)
end

g.test_gc_consumers_basic = function(cg)
    gc_consumers_basic_test(cg, false)
end

g.test_gc_consumers_basic_persist = function(cg)
    gc_consumers_basic_test(cg, true)
end

-- Test if outdated consumer has no effect on GC.
local function gc_consumers_outdated_consumer_test(cg, restart)
    cg.master:exec(create_and_fill_space)
    cg.master:exec(function()
        local uuid = require('uuid')
        local replica_uuid = uuid.str()
        box.space._cluster:insert{10, replica_uuid}
        local vclock = box.info.vclock
        t.assert_equals(vclock, {1004})
        vclock[1] = 10
        box.space._gc_consumers:insert{replica_uuid, vclock}
        t.assert_equals(box.info.gc().consumers, {})
    end)

    -- Restart master to check if gc is persisted
    if restart then
        cg.master:restart()
    end

    cg.master:exec(load_space)
    cg.master:exec(function()
        -- Check if consumer hasn't changed
        local consumers = box.space._gc_consumers:select{}
        t.assert_equals(#consumers, 1)
        t.assert_equals(consumers[1].vclock[1], 10)
        t.assert_equals(box.info.gc().consumers, {})
    end)
    check_xlog_gt(cg.master.workdir, 1004)
end

g.test_gc_consumers_outdated_consumer = function(cg)
    gc_consumers_outdated_consumer_test(cg, false)
end

g.test_gc_consumers_outdated_consumer_persist = function(cg)
    gc_consumers_outdated_consumer_test(cg, true)
end

g.test_gc_consumers_vclock_clash = function(cg)
    cg.master:exec(create_and_fill_space)

    cg.master:exec(function()
        local fiber = require('fiber')
        local uuid = require('uuid')
        t.assert_equals(box.info.vclock, {1003})
        local saved_vclock = table.deepcopy(box.info.vclock)
        for i = 2, 10 do
            local replica_uuid = uuid.str()
            box.space._cluster:insert{i, replica_uuid}
            box.space._gc_consumers:insert{replica_uuid, saved_vclock}
        end
        for i = 1, 10 do
            for j = 1, 100 do
                box.space.test:replace{(i - 1) * 100 * j, 1}
            end
            box.snapshot()
            fiber.sleep(0)
        end
    end)
    check_xlog(cg.master.workdir, 1003)

    cg.master:exec(function()
        local fiber = require('fiber')
        box.space._cluster:delete{10}
        for i = 1, 10 do
            for j = 1, 100 do
                box.space.test:replace{(i - 1) * 100 * j, 1}
            end
            box.snapshot()
            fiber.sleep(0)
        end
    end)
    check_xlog(cg.master.workdir, 1003)
end

g.test_gc_consumers_register_early = function(cg)
    cg.master:exec(create_and_fill_space)

    cg.master:exec(function()
        local fiber = require('fiber')
        local done = false
        local uuid = require('uuid')
        local replica_uuid = uuid.str()
        box.space._cluster:insert{10, replica_uuid}
        t.assert_equals(box.info.vclock, {1004})

        local fiber_f = function()
            box.begin()
            box.space._gc_consumers:insert{replica_uuid, box.info.vclock}
            while not done do
                fiber.sleep(0)
            end
            box.commit()
        end
        local fib = fiber.create(fiber_f)
        fib:set_joinable(true)

        for i = 1, 10 do
            for j = 1, 100 do
                box.space.test:replace{(i - 1) * 100 * j, 1}
            end
            box.snapshot()
            fiber.sleep(0)
        end
        done = true
        fib:join()
    end)
    check_xlog(cg.master.workdir, 1004)
end

g.test_gc_consumers_unregister_late = function(cg)
    cg.master:exec(create_and_fill_space)

    cg.master:exec(function()
        local fiber = require('fiber')
        local done = false
        local uuid = require('uuid')
        local replica_uuid = uuid.str()
        box.space._cluster:insert{10, replica_uuid}
        t.assert_equals(box.info.vclock, {1004})
        box.space._gc_consumers:insert{replica_uuid, box.info.vclock}

        local fiber_f = function()
            box.begin()
            box.space._gc_consumers:delete{replica_uuid}
            while not done do
                fiber.sleep(0)
            end
            box.rollback()
        end
        local fib = fiber.create(fiber_f)
        fib:set_joinable(true)

        for i = 1, 10 do
            for j = 1, 100 do
                box.space.test:replace{(i - 1) * 100 * j, 1}
            end
            box.snapshot()
            fiber.sleep(0)
        end
        done = true
        fib:join()
    end)
    check_xlog(cg.master.workdir, 1000)
end

-- Check if manipulations inside one transaction work correctly.
g.test_gc_consumers_in_transactions = function(cg)
    cg.master:exec(function()
        local uuid = require('uuid')
        local replica_uuid = uuid.str()
        box.space._cluster:insert{10, replica_uuid}
        local vclock = box.info.vclock
        local new_vclock = table.deepcopy(vclock)
        new_vclock[1] = 4242

        box.begin()
        box.space._gc_consumers:insert{replica_uuid, vclock}
        box.space._gc_consumers:replace{replica_uuid, new_vclock}
        box.space._gc_consumers:delete{replica_uuid}
        box.space._gc_consumers:insert{replica_uuid, vclock}
        box.rollback()

        -- No consumers should be registered
        t.assert_equals(box.info.gc().consumers, {})

        box.begin()
        box.space._gc_consumers:insert{replica_uuid, vclock}
        box.space._gc_consumers:replace{replica_uuid, new_vclock}
        box.space._gc_consumers:delete{replica_uuid}
        box.space._gc_consumers:insert{replica_uuid, new_vclock}
        box.commit()

        t.assert_equals(#box.info.gc().consumers, 1)
        t.assert_equals(box.info.gc().consumers[1].vclock, new_vclock)
    end)
end

-- Check if manipulations inside one transaction work correctly.
g.test_gc_consumers_in_transactions = function(cg)
    cg.master:exec(function()
        local uuid = require('uuid')
        local replica_uuid = uuid.str()
        local new_replica_uuid = uuid.str()
        box.space._cluster:insert{10, replica_uuid, 'name'}
        box.space._gc_consumers:insert{replica_uuid, box.info.vclock}

        box.space._cluster:replace{10, new_replica_uuid, 'name'}
        -- Consumer of old replica must be deleted.
        t.assert_equals(box.space._gc_consumers:select(), {})
    end)
end

g = t.group('Persistent gc')

g.before_each(function(cg)
    cg.master = server:new({
        alias = 'master',
        box_cfg = {
            replication_timeout = 0.1,
            checkpoint_count = 1,
            wal_cleanup_delay = 0,
            memtx_use_mvcc_engine = true,
        },
    })
    cg.master:start()
end)

g.after_each(function(cg)
    cg.master:drop()
end)

g.test_persistent_gc_basic = function(cg)
    cg.master:exec(create_and_fill_space)

    -- Connect replica
    local replica_uuid = uuid.str()
    local box_cfg = table.deepcopy(cg.master.box_cfg)
    box_cfg.replication = {cg.master.net_box_uri}
    box_cfg.instance_uuid = replica_uuid
    local replica = server:new({
        alias = 'replica',
        box_cfg = box_cfg,
    })
    replica:start()
    t.helpers.retrying({}, function()
        local master_lsn = replica:exec(function()
            return box.info.vclock[1]
        end)
        local expected_lsn = 1004
        t.assert_equals(master_lsn, expected_lsn)
    end)
    cg.master:exec(function()
        local consumers = box.space._gc_consumers:select{}
        t.assert_equals(#consumers, 1)
        t.assert_equals(consumers[1].vclock[1], 1004)
    end)

    -- Stop replica
    replica:stop()

    -- Restart master to check if gc is persisted
    cg.master:restart()
    cg.master:exec(load_space)
    check_xlog(cg.master.workdir, 1007)
    cg.master:exec(function()
        -- Consumer hasn't changed - replica is disabled
        local consumers = box.space._gc_consumers:select{}
        t.assert_equals(#consumers, 1)
        t.assert_equals(consumers[1].vclock[1], 1004)
    end)
    check_xlog(cg.master.workdir, 1007)
end

local function persistent_gc_delete_consumer_test(cg, rebootstrap)
    cg.master:exec(create_and_fill_space)

    -- Connect replica
    local replica_uuid = uuid.str()
    local box_cfg = table.deepcopy(cg.master.box_cfg)
    box_cfg.replication = {cg.master.net_box_uri}
    box_cfg.instance_uuid = replica_uuid
    local replica = server:new({
        alias = 'replica',
        box_cfg = box_cfg,
    })
    replica:start()
    t.helpers.retrying({}, function()
        local master_lsn = replica:exec(function()
            return box.info.vclock[1]
        end)
        local expected_lsn = 1004
        t.assert_equals(master_lsn, expected_lsn)
    end)
    cg.master:exec(function()
        local consumers = box.space._gc_consumers:select{}
        t.assert_equals(#consumers, 1)
        t.assert_equals(consumers[1].vclock[1], 1004)
    end)

    -- Stop replica
    replica:stop()

    -- Restart master to check if gc is persisted
    cg.master:restart()
    cg.master:exec(load_space)
    check_xlog(cg.master.workdir, 1007)
    cg.master:exec(function()
        -- Drop consumer
        local replica_uuid = box.space._cluster:select{}[2][2]
        box.space._gc_consumers:delete(replica_uuid)
    end)
    if rebootstrap then
        -- Load space to prune xlogs
        cg.master:exec(load_space)
    end
    replica:start()
    t.helpers.retrying({}, function()
        local master_lsn = replica:exec(function()
            return box.info.vclock[1]
        end)
        t.assert_ge(master_lsn, 2004)
    end)
end

g.test_resubscribe_without_consumer = function(cg)
    persistent_gc_delete_consumer_test(cg, false)
end

g.test_resubscribe_without_consumer_with_rebootstrap = function(cg)
    persistent_gc_delete_consumer_test(cg, true)
end
