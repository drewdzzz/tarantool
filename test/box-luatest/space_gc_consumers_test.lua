local t = require('luatest')
local server = require('luatest.server')
local uuid = require('uuid')
local fio = require('fio')

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

        -- Check consumer object with info
        t.assert_equals(#box.info.gc().consumers, 1)
        t.assert_equals(box.info.gc().consumers[1].vclock, {1004})
    end)
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
end

g.test_gc_consumers_outdated_consumer = function(cg)
    gc_consumers_outdated_consumer_test(cg, false)
end

g.test_gc_consumers_outdated_consumer_persist = function(cg)
    gc_consumers_outdated_consumer_test(cg, true)
end

-- The test checks if the consumer is created on replace, not commit
g.test_gc_consumers_register_early = function(cg)
    cg.master:exec(function()
        local fiber = require('fiber')
        local done = false
        local uuid = require('uuid')
        local replica_uuid = uuid.str()
        box.space._cluster:insert{10, replica_uuid}

        local vclock = box.info.vclock
        vclock[1] = math.random(100, 1000)
        box.begin()
        box.space._gc_consumers:insert{replica_uuid, vclock}
        t.assert_equals(#box.info.gc().consumers, 1)
        t.assert_equals(box.info.gc().consumers[1].vclock, vclock)
        box.commit()
    end)
end

-- The test checks if the consumer is deleted on commit, not replace
g.test_gc_consumers_unregister_late = function(cg)
    cg.master:exec(function()
        local fiber = require('fiber')
        local done = false
        local uuid = require('uuid')
        local replica_uuid = uuid.str()
        box.space._cluster:insert{10, replica_uuid}

        local vclock = box.info.vclock
        vclock[1] = math.random(100, 1000)
        box.space._gc_consumers:insert{replica_uuid, vclock}
        box.begin()
        box.space._gc_consumers:delete(replica_uuid)
        t.assert_equals(#box.info.gc().consumers, 1)
        t.assert_equals(box.info.gc().consumers[1].vclock, vclock)
        box.commit()
        t.assert_equals(box.info.gc().consumers, {})
    end)
end

g.test_gc_consumers_rollback = function(cg)
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
            box.rollback()
        end
        local fib = fiber.create(fiber_f)
        fib:set_joinable(true)

        box.space._cluster:delete(10)
        done = true
        fib:join()
        t.assert_equals(box.info.gc().consumers, {})
    end)
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
        box.space._gc_consumers:delete{replica_uuid}
        box.space._gc_consumers:insert{replica_uuid, new_vclock}

        t.assert_equals(#box.info.gc().consumers, 1)
        t.assert_equals(box.info.gc().consumers[1].vclock, new_vclock)
    end)
end

-- Check if concurrent manipulation over space _gc_consumers work correctly
g.test_gc_consumers_concurrency = function(cg)
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
            local vclock = box.info.vclock
            vclock[1] = math.random(2000, 5000)
            box.space._gc_consumers:replace{replica_uuid, vclock}
            while not done do
                fiber.sleep(0)
            end
            if math.random(3) < 2 then
                box.rollback()
            else
                box.commit()
            end
        end

        local fibs = {}
        for i = 1, 1000 do
            local fib = fiber.create(fiber_f)
            fib:set_joinable(true)
            table.insert(fibs, fib)
        end

        -- Shuffle fibers
        for i = #fibs, 2, -1 do
            local j = math.random(i)
            fibs[i], fibs[j] = fibs[j], fibs[i]
        end

        done = true
        for _, fib in pairs(fibs) do
            fib:join()
        end
        t.assert_equals(#box.info.gc().consumers, 1)
        local persisted_vclock = box.space._gc_consumers:get(replica_uuid)[2]
        t.assert_equals(box.info.gc().consumers[1].vclock, persisted_vclock)
    end)
end

-- Check if consumer is dropped on replica uuid update
g.test_gc_consumers_on_replica_uuid_update = function(cg)
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
