#!/usr/bin/env tarantool

box.error.injection.set('ERRINJ_VYRUN_INDEX_GARBAGE', true)

box.cfg {
    listen = os.getenv("LISTEN"),
    vinyl_memory = 128 * 1024 * 1024,
    force_recovery = true,
}

require('fiber').set_default_slice(10000)
require('console').listen(os.getenv('ADMIN'))
