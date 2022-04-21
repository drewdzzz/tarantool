#!/usr/bin/env tarantool

box.cfg{
    listen              = os.getenv("LISTEN"),
    memtx_memory        = 107374182,
    pid_file            = "tarantool.pid",
    wal_max_size        = 2500
}

require('fiber').set_default_slice(10000)
require('console').listen(os.getenv('ADMIN'))
