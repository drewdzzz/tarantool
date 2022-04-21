#!/usr/bin/env tarantool
local os = require('os')

box.cfg{
    listen = os.getenv("LISTEN"),
    read_only = true
}

require('fiber').set_default_slice(10000)
require('console').listen(os.getenv('ADMIN'))
