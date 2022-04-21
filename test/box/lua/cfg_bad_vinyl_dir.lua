#!/usr/bin/env tarantool
local os = require('os')

box.cfg{
    listen              = os.getenv("LISTEN"),
    vinyl_dir           = 'path/to/nowhere'
}

require('fiber').set_default_slice(10000)
require('console').listen(os.getenv('ADMIN'))
