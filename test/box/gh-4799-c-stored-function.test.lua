--
-- gh-4799: C stored function's output used to be inconsistent:
-- multireturn if called locally, a tuple if called via netbox.
-- The latter was chosen as preferred behavior because it existed
-- long before local call was introduced in box.func.
--
build_path = os.getenv("BUILDDIR")
package.cpath = build_path..'/test/box/?.so;'..build_path..'/test/box/?.dylib;'..package.cpath
box.schema.func.create('function1.multireturn', {language = "C", exports = {'LUA'}})
box.schema.user.grant('guest', 'super')
c = require('net.box').connect(box.cfg.listen)

-- Both calls should return a tuple now.
c:call('function1.multireturn')
box.func['function1.multireturn']:call()

box.schema.user.revoke('guest', 'super')
box.schema.func.drop('function1.multireturn')
