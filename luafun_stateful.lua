---
--- Lua Fun - a high-performance functional programming library for LuaJIT
---
--- Copyright (c) 2013-2017 Roman Tsisyk <roman@tsisyk.com>
---
--- Distributed under the MIT/X11 License. See COPYING.md for more details.
---

local exports = {}
local methods = {}

-- compatibility with Lua 5.1/5.2
local unpack = rawget(table, "unpack") or unpack

--------------------------------------------------------------------------------
-- Tools
--------------------------------------------------------------------------------

local function deepcopy(orig) -- used by cycle()
    local orig_type = type(orig)
    local copy
    if orig_type == 'table' then
        copy = {}
        for orig_key, orig_value in next, orig, nil do
            copy[deepcopy(orig_key)] = deepcopy(orig_value)
        end
    else
        copy = orig
    end
    return copy
end

local iterator_mt = {
    -- usually called by for-in loop
    __call = function(self, param, state)
        return self.gen(param, state)
    end;
    __tostring = function(self)
        return '<generator>'
    end;
    -- add all exported methods
    __index = methods;
}

local wrap = function(gen, param, state)
    return setmetatable({
        gen = gen,
        param = param,
        state = state
    }, iterator_mt), param, state
end
exports.wrap = wrap

local unwrap = function(self)
    return self.gen, self.param, self.state
end
methods.unwrap = unwrap

--------------------------------------------------------------------------------
-- Basic Functions
--------------------------------------------------------------------------------

local nil_gen = function(_param, _state)
    return nil
end

local string_gen = function(param, state)
    local state = state + 1
    if state > #param then
        return nil
    end
    local r = string.sub(param, state, state)
    return state, r
end

local ipairs_gen = ipairs({}) -- get the generating function from ipairs

local pairs_gen = pairs({ a = 0 }) -- get the generating function from pairs
local map_gen = function(tab, key)
    local value
    local key, value = pairs_gen(tab, key)
    return key, key, value
end

local rawiter = function(obj, param, state)
    assert(obj ~= nil, "invalid iterator")
    if type(obj) == "table" then
        local mt = getmetatable(obj);
        if mt ~= nil then
            if mt == iterator_mt then
                return obj.gen, obj.param, obj.state
            elseif mt.__ipairs ~= nil then
                return mt.__ipairs(obj)
            elseif mt.__pairs ~= nil then
                return mt.__pairs(obj)
            end
        end
        if #obj > 0 then
            -- array
            return ipairs(obj)
        else
            -- hash
            return map_gen, obj, nil
        end
    elseif (type(obj) == "function") then
        return obj, param, state
    elseif (type(obj) == "string") then
        if #obj == 0 then
            return nil_gen, nil, nil
        end
        return string_gen, obj, 0
    end
    error(string.format('object %s of type "%s" is not iterable',
          obj, type(obj)))
end

local iter = function(obj, param, state)
    return wrap(rawiter(obj, param, state))
end
exports.iter = iter

local method1 = function(fun)
    return function(self, arg1)
        return fun(arg1, self.gen, self.param, self.state)
    end
end

local export1 = function(fun)
    return function(arg1, gen, param, state)
        return fun(arg1, rawiter(gen, param, state))
    end
end

----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
-- Implementation of drop_while
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------

-- Helper of 'wrap_with_elems'
local gen_with_elems_wrapper = function(gen_param_elems, state)
    local elems = gen_param_elems[3]
    if not elems then
        return gen_param_elems[1](gen_param_elems[2], state)
    else
        gen_param_elems[3] = nil
        return state, unpack(elems)
    end
end

-- Wrap an iterator that yields all values from 'elems' table on the first
-- iteration and then works in the same way as 'wrap(gen, param, state)'
local wrap_with_elems = function(elems, gen, param, state)
    local gen_param_elems = {gen, param, elems}
    return wrap(gen_with_elems_wrapper, gen_param_elems, state)
end

-- Checks if drop_while should continue skipping. If iterator is not exhausted,
-- elements returned by iterator are wrapped into a table and returned as the
-- third return value
local stateful_drop_while_check = function(fun, state_x, ...)
    if state_x == nil then
        return state_x, false
    end
    return state_x, fun(...), {...}
end

-- Implementation of method drop_while: original one copies iterator state to
-- iterate over the same element twice - it doesn't work with stateful iterators
local stateful_drop_while = function(fun, gen_x, param_x, state_x)
    assert(type(fun) == "function", "invalid first argument to drop_while")
    local cont = true
    local elems
    while state_x ~= nil and cont do
        state_x, cont, elems = stateful_drop_while_check(fun, gen_x(param_x, state_x))
    end
    if state_x == nil then
        return wrap(nil_gen, nil, nil)
    end
    return wrap_with_elems(elems, gen_x, param_x, state_x)
end

methods.drop_while = method1(stateful_drop_while)
exports.drop_while = export1(stateful_drop_while)

-- a special syntax sugar to export all functions to the global table
setmetatable(exports, {
    __call = function(t, override)
        for k, v in pairs(t) do
            if rawget(_G, k) ~= nil then
                local msg = 'function ' .. k .. ' already exists in global scope.'
                if override then
                    rawset(_G, k, v)
                    print('WARNING: ' .. msg .. ' Overwritten.')
                else
                    print('NOTICE: ' .. msg .. ' Skipped.')
                end
            else
                rawset(_G, k, v)
            end
        end
    end,
})

return exports
