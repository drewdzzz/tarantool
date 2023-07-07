/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2023, Tarantool AUTHORS, please see AUTHORS file.
 */

#include "box/lua/func_adapter.h"
#include "core/event.h"
#include <diag.h>
#include <fiber.h>
#include "lua/utils.h"

/**
 * Sets a trigger with passed name to the passed event.
 * The first argument is event name, the second one is trigger name, the third
 * one is new trigger handler - it can be a Lua function or another callable
 * object. If there is an already registered trigger with such name in the
 * event, it is replaced with new trigger.
 * Returns new trigger handler (which was passed as the third argument).
 */
static int
luaT_trigger_set(struct lua_State *L)
{
	const char *event_name = luaL_checkstring(L, 1);
	const char *trigger_name = luaL_checkstring(L, 2);
	if (lua_gettop(L) < 3 || !luaL_iscallable(L, 3))
		luaL_typerror(L, 3, "callable");
	/*
	 * The following code is written in assumption no error will be thrown.
	 */
	struct event *event = event_registry_get(event_name, true);
	assert(event != NULL);
	struct func_adapter *func = func_adapter_lua_create(L, 3);
	struct event_trigger *trg = event_trigger_new(func, trigger_name);
	event_reset_trigger(event, trigger_name, trg, NULL);
	func_adapter_lua_get_func(func, L);
	return 1;
}

/**
 * Deletes a trigger with passed name from passed event.
 * The first argument is event name, the second one is trigger name.
 * Returns deleted trigger handler.
 */
static int
luaT_trigger_del(struct lua_State *L)
{
	const char *event_name = luaL_checkstring(L, 1);
	struct event *event = event_registry_get(event_name, false);
	if (event == NULL)
		return 0;
	const char *trigger_name = luaL_checkstring(L, 2);
	struct event_trigger *old;
	event_reset_trigger(event, trigger_name, NULL, &old);
	if (old != NULL) {
		func_adapter_lua_get_func(old->func, L);
		event_trigger_unref(old);
	} else {
		lua_pushnil(L);
	}

	return 1;
}

/**
 * An argument for callback that calls a trigger.
 */
struct call_trigger_arg {
	/** Lua stack containing arguments in direct order on its top. */
	struct lua_State *L;
	/** Number of arguments. */
	int narg;
};

/**
 * A callback for event_foreach function that calls a trigger and discards all
 * the returned valued.
 * Since the only supported language is Lua, all the handlers are Lua objects.
 */
static int
trigger_call_cb(struct event_trigger *trigger, void *arg)
{
	struct call_trigger_arg *call_arg = (struct call_trigger_arg *)arg;
	struct lua_State *L = call_arg->L;
	int narg = call_arg->narg;
	int top = lua_gettop(L);
	assert(top >= narg);
	func_adapter_lua_get_func(trigger->func, L);
	for (int i = top - narg + 1; i <= top; ++i)
		lua_pushvalue(L, i);
	int rc = luaT_call(L, narg, LUA_MULTRET);
	lua_settop(L, top);
	return rc;
}

/**
 * Calls all the triggers registered on passed event with variable number of
 * arguments. Execution is stopped by a first exception.
 * First argument must be a string - all the other arguments will be passed
 * to the triggers without any processing or copying.
 * Returns no values on success. If one of the triggers threw an error, it is
 * raised again.
 */
static int
luaT_trigger_call(struct lua_State *L)
{
	const char *event_name = luaL_checkstring(L, 1);
	struct event *event = event_registry_get(event_name, false);
	if (event == NULL)
		return 0;
	int narg = lua_gettop(L) - 1;
	struct call_trigger_arg arg = {.L = L, .narg = narg};
	if (event_foreach(event, trigger_call_cb, &arg) != 0)
		return luaT_error(L);
	return 0;
}

/**
 * An argument for callback that pushes triggers of one event onto Lua stack.
 */
struct push_trigger_arg {
	/** Lua stack. */
	lua_State *L;
	/** Current index in an array. */
	size_t idx;
};

/**
 * Appends an array [trigger_name, trigger_handler] to a pre-created array.
 */
static int
trigger_info_push_trigger(struct event_trigger *trigger, void *arg)
{
	struct push_trigger_arg *push_arg = (struct push_trigger_arg *)arg;
	lua_State *L = push_arg->L;
	push_arg->idx++;
	lua_createtable(L, 2, 0);
	lua_pushstring(L, trigger->name);
	lua_rawseti(L, -2, 1);
	func_adapter_lua_get_func(trigger->func, L);
	lua_rawseti(L, -2, 2);
	lua_rawseti(L, -2, push_arg->idx);
	return 0;
}

/**
 * Sets an array of arrays [trigger_name, trigger_handler] by event->name key
 * in a pre-created table. Never sets an empty array.
 */
static bool
trigger_info_push_event(struct event *event, void *arg)
{
	lua_State *L = (lua_State *)arg;
	struct push_trigger_arg push_arg = {.L = L, .idx = 0};
	lua_createtable(L, 0, 0);
	int rc = event_foreach(event, trigger_info_push_trigger, &push_arg);
	assert(rc == 0);
	(void)rc;
	assert(push_arg.idx > 0);
	lua_setfield(L, -2, event->name);
	return true;
}

/**
 * Pushes a key-value table, where the key is the event name and value is an
 * array of triggers, represented by two-element [trigger_name, trigger_handler]
 * arrays, registered on this event, in the order in which they will be called.
 * If an event name is passed, a table contains only one key which is passed
 * argument, if there is an event with such a name, or returned table is empty,
 * if the event does not exist.
 */
static int
luaT_trigger_info(struct lua_State *L)
{
	if (lua_gettop(L) == 0) {
		lua_createtable(L, 0, 0);
		bool ok = event_registry_foreach(trigger_info_push_event, L);
		assert(ok);
		(void)ok;
	} else {
		const char *event_name = luaL_checkstring(L, 1);
		struct event *event = event_registry_get(event_name, false);
		if (event == NULL || event_is_empty(event)) {
			lua_createtable(L, 0, 0);
			return 1;
		}
		lua_createtable(L, 0, 1);
		trigger_info_push_event(event, L);
	}
	return 1;
}

const char *trigger_iterator_typename = "trigger.iterator";

/**
 * Lua-iterator over event_trigger_list.
 */
struct lua_trigger_iterator {
	/** An iterator over event_trigger_list. */
	struct event_iterator it;
};

/**
 * Gets lua_trigger_iterator from Lua stack with type check.
 */
static inline struct lua_trigger_iterator *
lua_check_trigger_iterator(struct lua_State *L, int idx)
{
	return luaL_checkudata(L, idx, trigger_iterator_typename);
}

/**
 * Takes an iterator step.
 */
static int
luaT_trigger_iterator_next(struct lua_State *L)
{
	struct lua_trigger_iterator *it = lua_check_trigger_iterator(L, 1);
	struct event_trigger *trigger = event_iterator_next(&it->it);
	if (trigger == NULL)
		return 0;
	lua_pushstring(L, trigger->name);
	func_adapter_lua_get_func(trigger->func, L);
	return 2;
}

/**
 * Destroys an iterator.
 */
static int
luaT_trigger_iterator_gc(struct lua_State *L)
{
	struct lua_trigger_iterator *it = lua_check_trigger_iterator(L, 1);
	event_iterator_destroy(&it->it);
	TRASH(it);
	return 0;
}

/**
 * Creates iterator over triggers of event with passed name.
 * The iterator yields a pair [trigger_name, trigger_handler].
 * Return next method of iterator and iterator itself.
 */
static int
luaT_trigger_pairs(struct lua_State *L)
{
	const char *event_name = luaL_checkstring(L, 1);
	struct event *event = event_registry_get(event_name, false);
	if (event == NULL)
		return 0;
	lua_pushcfunction(L, luaT_trigger_iterator_next);
	struct lua_trigger_iterator *it = lua_newuserdata(L, sizeof(*it));
	event_iterator_create(&it->it, event);
	luaL_getmetatable(L, trigger_iterator_typename);
	lua_setmetatable(L, -2);
	return 2;
}

void
box_lua_trigger_init(struct lua_State *L)
{
	const struct luaL_Reg module_funcs[] = {
		{"set", luaT_trigger_set},
		{"del", luaT_trigger_del},
		{"call", luaT_trigger_call},
		{"info", luaT_trigger_info},
		{"pairs", luaT_trigger_pairs},
		{NULL, NULL}
	};
	luaT_newmodule(L, "trigger", module_funcs);
	lua_pop(L, 1);
	const struct luaL_Reg trigger_iterator_methods[] = {
		{"__gc", luaT_trigger_iterator_gc},
		{"next", luaT_trigger_iterator_next},
		{ NULL, NULL }
	};
	luaL_register_type(L, trigger_iterator_typename,
			   trigger_iterator_methods);
}

/**
 * Old API compatibility.
 */

/**
 * Appends a trigger handler to a pre-created array.
 */
static int
luaT_event_push_triggers_old(struct event_trigger *trigger, void *arg)
{
	assert(trigger != NULL);
	assert(trigger->func != NULL);
	struct push_trigger_arg *push_arg = (struct push_trigger_arg *)arg;
	lua_State *L = push_arg->L;
	push_arg->idx++;
	func_adapter_lua_get_func(trigger->func, L);
	lua_rawseti(L, -2, push_arg->idx);
	return 0;
}

/**
 * Checks arguments for trigger setter.
 * Throws an error if the format is not suitable.
 */
static void
luaT_event_reset_trigger_check_input(struct lua_State *L, int bottom)
{
	assert(lua_checkstack(L, bottom + 2));
	/* Push optional arguments. */
	while (lua_gettop(L) < bottom + 2)
		lua_pushnil(L);
	/*
	 * (nil, function) is OK, deletes the trigger
	 * (function, nil), is OK, adds the trigger
	 * (function, function) is OK, replaces the trigger
	 * no arguments is OK, lists all trigger
	 * anything else is error.
	 */
	bool ok = true;
	/* Name must be a string if it is passed. */
	ok = ok && (lua_isnil(L, bottom + 2) || lua_isstring(L, bottom + 2));
	ok = ok && (lua_isnil(L, bottom + 1) || luaL_iscallable(L, bottom + 1));
	ok = ok && (lua_isnil(L, bottom) || luaL_iscallable(L, bottom));
	if (ok)
		return;
	luaL_error(L, "trigger reset: incorrect arguments");
}

/**
 * Set trigger by name. Returns passed handler.
 */
static int
luaT_event_set_trigger_by_name(struct lua_State *L, struct event *event,
			       int name_idx, int func_idx)
{
	assert(luaL_iscallable(L, func_idx));
	assert(lua_type(L, name_idx) == LUA_TSTRING);

	struct event_trigger *new_trigger = NULL;
	const char *trigger_name = lua_tostring(L, name_idx);
	struct func_adapter *func = func_adapter_lua_create(L, func_idx);
	new_trigger = event_trigger_new(func, trigger_name);
	lua_pushvalue(L, func_idx);
	event_reset_trigger(event, trigger_name, new_trigger, NULL);
	return 1;
}

/**
 * Delete trigger by name. Always returns nothing.
 */
static int
luaT_event_del_trigger_by_name(struct lua_State *L, struct event *event,
			       int name_idx)
{
	assert(lua_type(L, name_idx) == LUA_TSTRING);

	const char *trigger_name = lua_tostring(L, name_idx);
	event_reset_trigger(event, trigger_name, NULL, NULL);
	return 0;
}

/**
 * Set or delete trigger depending on passed arguments.
 */
static int
luaT_event_reset_trigger_by_name(struct lua_State *L, struct event *event,
				 int name_idx, int func_idx)
{
	if (lua_type(L, name_idx) != LUA_TSTRING)
		luaL_error(L, "trigger reset: incorrect arguments");
	if (luaL_iscallable(L, func_idx))
		return luaT_event_set_trigger_by_name(L, event, name_idx,
						      func_idx);
	else if (lua_isnil(L, func_idx) || luaL_isnull(L, func_idx))
		return luaT_event_del_trigger_by_name(L, event, name_idx);
	else
		luaL_error(L, "trigger reset: incorrect arguments");
	unreachable();
}

int
luaT_event_reset_trigger(struct lua_State *L, int bottom, struct event *event)
{
	assert(L != NULL);
	assert(event != NULL);
	assert(bottom >= 1);
	/* New way with key-value arguments. */
	if (lua_gettop(L) == bottom && lua_istable(L, -1) &&
	    !luaL_iscallable(L, -1)) {
		lua_getfield(L, bottom, "name");
		lua_getfield(L, bottom, "func");
		return luaT_event_reset_trigger_by_name(L, event, -2, -1);
	}
	/* Old way with name support. */
	luaT_event_reset_trigger_check_input(L, bottom);
	const int top = bottom + 2;
	if (!lua_isnil(L, top) && !luaL_isnull(L, top)) {
		if (lua_type(L, top) != LUA_TSTRING)
			luaL_error(L, "trigger reset: incorrect arguments");
		return luaT_event_reset_trigger_by_name(L, event, top, bottom);
	}
	/*
	 * Name is not passed - old API support.
	 * 1. If triggers are not passed, return table of triggers.
	 * 2. If new_trigger is passed and old_trigger is not - set
	 * new_trigger using string representation of function as name.
	 * 3. If old_trigger is passed and new_trigger is not - delete
	 * trigger with by function string representation as a name.
	 * 4. If both triggers are provided - replace old_trigger with
	 * new one and change its name.
	 */
	/* Reference the event to safely perform several operation. */
	event_ref(event);
	int ret_count = 0;
	if (lua_isnil(L, bottom) && lua_isnil(L, bottom + 1)) {
		struct push_trigger_arg arg = {.L = L, .idx = 0};
		lua_createtable(L, 0, 0);
		int rc = event_foreach(event, luaT_event_push_triggers_old,
				       &arg);
		assert(rc == 0);
		(void)rc;
		ret_count = 1;
	} else if (!lua_isnil(L, bottom) && lua_isnil(L, bottom + 1)) {
		const char *name = luaT_tolstring(L, bottom, NULL);
		assert(name != NULL);
		(void)name;
		ret_count = luaT_event_set_trigger_by_name(L, event, -1,
							   bottom);
	} else if (lua_isnil(L, bottom) && !lua_isnil(L, bottom + 1)) {
		const char *name = luaT_tolstring(L, bottom + 1, NULL);
		assert(name != NULL);
		(void)name;
		ret_count = luaT_event_del_trigger_by_name(L, event, -1);
	} else {
		assert(!lua_isnil(L, bottom) && !lua_isnil(L, bottom + 1));
		const char *new_name = luaT_tolstring(L, bottom, NULL);
		const char *old_name = luaT_tolstring(L, bottom + 1, NULL);
		assert(new_name != NULL);
		assert(old_name != NULL);
		bool is_replace = strcmp(new_name, old_name) == 0;
		struct event_trigger *trg;
		event_find_trigger(event, old_name, &trg);
		if (trg == NULL) {
			event_unref(event);
			luaL_error(L, "trigger reset: Trigger is not found");
		}
		/*
		 * Delete trigger with new name to avoid duplication.
		 */
		if (!is_replace)
			event_reset_trigger(event, new_name, NULL, NULL);
		/* TODO: replace in a more convenient way. */
		trg->name = xstrdup(new_name);
		struct func_adapter *func =
			func_adapter_lua_create(L, top - 2);
		struct event_trigger *new_trg =
			event_trigger_new(func, new_name);
		struct event_trigger *old_trg;
		event_reset_trigger(event, new_name, new_trg, &old_trg);
		assert(old_trg == trg);
		free(trg->name);
		event_trigger_unref(old_trg);
		lua_pushvalue(L, top - 2);
		ret_count = 1;
	}
	event_unref(event);
	return ret_count;
}
