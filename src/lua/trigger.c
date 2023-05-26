/*
 * Copyright 2010-2023, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "lua/trigger.h"
#include "lua/utils.h"
#include <diag.h>
#include <fiber.h>
#include "core/event.h"

/**
 * Lua trigger for event registry.
 */
struct lua_trigger {
	/** Base trigger. */
	struct trigger base;
	/** A reference to Lua trigger object. */
	int ref;
	/** Name of a trigger. */
	const char *name;
	trigger_prepare_state_f *push;
	trigger_extract_state_f *pop;
};

/**
 * Virtual destructor for lua_trigger.
 */
static void
lua_trigger_destroy(struct trigger *ptr)
{
	if (tarantool_L) {
		struct lua_trigger *trigger = (struct lua_trigger *)ptr;
		luaL_unref(tarantool_L, LUA_REGISTRYINDEX, trigger->ref);
		TRASH(trigger);
	}
	TRASH(ptr);
	free(ptr);
}

static int
lua_trigger_run(struct trigger *ptr, void *event)
{
	struct lua_trigger *trigger = (struct lua_trigger *)ptr;
	int rc = -1;
	/*
	 * Create a new coro and reference it. Remove it
	 * from tarantool_L stack, which is a) scarce
	 * b) can be used by other triggers while this
	 * trigger yields, so when it's time to clean
	 * up the coro, we wouldn't know which stack position
	 * it is on.
	 */
	lua_State *L;
	int coro_ref = LUA_NOREF;
	if (fiber()->storage.lua.stack == NULL) {
		L = luaT_newthread(tarantool_L);
		if (L == NULL)
			goto out;
		coro_ref = luaL_ref(tarantool_L, LUA_REGISTRYINDEX);
	} else {
		L = fiber()->storage.lua.stack;
		coro_ref = LUA_REFNIL;
	}
	int top_svp = lua_gettop(L);
	lua_rawgeti(L, LUA_REGISTRYINDEX, trigger->ref);
	int top = lua_gettop(L);
	struct lua_trigger_arg arg;
	arg.L = L;
	arg.nret = 0;
	if (trigger->push != NULL && *(trigger->push) != NULL) {
		rc = (*(trigger->push))(&arg, event);
		if (rc != 0)
			goto out;
	}
	int nargs = lua_gettop(L) - top;
	/*
	 * There are two cases why we can't access `trigger` after
	 * calling it's function:
	 * - trigger can be unregistered and destroyed
	 *   directly in its function.
	 * - trigger function may yield and someone destroy trigger
	 *   at this moment.
	 * So we keep 'trigger->pop_event' in local variable for
	 * further use.
	 */
	trigger_extract_state_f pop = NULL;
	if (trigger->pop != NULL && *(trigger->pop) != NULL)
		pop = *(trigger->pop);
	trigger = NULL;
	if (luaT_call(L, nargs, LUA_MULTRET))
		goto out;
	int nret = lua_gettop(L) - top_svp;
	arg.nret = nret;
	if (pop != NULL &&
	    pop(&arg, event) != 0) {
		lua_settop(L, top);
		goto out;
	}
	/*
	 * Clear the stack after pop_event saves all
	 * the needed return values.
	 */
	lua_settop(L, top_svp);
	rc = 0;
out:
	luaL_unref(tarantool_L, LUA_REGISTRYINDEX, coro_ref);
	return rc;
}

/**
 * Virtual method run for lua_trigger.
 * Event must be a pointer to int which is a reference to table with passed
 * arguments in Lua registry.
 * All the returned values are on the top of the stack.
 */
static int
lua_trigger_run_raw(struct trigger *ptr, struct lua_State *L, int nargs)
{
	struct lua_trigger *trigger = (struct lua_trigger *)ptr;
	int top_svp = lua_gettop(L);
	lua_rawgeti(L, LUA_REGISTRYINDEX, trigger->ref);
	int top = lua_gettop(L);
	assert(top >= nargs);
	for (int i = nargs; i > 0; --i)
		lua_pushvalue(L, top - i);
	if (luaT_call(L, nargs, LUA_MULTRET) != 0)
		return -1;
	lua_settop(L, top_svp);
	return 0;
}

/**
 * Finds lua_trigger in trigger list by name. Returns NULL, if there is
 * no trigger with such name.
 */
static struct lua_trigger *
lua_trigger_find(struct rlist *list, const char *name)
{
	struct lua_trigger *trigger;
	/** Find the old trigger, if any. */
	rlist_foreach_entry(trigger, list, base.link) {
		assert(trigger->base.run == lua_trigger_run);
		if (strcmp(trigger->name, name) == 0)
			return trigger;
	}
	return NULL;
}

/**
 * Inserts Lua object as trigger in list with trigger_name.
 * If passed object is not callable, an error is thrown.
 * New trigger is returned.
 */
int
lua_trigger_set(struct lua_State *L, int idx, const char *trigger_name,
		struct rlist *list, trigger_prepare_state_f *prepare,
		trigger_extract_state_f *extract)
{
	if (!luaL_iscallable(L, idx))
		luaL_error(L, "event trigger set: incorrect arguments");

	struct lua_trigger *trg = lua_trigger_find(list, trigger_name);

	if (trg != NULL) {
		luaL_unref(L, LUA_REGISTRYINDEX, trg->ref);
	} else {
		trg = xmalloc(sizeof(*trg));
		trigger_create(&trg->base, lua_trigger_run, NULL,
			       lua_trigger_destroy);
		trg->ref = LUA_NOREF;
		trigger_add(list, &trg->base);
		trg->name = xstrdup(trigger_name);
		trg->push = prepare;
		trg->pop = extract;
	}
	trg->ref = luaL_ref(L, LUA_REGISTRYINDEX);
	lua_rawgeti(L, LUA_REGISTRYINDEX, trg->ref);
	return 1;
}

/**
 * Deletes a trigger by name from the event. Deleted trigger is returned.
 */
int
lua_trigger_del(lua_State *L, const char *trigger_name, struct rlist *list)
{
	struct lua_trigger *trg =
		lua_trigger_find(list, trigger_name);
	if (trg == NULL)
		return 0;

	trigger_clear(&trg->base);
	lua_rawgeti(L, LUA_REGISTRYINDEX, trg->ref);
	luaL_unref(L, LUA_REGISTRYINDEX, trg->ref);
	return 1;
}

/**
 * Sets a trigger with passed name to the passed event.
 * The first argument is event name, the second one is trigger name, the third
 * one is trigger function.
 */
static int
luaT_trigger_set(struct lua_State *L)
{
	const char *event_name = luaL_checkstring(L, 1);
	struct event *event = event_registry_get(event_name, true);
	assert(event != NULL);
	const char *trigger_name = luaL_checkstring(L, 2);
	return lua_trigger_set(L, 3, trigger_name, &event->triggers,
			       &event->push, &event->pop);
}

/**
 * Deletes a trigger with passed name from passed event.
 * The first argument is event name, the second one is trigger name.
 */
static int
luaT_trigger_del(struct lua_State *L)
{
	const char *event_name = luaL_checkstring(L, 1);
	struct event *event = event_registry_get(event_name, false);
	if (event == NULL)
		return 0;
	const char *trigger_name = luaL_checkstring(L, 2);
	int ret_count = lua_trigger_del(L, trigger_name, &event->triggers);
	if (ret_count != 0)
		event_registry_delete_if_unused(event);
	return ret_count;
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
	struct rlist trigger_list;
	int rc = trigger_stable_list_create(&trigger_list, &event->triggers);
	while (!rlist_empty(&trigger_list) && rc == 0) {
		struct trigger *trg = trigger_stable_list_take(&trigger_list);
		rc = lua_trigger_run_raw(trg, L, lua_gettop(L) - 1);
	}
	if (rc != 0)
		return luaT_error(L);
	return 0;
}

/**
 * Pushes an array of arrays [trigger_name, trigger_handler] onto Lua stack.
 */
static void
luaT_trigger_info_push_event(struct lua_State *L, struct event *event)
{
	assert(event != NULL);
	size_t idx = 0;
	struct trigger *trigger;
	lua_createtable(L, 0, 0);
	rlist_foreach_entry(trigger, &event->triggers, link) {
		assert(trigger->run == lua_trigger_run);
		idx++;
		struct lua_trigger *lua_trigger = (struct lua_trigger *)trigger;
		lua_createtable(L, 2, 0);
		lua_pushstring(L, lua_trigger->name);
		lua_rawseti(L, -2, 1);
		lua_rawgeti(L, LUA_REGISTRYINDEX, lua_trigger->ref);
		lua_rawseti(L, -2, 2);
		lua_rawseti(L, -2, idx);
	}
}

/**
 * Pushes an event onto Lua stack, passed as arg, and sets it as a value for
 * event->name key of a table on the top of passed Lua stack.
 */
static bool
luaT_trigger_info_foreach_cb(struct event *event, void *arg)
{
	lua_State *L = (lua_State *)arg;
	luaT_trigger_info_push_event(L, event);
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
		bool ok =
			event_registry_foreach(luaT_trigger_info_foreach_cb, L);
		assert(ok);
		(void)ok;
	} else {
		const char *event_name = luaL_checkstring(L, 1);
		struct event *event = event_registry_get(event_name, false);
		if (event == NULL) {
			lua_createtable(L, 0, 0);
			return 1;
		}
		lua_createtable(L, 0, 1);
		luaT_trigger_info_push_event(L, event);
		lua_setfield(L, -2, event->name);
	}
	return 1;
}

const char *trigger_iterator_typename = "trigger.iterator";

/**
 * Iterator over Lua triggers.
 */
struct lua_trigger_iterator {
	/** A stable list of triggers from the event. */
	struct rlist list;
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
	struct trigger *trigger = trigger_stable_list_take(&it->list);
	if (trigger == NULL)
		return 0;
	assert(trigger->run == lua_trigger_run);
	struct lua_trigger *lua_trigger = (struct lua_trigger *)trigger;
	assert(lua_trigger->name != NULL);
	lua_pushstring(L, lua_trigger->name);
	lua_rawgeti(L, LUA_REGISTRYINDEX, lua_trigger->ref);
	return 2;
}

/**
 * Destroys an iterator.
 */
static int
luaT_trigger_iterator_gc(struct lua_State *L)
{
	struct lua_trigger_iterator *it = lua_check_trigger_iterator(L, 1);
	trigger_stable_list_clear(&it->list);
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
	rlist_create(&it->list);
	luaL_getmetatable(L, trigger_iterator_typename);
	lua_setmetatable(L, -2);
	if (trigger_stable_list_create(&it->list, &event->triggers) != 0)
		return luaT_error(L);
	return 2;
}

void
tarantool_lua_trigger_init(struct lua_State *L)
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
 * Support of old triggers - lbox_trigger contains push_event and pop_event
 * methods - they provide the same behavior as a base trigger, but one has to
 * know in advance what arguments will be passed to a trigger.
 */
struct lbox_trigger {
	struct trigger base;
	/** A reference to Lua trigger function. */
	int ref;
	/*
	 * A pointer to a C function which pushes the
	 * event data to Lua stack as arguments of the
	 * Lua trigger.
	 */
	lbox_push_event_f push_event;
	/**
	 * A pointer to a C function which is called
	 * upon successful execution of the trigger
	 * callback.
	 */
	lbox_pop_event_f pop_event;
};

static void
lbox_trigger_destroy(struct trigger *ptr)
{
	if (tarantool_L) {
		struct lbox_trigger *trigger = (struct lbox_trigger *) ptr;
		luaL_unref(tarantool_L, LUA_REGISTRYINDEX, trigger->ref);
		TRASH(trigger);
	}
	TRASH(ptr);
	free(ptr);
}

static int
lbox_trigger_run(struct trigger *ptr, void *event)
{
	struct lbox_trigger *trigger = (struct lbox_trigger *) ptr;
	int rc = -1;
	/*
	 * Create a new coro and reference it. Remove it
	 * from tarantool_L stack, which is a) scarce
	 * b) can be used by other triggers while this
	 * trigger yields, so when it's time to clean
	 * up the coro, we wouldn't know which stack position
	 * it is on.
	 */
	lua_State *L;
	int coro_ref = LUA_NOREF;
	if (fiber()->storage.lua.stack == NULL) {
		L = luaT_newthread(tarantool_L);
		if (L == NULL)
			goto out;
		coro_ref = luaL_ref(tarantool_L, LUA_REGISTRYINDEX);
	} else {
		L = fiber()->storage.lua.stack;
		coro_ref = LUA_REFNIL;
	}
	int top = lua_gettop(L);
	lua_rawgeti(L, LUA_REGISTRYINDEX, trigger->ref);
	int nargs = 0;
	if (trigger->push_event != NULL) {
		nargs = trigger->push_event(L, event);
		if (nargs < 0)
			goto out;
	}
	/*
	 * There are two cases why we can't access `trigger` after
	 * calling it's function:
	 * - trigger can be unregistered and destroyed
	 *   directly in its function.
	 * - trigger function may yield and someone destroy trigger
	 *   at this moment.
	 * So we keep 'trigger->pop_event' in local variable for
	 * further use.
	 */
	lbox_pop_event_f pop_event = trigger->pop_event;
	trigger = NULL;
	if (luaT_call(L, nargs, LUA_MULTRET))
		goto out;
	int nret = lua_gettop(L) - top;
	if (pop_event != NULL &&
	    pop_event(L, nret, event) != 0) {
		lua_settop(L, top);
		goto out;
	}
	/*
	 * Clear the stack after pop_event saves all
	 * the needed return values.
	 */
	lua_settop(L, top);
	rc = 0;
out:
	luaL_unref(tarantool_L, LUA_REGISTRYINDEX, coro_ref);
	return rc;
}

static struct lbox_trigger *
lbox_trigger_find(struct lua_State *L, int index, struct rlist *list)
{
	struct lbox_trigger *trigger;
	/** Find the old trigger, if any. */
	rlist_foreach_entry(trigger, list, base.link) {
		if (trigger->base.run == lbox_trigger_run) {
			lua_rawgeti(L, LUA_REGISTRYINDEX, trigger->ref);
			bool found = lua_equal(L, index, lua_gettop(L));
			lua_pop(L, 1);
			if (found)
				return trigger;
		}
	}
	return NULL;
}

static int
lbox_list_all_triggers(struct lua_State *L, struct rlist *list)
{
	struct lbox_trigger *trigger;
	int count = 1;
	lua_newtable(L);
	rlist_foreach_entry_reverse(trigger, list, base.link) {
		if (trigger->base.run == lbox_trigger_run) {
			lua_rawgeti(L, LUA_REGISTRYINDEX, trigger->ref);
			lua_rawseti(L, -2, count);
			count++;
		}
	}
	return 1;
}

static void
lbox_trigger_check_input(struct lua_State *L, int top)
{
	assert(lua_checkstack(L, top));
	/* Push optional arguments. */
	while (lua_gettop(L) < top)
		lua_pushnil(L);
	/*
	 * (nil, function) is OK, deletes the trigger
	 * (function, nil), is OK, adds the trigger
	 * (function, function) is OK, replaces the trigger
	 * no arguments is OK, lists all trigger
	 * anything else is error.
	 */
	if ((lua_isnil(L, top) && lua_isnil(L, top - 1)) ||
	    (lua_isfunction(L, top) && lua_isnil(L, top - 1)) ||
	    (lua_isnil(L, top) && lua_isfunction(L, top - 1)) ||
	    (lua_isfunction(L, top) && lua_isfunction(L, top - 1)))
		return;

	luaL_error(L, "trigger reset: incorrect arguments");
}

int
lbox_trigger_reset(struct lua_State *L, int top, struct rlist *list,
		   lbox_push_event_f push_event, lbox_pop_event_f pop_event)
{
	/**
	 * If the stack is empty, pushes nils for optional
	 * arguments
	 */
	lbox_trigger_check_input(L, top);
	/* If no args - return triggers table */
	if (lua_isnil(L, top) && lua_isnil(L, top - 1))
		return lbox_list_all_triggers(L, list);

	struct lbox_trigger *trg = lbox_trigger_find(L, top, list);

	if (trg) {
		luaL_unref(L, LUA_REGISTRYINDEX, trg->ref);

	} else if (lua_isfunction(L, top)) {
		return luaL_error(L, "trigger reset: Trigger is not found");
	}
	/*
	 * During update of a trigger, we must preserve its
	 * relative position in the list.
	 */
	if (lua_isfunction(L, top - 1)) {
		if (trg == NULL) {
			trg = (struct lbox_trigger *) malloc(sizeof(*trg));
			if (trg == NULL)
				luaL_error(L, "failed to allocate trigger");
			trigger_create(&trg->base, lbox_trigger_run, NULL,
				       lbox_trigger_destroy);
			trg->ref = LUA_NOREF;
			trg->push_event = push_event;
			trg->pop_event = pop_event;
			trigger_add(list, &trg->base);
		}
		/*
		 * Make the new trigger occupy the top
		 * slot of the Lua stack.
		 */
		lua_pop(L, 1);
		/* Reference. */
		trg->ref = luaL_ref(L, LUA_REGISTRYINDEX);
		lua_rawgeti(L, LUA_REGISTRYINDEX, trg->ref);
		return 1;

	} else if (trg) {
		trigger_clear(&trg->base);
		TRASH(trg);
		free(trg);
	}
	return 0;
}
