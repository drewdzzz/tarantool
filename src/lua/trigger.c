/*
 * Copyright 2010-2015, Tarantool AUTHORS, please see AUTHORS file.
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

struct lbox_trigger
{
	struct trigger base;
	/** An optional name. */
	char *name;
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
	struct lbox_trigger *trigger = (struct lbox_trigger *) ptr;
	if (trigger->name != NULL)
		free(trigger->name);
	if (tarantool_L) {
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
lbox_trigger_find(struct lua_State *L, int trg_idx, int name_idx,
		  struct rlist *list)
{
	struct lbox_trigger *trigger;
	const char *name = lua_tostring(L, name_idx);
	/** Find the old trigger, if any. */
	rlist_foreach_entry(trigger, list, base.link) {
		if (trigger->base.run == lbox_trigger_run) {
			bool found = false;
			if (name != NULL) {
				found = trigger->name != NULL &&
					strcmp(trigger->name, name) == 0;
			} else {
				lua_rawgeti(L, LUA_REGISTRYINDEX, trigger->ref);
				found = lua_equal(L, trg_idx, lua_gettop(L));
			}
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
	rlist_foreach_entry(trigger, list, base.link) {
		if (trigger->base.run == lbox_trigger_run) {
			lua_rawgeti(L, LUA_REGISTRYINDEX, trigger->ref);
			lua_rawseti(L, -2, count);
			count++;
		}
	}
	return 1;
}

static void
lbox_trigger_check_input(struct lua_State *L, int bottom)
{
	const int top = bottom + 2;
	assert(lua_checkstack(L, top));
	/* Push optional arguments. */
	while (lua_gettop(L) < top)
		lua_pushnil(L);
	/*
	 * (nil, callable, nil) is OK, deletes the trigger by object
	 * (nil, callable, string) is OK, deletes the trigger by name
	 * (callable, nil, nil), is OK, adds the trigger without name
	 * (callable, nil, string), is OK, adds the trigger with name
	 * (callable, callable, nil) is OK, replaces the trigger
	 * (callable, callable, string) is OK, replaces the trigger by name,
	 * but the second argument is ignored.
	 * no arguments is OK, lists all trigger
	 * anything else is error.
	 */
	const int new_idx = bottom;
	const int old_idx = bottom + 1;
	const int name_idx = bottom + 2;
	bool name_is_ok = lua_isnil(L, name_idx) || luaL_isnull(L, name_idx) ||
		lua_type(L, name_idx) == LUA_TSTRING;
	bool new_is_ok = lua_isnil(L, new_idx) || luaL_isnull(L, new_idx) ||
		luaL_iscallable(L, new_idx);
	bool old_is_ok = lua_isnil(L, old_idx) || luaL_isnull(L, old_idx) ||
		luaL_iscallable(L, old_idx);
	if (name_is_ok && new_is_ok && old_is_ok)
		return;

	luaL_error(L, "trigger reset: incorrect arguments");
}

int
lbox_trigger_reset(struct lua_State *L, int bottom, struct rlist *list,
		   lbox_push_event_f push_event, lbox_pop_event_f pop_event)
{
	/**
	 * If the stack is empty, pushes nils for optional
	 * arguments
	 */
	lbox_trigger_check_input(L, bottom);
	const int new_trg_idx = bottom;
	const int old_trg_idx = bottom + 1;
	const int name_idx = bottom + 2;
	/* If no args - return triggers table */
	if ((lua_isnil(L, new_trg_idx) || luaL_isnull(L, new_trg_idx)) &&
	    (lua_isnil(L, old_trg_idx) || luaL_isnull(L, old_trg_idx)) &&
	    (lua_isnil(L, name_idx) || luaL_isnull(L, name_idx)))
		return lbox_list_all_triggers(L, list);

	struct lbox_trigger *trg = lbox_trigger_find(L, old_trg_idx, name_idx,
						     list);

	if (trg) {
		luaL_unref(L, LUA_REGISTRYINDEX, trg->ref);
	} else if (luaL_iscallable(L, old_trg_idx)) {
		return luaL_error(L, "trigger reset: Trigger is not found");
	}
	/*
	 * During update of a trigger, we must preserve its
	 * relative position in the list.
	 */
	if (luaL_iscallable(L, new_trg_idx)) {
		if (trg == NULL) {
			trg = (struct lbox_trigger *) malloc(sizeof(*trg));
			if (trg == NULL)
				luaL_error(L, "failed to allocate trigger");
			trigger_create(&trg->base, lbox_trigger_run, NULL,
				       lbox_trigger_destroy);
			trg->ref = LUA_NOREF;
			trg->name = NULL;
			if (!lua_isnil(L, name_idx) &&
			    !luaL_isnull(L, name_idx)) {
				size_t name_len = 0;
				const char *name = lua_tolstring(L, name_idx,
								 &name_len);
				trg->name = xstrndup(name, name_len);
			}
			trg->push_event = push_event;
			trg->pop_event = pop_event;
			trigger_add(list, &trg->base);
		}
		/*
		 * Make the new trigger occupy the top
		 * slot of the Lua stack.
		 */
		lua_pushvalue(L, new_trg_idx);
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
