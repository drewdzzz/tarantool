/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2023, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

struct event;

/**
 * Initializes module trigger.
 */
void
box_lua_trigger_init(struct lua_State *L);

/**
 * The function supports an alternative way (see below) to set a Lua trigger.
 * It is used for support of old trigger API.
 * Arguments start at position bottom. Possible argument formats:
 *
 * L[bottom + 2] - optional string
 * L[bottom + 1] - optional callable object
 * L[bottom] - optional callable object
 *
 * or
 *
 * L[bottom] - a table with keys 'func' and 'name'
 */
int
luaT_event_reset_trigger(struct lua_State *L, int bottom, struct event *event);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
