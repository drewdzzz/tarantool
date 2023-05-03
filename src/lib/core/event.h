/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2023, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#include "trigger.h"
#include <stdbool.h>

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

/**
 * List of triggers registered on event identified by name.
 */
struct event {
	/** List of triggers. */
	struct rlist triggers;
	/** Name of event. */
	char *name;
};

typedef bool
event_registry_foreach_f(struct event *event, void *arg);

/**
 * Initializes triggers registry.
 */
void
event_registry_init(void);

/**
 * Frees triggers registry.
 */
void
event_registry_free(void);

/**
 * Finds an event by its name. Name must be a zero-terminated string.
 * Creates new event and inserts it to registry if there is no event with such
 * name when flag create_if_not_exist is true.
 */
struct event *
event_registry_get(const char *name, bool create_if_not_exist);

/**
 * Removes event from register and destroys the event. No-op if event's
 * trigger list is not empty.
 */
void
event_registry_delete_if_unused(struct event *event);

/**
 * Invokes a callback for each registered event with no particular order.
 *
 * The callback is passed an event object and the given argument.
 * If it returns true, iteration continues. Otherwise, iteration breaks, and
 * the function returns false.
 */
bool
event_registry_foreach(event_registry_foreach_f cb, void *arg);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
