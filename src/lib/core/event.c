/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2023, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "event.h"

#include "assoc.h"
#include "diag.h"
#include "trivia/util.h"

/** Registry of all triggers: name -> event. */
static struct mh_strnptr_t *event_registry;

/**
 * Allocates and initializes a new event.
 * Passed name will be copied, it must be zero terminated string.
 */
static struct event *
event_create(const char *name)
{
	assert(name != NULL);
	struct event *event = xmalloc(sizeof(*event));
	rlist_create(&event->triggers);
	event->name = xstrdup(name);
	event->ref_count = 0;
	event->push = NULL;
	event->pop = NULL;
	return event;
}

/**
 * Destroys an event. Underlying trigger list is destroyed.
 */
static void
event_destroy(struct event *event)
{
	assert(event != NULL);
	trigger_destroy(&event->triggers);
	free(event->name);
	free(event);
}

void
event_registry_init(void)
{
	event_registry = mh_strnptr_new();
}

void
event_registry_free(void)
{
	assert(event_registry != NULL);
	struct mh_strnptr_t *h = event_registry;
	if (h == NULL)
		return;
	mh_int_t i;
	mh_foreach(h, i) {
		struct event *event = mh_strnptr_node(h, i)->val;
		event_destroy(event);
	}
	mh_strnptr_delete(h);
	event_registry = NULL;
}

struct event *
event_registry_get(const char *name, bool create_if_not_exist)
{
	assert(event_registry != NULL);
	assert(name != NULL);
	struct mh_strnptr_t *h = event_registry;
	uint32_t name_len = strlen(name);
	mh_int_t i = mh_strnptr_find_str(h, name, name_len);
	if (i != mh_end(h))
		return mh_strnptr_node(h, i)->val;
	else if (!create_if_not_exist)
		return NULL;

	struct event *event = event_create(name);
	name = event->name;
	uint32_t name_hash = mh_strn_hash(name, name_len);
	struct mh_strnptr_node_t node = {name, name_len, name_hash, event};
	struct mh_strnptr_node_t prev;
	struct mh_strnptr_node_t *prev_ptr = &prev;
	mh_int_t pos = mh_strnptr_put(h, &node, &prev_ptr, NULL);
	(void)pos;
	assert(pos != mh_end(h));
	assert(prev_ptr == NULL);
	return event;
}

void
event_registry_delete_if_unused(struct event *event)
{
	assert(event_registry != NULL);
	assert(event != NULL);
	if (!rlist_empty(&event->triggers) || event->ref_count > 0)
		return;
	struct mh_strnptr_t *h = event_registry;
	const char *name = event->name;
	size_t name_len = strlen(name);
	mh_int_t i = mh_strnptr_find_str(h, name, name_len);
	assert(i != mh_end(h));
	assert(mh_strnptr_node(h, i)->val == event);
	mh_strnptr_del(h, i, 0);
	event_destroy(event);
}

bool
event_registry_foreach(event_registry_foreach_f cb, void *arg)
{
	struct mh_strnptr_t *h = event_registry;
	mh_int_t i;
	mh_foreach(h, i) {
		struct mh_strnptr_node_t *node = mh_strnptr_node(h, i);
		struct event *event = node->val;
		if (!cb(event, arg))
			return false;
	}
	return true;
}
