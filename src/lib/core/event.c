/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2023, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "event.h"

#include <assert.h>

#include "assoc.h"
#include "diag.h"
#include "trivia/util.h"

/** Registry of all triggers: name -> event. */
static struct mh_strnptr_t *event_registry;

/**
 * Allocates, initializes a new event and inserts it to event registry.
 * Passed name will be copied.
 */
static struct event *
event_new(const char *name, size_t name_len)
{
	assert(name != NULL);
	assert(name_len > 0);
	struct event *event = xmalloc(sizeof(*event) + name_len + 1);
	rlist_create(&event->triggers);
	event->ref_count = 0;
	event->name = (char *)(event + 1);
	strlcpy(event->name, name, name_len + 1);

	struct mh_strnptr_t *h = event_registry;
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

/**
 * Destroys an event and removes it from event registry.
 * Underlying trigger list is destroyed.
 */
void
event_delete(struct event *event)
{
	assert(event != NULL);

	struct mh_strnptr_t *h = event_registry;
	const char *name = event->name;
	size_t name_len = strlen(name);
	mh_int_t i = mh_strnptr_find_str(h, name, name_len);
	assert(i != mh_end(h));
	assert(mh_strnptr_node(h, i)->val == event);
	mh_strnptr_del(h, i, 0);

	/*
	 * If the only thing that holds the event is its trigger list,
	 * the reference counter will reach zero when the list will be cleared
	 * and the destructor will be called again. Let's reference the event
	 * in order to prevent such situation.
	 */
	event_ref(event);
	struct event_trigger *trigger, *tmp;
	rlist_foreach_entry_safe(trigger, &event->triggers, link, tmp) {
		event_trigger_delete(trigger);
	}
	TRASH(event);
	free(event);
}

void
event_find_trigger(struct event *event, const char *name,
		   struct event_trigger **trigger)
{
	*trigger = NULL;
	struct event_trigger *curr = NULL;
	rlist_foreach_entry(curr, &event->triggers, link) {
		if (!curr->is_deleted && strcmp(curr->name, name) == 0) {
			*trigger = curr;
			break;
		}
	}
}

void
event_reset_trigger(struct event *event, const char *name,
		    struct event_trigger *new_trigger,
		    struct event_trigger **old_trigger)
{
	assert(event != NULL);
	assert(name != NULL);
	assert(new_trigger == NULL || strcmp(new_trigger->name, name) == 0);
	if (old_trigger != NULL)
		*old_trigger = NULL;
	struct event_trigger *trigger;
	struct rlist *head = &event->triggers;
	event_find_trigger(event, name, &trigger);
	if (trigger != NULL) {
		if (old_trigger != NULL) {
			event_trigger_ref(trigger);
			*old_trigger = trigger;
		}
		/*
		 * Set the previous trigger as head - this ensures that
		 * trigger with this name will be called once.
		 */
		head = trigger->link.prev;
		/* Delete the old trigger - set marker and unref. */
		trigger->is_deleted = true;
		event_trigger_unref(trigger);
	}
	if (new_trigger == NULL)
		return;
	event_trigger_ref(new_trigger);
	/* A new trigger in event must ref it. */
	new_trigger->event = event;
	event_ref(event);
	rlist_add_entry(head, new_trigger, link);
}

struct event_trigger *
event_trigger_new(struct func_adapter *func, const char *name)
{
	assert(name != NULL);
	size_t name_len = strlen(name);
	struct event_trigger *trg = xmalloc(sizeof(*trg) + name_len + 1);
	trg->func = func;
	trg->name = (char *)(trg + 1);
	strlcpy(trg->name, name, name_len + 1);
	trg->ref_count = 0;
	trg->is_deleted = false;
	trg->event = NULL;
	rlist_create(&trg->link);
	return trg;
}

void
event_trigger_delete(struct event_trigger *trigger)
{
	assert(trigger->func != NULL);
	rlist_del(&trigger->link);
	if (trigger->event != NULL)
		event_unref(trigger->event);
	func_adapter_destroy(trigger->func);
	TRASH(trigger);
	free(trigger);
}

void
event_iterator_create(struct event_iterator *it, struct event *event)
{
	event_ref(event);
	it->event = event;
	it->curr = &event->triggers;
}

struct event_trigger *
event_iterator_next(struct event_iterator *it)
{
	/* Iterator is exhausted - return. */
	if (it->curr == NULL)
		return NULL;
	struct event_trigger *trg;
	struct rlist *old = it->curr;
	/* We need to skip all the deleted triggers. */
	do {
		it->curr = rlist_next(it->curr);
		trg = rlist_entry(it->curr, struct event_trigger, link);
		/* We have traversed the whole list. */
		if (it->curr == &it->event->triggers) {
			trg = NULL;
			goto release;
		}
	} while (trg->is_deleted);
	assert(trg != NULL);
	event_trigger_ref(trg);
release:
	if (old != NULL && old != &it->event->triggers) {
		struct event_trigger *old_trg =
			rlist_entry(old, struct event_trigger, link);
		assert(old_trg != trg);
		event_trigger_unref(old_trg);
	}
	return trg;
}

void
event_iterator_destroy(struct event_iterator *it)
{
	if (it->curr != NULL && it->curr != &it->event->triggers) {
		struct event_trigger *curr_trg =
			rlist_entry(it->curr, struct event_trigger, link);
		event_trigger_unref(curr_trg);
	}
	event_unref(it->event);
	it->event = NULL;
	it->curr = NULL;
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
		event_delete(event);
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

	struct event *event = event_new(name, name_len);
	return event;
}

bool
event_registry_foreach(event_registry_foreach_f cb, void *arg)
{
	struct mh_strnptr_t *h = event_registry;
	mh_int_t i;
	mh_foreach(h, i) {
		struct mh_strnptr_node_t *node = mh_strnptr_node(h, i);
		struct event *event = node->val;
		if (event_is_empty(event))
			continue;
		if (!cb(event, arg))
			return false;
	}
	return true;
}
