/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2023, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#include "func_adapter.h"
#include "small/rlist.h"
#include "trivia/util.h"
#include <stdbool.h>

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

/**
 * A node of the list, containing func_adapter.
 * Since event_triggers have completely different call interface,
 * they are not inherited from core triggers.
 */
struct event_trigger {
	/** A link in a list of all registered event triggers. */
	struct rlist link;
	/** Trigger function. */
	struct func_adapter *func;
	/** Backlink to event. Needed for reference count of event. */
	struct event *event;
	/** Unique name of the trigger. */
	char *name;
	/** Trigger reference counter. */
	uint32_t ref_count;
	/** This flag is set when the trigger is deleted. */
	bool is_deleted;
};

/**
 * Creates trigger from func_adapter and name. Created trigger owns the adapter
 * and will destroy it in its destructor. Passed name should be zero terminated
 * and will be copied.
 */
struct event_trigger *
event_trigger_new(struct func_adapter *func, const char *name);

/**
 * Destroys an event_trigger. Underlying func_adapter is destroyed,
 * event_trigger is deallocated.
 */
void
event_trigger_delete(struct event_trigger *trigger);

/**
 * Increments event_trigger reference counter.
 */
static inline void
event_trigger_ref(struct event_trigger *trigger)
{
	trigger->ref_count++;
}

/**
 * Decrements event_trigger reference counter. The event_trigger is destroyed
 * if the counter reaches zero.
 */
static inline void
event_trigger_unref(struct event_trigger *trigger)
{
	assert(trigger != NULL);
	assert(trigger->ref_count > 0);
	trigger->ref_count--;
	assert(trigger->ref_count > 0 || trigger->is_deleted);
	if (trigger->ref_count == 0)
		event_trigger_delete(trigger);
}

/**
 * List of triggers registered on event identified by name.
 */
struct event {
	/** List of triggers. */
	struct rlist triggers;
	/** Name of event. */
	char *name;
	/** Reference count. */
	uint32_t ref_count;
};

/**
 * Destroys an event and removes it from event registry.
 * NB: The method is private and must not be called manually.
 */
void
event_delete(struct event *event);

/**
 * Increments event reference counter.
 */
static inline void
event_ref(struct event *event)
{
	event->ref_count++;
}

/**
 * Decrements event reference counter. The event is destroyed if the counter
 * reaches zero.
 */
static inline void
event_unref(struct event *event)
{
	assert(event->ref_count > 0);
	event->ref_count--;
	if (event->ref_count == 0) {
		assert(rlist_empty(&event->triggers));
		event_delete(event);
	}
}

/**
 * Checks if the event has no triggers.
 */
static inline bool
event_is_empty(struct event *event)
{
	if (rlist_empty(&event->triggers))
		return true;
	struct event_trigger *trigger;
	rlist_foreach_entry(trigger, &event->triggers, link) {
		if (!trigger->is_deleted)
			return false;
	}
	return true;
}

/**
 * Find a trigger by name. All the arguments must not be NULL.
 */
void
event_find_trigger(struct event *event, const char *name,
		   struct event_trigger **trigger);

/**
 * Resets event_trigger in an event.
 * Arguments event and name must not be NULL.
 * If new_trigger is NULL, the function removes a trigger by name from the
 * event. Otherwise, it replaces trigger by name or inserts it in the beginning
 * of the underlying list of event. If both name and new_trigger passed, name of
 * new_trigger must be the same as passed name.
 * If old_trigger is not NULL, removed or replaced trigger is returned via this
 * argument. The returned trigger is referenced, so the caller must unreference
 * it after use.
 */
void
event_reset_trigger(struct event *event, const char *name,
		    struct event_trigger *new_trigger,
		    struct event_trigger **old_trigger);

/**
 * If returns -1, diag must be set.
 */
typedef int
(*event_trigger_foreach_f)(struct event_trigger *trg, void *arg);

/**
 * Iterator over triggers from event. Never invalidates.
 */
struct event_iterator {
	/**
	 * Current element. Points to event->triggers in the beginning and
	 * becomes NULL when the iterator is exhausted.
	 */
	struct rlist *curr;
	/** Underlying event. */
	struct event *event;
};

/**
 * Initializes iterator.
 */
void
event_iterator_create(struct event_iterator *it, struct event *event);

/**
 * Returns the next event_trigger from the underlying list of triggers of event.
 */
struct event_trigger *
event_iterator_next(struct event_iterator *it);

/**
 * Deinitializes event_iterator. Does not free memory.
 */
void
event_iterator_destroy(struct event_iterator *it);

/**
 * Call a callback on every trigger in the event, in order they are placed
 * in the underlying list (see event_reset_trigger description for details).
 * The execution is stopped after the first returned error code (callback
 * returned non-zero value). If the callback returns an error, it must set diag.
 */
static inline int
event_foreach(struct event *event, event_trigger_foreach_f cb, void *arg)
{
	struct event_iterator it;
	event_iterator_create(&it, event);
	struct event_trigger *trigger = event_iterator_next(&it);
	int rc = 0;
	for (; trigger != NULL && rc == 0; trigger = event_iterator_next(&it))
		rc = cb(trigger, arg);
	event_iterator_destroy(&it);
	return rc;
}

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

typedef bool
event_registry_foreach_f(struct event *event, void *arg);

/**
 * Invokes a callback for each registered event with no particular order.
 *
 * The callback is passed an event object and the given argument.
 * If it returns true, iteration continues. Otherwise, iteration breaks, and
 * the function returns false.
 *
 * Empty events are guaranteed to be skipped.
 */
bool
event_registry_foreach(event_registry_foreach_f cb, void *arg);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
