#include <stdbool.h>
#include <stddef.h>
#include <string.h>

#include "diag.h"
#include "error.h"
#include "errcode.h"
#include "fiber.h"
#include "memory.h"
#include "trivia/util.h"
#include "event.h"
#include "tt_static.h"

#define UNIT_TAP_COMPATIBLE 1
#include "unit.h"

bool func_destroyed = false;

void
func_destroy(struct func_adapter *func)
{
	func_destroyed = true;
}

static void
test_basic(void)
{
	plan(4 * 9);

	struct func_adapter_vtab vtab = {.destroy = func_destroy};
	struct func_adapter func = {.vtab = &vtab};
	const char *trg_name = "my_triggers.trg[1]";

	const char *names[] = {
		"name",
		"name with spaces",
		"namespace.name",
		"NAMESPACE[123].name"
	};
	for (size_t i = 0; i < lengthof(names); ++i) {
		struct event_trigger *trigger = event_trigger_new(&func,
								  trg_name);
		const char *name = names[i];
		struct event *event = event_registry_get(name, false);
		is(event, NULL, "No such event");
		event = event_registry_get(name, true);
		isnt(event, NULL, "Event must be created");
		struct event *found_event = event_registry_get(name, false);
		is(found_event, event, "Created event must be found");
		struct event_trigger *old;
		event_reset_trigger(event, trg_name, trigger, &old);
		is(old, NULL, "There was no old trigger");
		found_event = event_registry_get(name, false);
		is(found_event, event, "Must not delete event with triggers");
		event_reset_trigger(event, trg_name, NULL, &old);
		is(old, trigger, "Old trigger must be returned");
		ok(!func_destroyed, "Func must not be destroyed yet")
		event_trigger_unref(old);

		ok(func_destroyed, "Func must be destroyed")
		found_event = event_registry_get(name, false);
		is(found_event, NULL, "Empty event must be deleted");
		func_destroyed = false;
	}
	check_plan();
}

struct test_event_registry_foreach_arg {
	int names_len;
	const char **names;
	int traversed;
};

static bool
test_event_registry_foreach_f(struct event *event, void *arg)
{
	struct test_event_registry_foreach_arg *data =
		(struct test_event_registry_foreach_arg *)arg;
	data->traversed++;
	bool name_found = false;
	for (int i = 0; i < data->names_len && !name_found; ++i) {
		name_found = strcmp(event->name, data->names[i]) == 0;
	}
	ok(name_found, "Traversed event must really exist");
	return true;
}

static void
test_event_registry_foreach(void)
{
	plan(6);
	const char *names[] = {
		"event",
		"my_events.event1",
		"my_events.event3",
		"my_events[15].event"
	};
	struct func_adapter_vtab vtab = {.destroy = func_destroy};
	struct func_adapter func = {.vtab = &vtab};
	const char *trg_name = "test.foreach";
	for (size_t i = 0; i < lengthof(names); ++i) {
		struct event *event = event_registry_get(names[i], true);
		const char *trg_name = tt_sprintf("%zu", i);
		struct event_trigger *trg =
			event_trigger_new(&func, trg_name);
		event_reset_trigger(event, trg_name, trg, NULL);
	}

	struct test_event_registry_foreach_arg arg = {
		.names_len = lengthof(names),
		.names = names,
		.traversed = 0,
	};

	event_registry_foreach(test_event_registry_foreach_f, &arg);
	is(arg.traversed, lengthof(names), "All the events must be traversed");

	for (size_t i = 0; i < lengthof(names); ++i) {
		struct event *event = event_registry_get(names[i], false);
		/* Reference event to keep them empty. */
		event_ref(event);
		const char *trg_name = tt_sprintf("%zu", i);
		event_reset_trigger(event, trg_name, NULL, NULL);
	}

	arg.traversed = 0;
	event_registry_foreach(test_event_registry_foreach_f, &arg);
	is(arg.traversed, 0, "All the events are empty - nothing to traverse");

	/* Unreference all event. */
	for (size_t i = 0; i < lengthof(names); ++i) {
		struct event *event = event_registry_get(names[i], false);
		fail_if(event == NULL);
		event_unref(event);
	}

	check_plan();
}

struct test_event_foreach_arg {
	int names_len;
	const char **names;
	int traversed;
	int breakpoint;
};

static int
test_event_foreach_f(struct event_trigger *trigger, void *arg)
{
	struct test_event_foreach_arg *data =
		(struct test_event_foreach_arg *)arg;
	int idx = data->traversed;
	fail_unless(idx < data->names_len);
	is(strcmp(data->names[idx], trigger->name), 0,
	   "All the triggers must be traversed in order");
	data->traversed++;
	if (data->traversed == data->breakpoint) {
		diag_set(SystemError, "some error");
		return -1;
	}
	return 0;
}

static void
test_event_foreach(void)
{
	plan(16);
	const char *event_name = "test_event";
	const char *trigger_names[] = {
		"0", "1", "2", "3", "4", "5", "6", "7"
	};
	struct func_adapter_vtab vtab = {.destroy = func_destroy};
	struct func_adapter func = {.vtab = &vtab};

	struct event *event = event_registry_get(event_name, true);
	for (int i = lengthof(trigger_names) - 1; i >= 0; --i) {
		struct event_trigger *trg = event_trigger_new(
			&func, trigger_names[i]);
		event_reset_trigger(event, trigger_names[i], trg, NULL);
	}

	struct test_event_foreach_arg arg = {
		.names_len = lengthof(trigger_names),
		.names = trigger_names,
		.traversed = 0,
		.breakpoint = lengthof(trigger_names) + 1,
	};
	int rc = event_foreach(event, test_event_foreach_f, &arg);
	is(rc, 0, "Traversal must be completed successfully");
	is(arg.traversed, lengthof(trigger_names),
	   "All the events must be traversed");

	arg.traversed = 0;
	arg.breakpoint = lengthof(trigger_names) / 2;
	rc = event_foreach(event, test_event_foreach_f, &arg);
	is(rc, -1, "Traversal must fail");
	is(arg.traversed, arg.breakpoint, "Traversal must fail at breakpoint");

	for (size_t i = 0; i < lengthof(trigger_names); ++i) {
		event_reset_trigger(event, trigger_names[i], NULL, NULL);
	}

	check_plan();
}

static void
test_event_iterator(void)
{
	plan(10);
	const char *event_name = "test_event";
	const char *trigger_names[] = {
		"0", "1", "2", "3", "4", "5", "6", "7"
	};
	struct func_adapter_vtab vtab = {.destroy = func_destroy};
	struct func_adapter func = {.vtab = &vtab};

	struct event *event = event_registry_get(event_name, true);
	for (int i = lengthof(trigger_names) - 1; i >= 0; --i) {
		struct event_trigger *trg = event_trigger_new(
			&func, trigger_names[i]);
		event_reset_trigger(event, trigger_names[i], trg, NULL);
	}

	struct event_iterator it;
	event_iterator_create(&it, event);
	size_t idx = 0;
	struct event_trigger *trg = event_iterator_next(&it);
	while (trg != NULL) {
		is(strcmp(trg->name, trigger_names[idx]), 0,
		   "Triggers must be traversed in reversed order");
		idx++;
		trg = event_iterator_next(&it);
	}
	is(idx, lengthof(trigger_names), "All the triggers must be traversed");
	is(trg, NULL, "Exhausted iterator must return NULL");

	for (size_t i = 0; i < lengthof(trigger_names); ++i) {
		event_reset_trigger(event, trigger_names[i], NULL, NULL);
	}

	check_plan();
}

/**
 * Stops at breakpoint and deletes triggers which are set in del mask.
 */
static void
test_event_iterator_stability_del_step(int breakpoint, const char *del_mask,
				       int trigger_num)
{
	fail_unless(breakpoint < trigger_num);
	size_t left_after_br = 0;
	for (int i = breakpoint + 1; i < trigger_num; ++i) {
		if (del_mask[i] == 0)
			left_after_br++;
	}
	plan(breakpoint + 1 + left_after_br + 1);

	const char *event_name = "test_event";
	struct func_adapter_vtab vtab = {.destroy = func_destroy};
	struct func_adapter func = {.vtab = &vtab};

	struct event *event = event_registry_get(event_name, true);
	for (int i = trigger_num - 1; i >= 0; --i) {
		const char *trg_name = tt_sprintf("%d", i);
		struct event_trigger *trg = event_trigger_new(
			&func, trg_name);
		event_reset_trigger(event, trg_name, trg, NULL);
	}

	struct event_iterator it;
	event_iterator_create(&it, event);
	struct event_trigger *trg = NULL;
	for (int i = 0; i <= breakpoint; i++) {
		const char *trg_name = tt_sprintf("%d", i);
		trg = event_iterator_next(&it);
		is(strcmp(trg->name, trg_name), 0,
		   "Triggers must be traversed in reversed order");
	}
	for (int i = 0; i < trigger_num; ++i) {
		if (del_mask[i] != 0) {
			const char *trg_name = tt_sprintf("%d", i);
			event_reset_trigger(event, trg_name, NULL, NULL);
		}
	}
	for (size_t i = 0; i < left_after_br; ++i) {
		trg = event_iterator_next(&it);
		isnt(trg, NULL, "Traversal must continue");
	}

	trg = event_iterator_next(&it);
	is(trg, NULL, "Iterator must be exhausted");

	for (int i = 0; i < trigger_num; ++i) {
		if (del_mask[i] != 0)
			continue;
		const char *trg_name = tt_sprintf("%d", i);
		event_reset_trigger(event, trg_name, NULL, NULL);
	}

	check_plan();
}

/**
 * Stops at breakpoint and replaces triggers which are set in replace mask.
 */
static void
test_event_iterator_stability_replace_step(int breakpoint,
					   const char *replace_mask,
					   int trigger_num)
{
	fail_unless(breakpoint < trigger_num);
	plan(breakpoint + 1 + 3 * (trigger_num - breakpoint - 1) + 1);

	const char *event_name = "test_event";
	struct func_adapter_vtab vtab = {.destroy = func_destroy};
	struct func_adapter func = {.vtab = &vtab};
	struct func_adapter new_func = {.vtab = &vtab};

	struct event *event = event_registry_get(event_name, true);
	for (int i = trigger_num - 1; i >= 0; --i) {
		const char *trg_name = tt_sprintf("%d", i);
		struct event_trigger *trg = event_trigger_new(
			&func, trg_name);
		event_reset_trigger(event, trg_name, trg, NULL);
	}

	struct event_iterator it;
	event_iterator_create(&it, event);
	struct event_trigger *trg = NULL;
	for (int i = 0; i <= breakpoint; i++) {
		const char *trg_name = tt_sprintf("%d", i);
		trg = event_iterator_next(&it);
		is(strcmp(trg->name, trg_name), 0,
		   "Triggers must be traversed in reversed order");
	}
	for (int i = 0; i < trigger_num; ++i) {
		if (replace_mask[i] != 0) {
			const char *trg_name = tt_sprintf("%d", i);
			struct event_trigger *new_trg = event_trigger_new(
				&new_func, trg_name);
			event_reset_trigger(event, trg_name, new_trg, NULL);
		}
	}
	for (int i = breakpoint + 1; i < trigger_num; ++i) {
		const char *trg_name = tt_sprintf("%d", i);
		trg = event_iterator_next(&it);
		isnt(trg, NULL, "Traversal must continue");
		is(strcmp(trg->name, trg_name), 0,
		   "Triggers must be traversed in reversed order");
		if (replace_mask[i] != 0) {
			is(trg->func, &new_func, "Trigger must be replaced");
		} else {
			is(trg->func, &func, "Trigger must be old");
		}
	}

	trg = event_iterator_next(&it);
	is(trg, NULL, "Iterator must be exhausted");

	for (int i = 0; i < trigger_num; ++i) {
		const char *trg_name = tt_sprintf("%d", i);
		event_reset_trigger(event, trg_name, NULL, NULL);
	}

	check_plan();
}

/**
 * Checks if iteration is stable in the cases of deletions and replaces.
 */
static void
test_event_iterator_stability(void)
{
	plan(6);
	char mask[8];
	const size_t trigger_num = lengthof(mask);
	memset(mask, 0, trigger_num);
	size_t br = trigger_num / 2;
	/**
	 * Delete current trigger.
	 */
	mask[br] = 1;
	test_event_iterator_stability_del_step(br, mask, trigger_num);
	test_event_iterator_stability_replace_step(br, mask, trigger_num);
	memset(mask, 0, trigger_num);
	/**
	 * Delete current, previous and next triggers.
	 */

	mask[br - 1] = 1;
	mask[br] = 1;
	mask[br + 1] = 1;
	test_event_iterator_stability_del_step(br, mask, trigger_num);
	test_event_iterator_stability_replace_step(br, mask, trigger_num);
	memset(mask, 0, trigger_num);
	/**
	 * Delete all the triggers in the middle of iteration.
	 */
	memset(mask, 1, trigger_num);
	test_event_iterator_stability_del_step(br, mask, trigger_num);
	test_event_iterator_stability_replace_step(br, mask, trigger_num);
	memset(mask, 0, trigger_num);

	check_plan();
}

static int
test_main(void)
{
	plan(5);
	test_basic();
	test_event_registry_foreach();
	test_event_foreach();
	test_event_iterator();
	test_event_iterator_stability();
	return check_plan();
}

int
main(void)
{
	srand(time(NULL));
	memory_init();
	fiber_init(fiber_c_invoke);
	event_registry_init();
	int rc = test_main();
	event_registry_free();
	fiber_free();
	memory_free();
	return rc;
}
