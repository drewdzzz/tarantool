#include <stdbool.h>
#include <stddef.h>
#include <string.h>

#include "diag.h"
#include "fiber.h"
#include "memory.h"
#include "trivia/util.h"
#include "event.h"

#define UNIT_TAP_COMPATIBLE 1
#include "unit.h"

static int
trigger_foo(struct trigger *trigger, void *event)
{
	(void)trigger;
	(void)event;
	return 0;
}

static void
test_basic(void)
{
	plan(4 * 6);
	const char *names[] = {
		"name",
		"name with spaces",
		"namespace.name",
		"NAMESPACE[123].name"
	};
	for (size_t i = 0; i < lengthof(names); ++i) {
		const char *name = names[i];
		struct event *event = event_registry_get(name, false);
		is(event, NULL, "No such event");
		event = event_registry_get(name, true);
		isnt(event, NULL, "Event must be created");
		struct event *found_event = event_registry_get(name, false);
		is(found_event, event, "Created event must be found");
		found_event = event_registry_get(name, true);
		is(found_event, event, "Duplicate event must not be created");
		TRIGGER(my_trigger, trigger_foo);
		trigger_add(&event->triggers, &my_trigger);
		event_registry_delete_if_unused(event);
		found_event = event_registry_get(name, false);
		is(found_event, event, "Must not delete event with triggers");
		trigger_clear(&my_trigger);
		event_registry_delete_if_unused(event);
		found_event = event_registry_get(name, false);
		is(found_event, NULL, "Empty event must be deleted");
	}
	check_plan();
}

struct foreach_arg {
	int names_len;
	const char **names;
	int traversed;
};

static bool
foreach_f(struct event *event, void *arg)
{
	struct foreach_arg *data = (struct foreach_arg *)arg;
	data->traversed++;
	bool name_found = false;
	for (int i = 0; i < data->names_len && !name_found; ++i) {
		name_found = strcmp(event->name, data->names[i]) == 0;
	}
	ok(name_found, "Traversed event must really exist");
	return true;
}

static void
test_foreach(void)
{
	plan(5);
	const char *names[] = {
		"event",
		"my_events.event1",
		"my_events.event3",
		"my_events[15].event"
	};
	for (size_t i = 0; i < lengthof(names); ++i)
		event_registry_get(names[i], true);

	struct foreach_arg arg = {
		.names_len = lengthof(names),
		.names = names,
		.traversed = 0,
	};

	event_registry_foreach(foreach_f, &arg);
	is(arg.traversed, lengthof(names), "All the events must be traversed");

	for (size_t i = 0; i < lengthof(names); ++i) {
		struct event *event = event_registry_get(names[i], false);
		event_registry_delete_if_unused(event);
	}

	check_plan();
}

static int
test_main(void)
{
	plan(2);
	test_basic();
	test_foreach();
	return check_plan();
}

int
main(void)
{
	memory_init();
	fiber_init(fiber_c_invoke);
	event_registry_init();
	int rc = test_main();
	event_registry_free();
	fiber_free();
	memory_free();
	return rc;
}
