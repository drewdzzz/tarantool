#include "diag.h"
#include "fiber.h"
#include "memory.h"
#include "msgpuck.h"
#include "lua/init.h"
#include "lua/error.h"
#include "lua/utils.h"
#include "lua/msgpack.h"
#include "box/tuple.h"
#include "box/func_adapter.h"
#include "core/func_adapter.h"

#define UNIT_TAP_COMPATIBLE 1
#include "unit.h"

#define EPS 0.0001

/**
 * Check if two floating point numbers are equal.
 */
static bool
number_eq(double a, double b)
{
	return fabs(a - b) < EPS;
}

#undef EPS

static double sum = 0.0;

static int
test_numeric_f(struct func_adapter_c_value *values, int argc)
{
	for (int i = 0; i < argc; i++) {
		is(values[i].type, FUNC_ADAPTER_TYPE_DOUBLE, "Double is expected");
		sum += values[i].value.number;
	}
	return 0;
}

static void
test_numeric(void)
{
	plan(5);
	header();

	struct func_adapter *func = func_adapter_c_create(test_numeric_f);
	struct func_adapter_ctx ctx;
	func_adapter_begin(func, &ctx);
	func_adapter_push_double(func, &ctx, 3);
	func_adapter_push_double(func, &ctx, 5);
	func_adapter_push_double(func, &ctx, 7);
	func_adapter_push_double(func, &ctx, 11);
	int rc = func_adapter_call(func, &ctx);
	fail_if(rc != 0);
	func_adapter_end(func, &ctx);
	func_adapter_destroy(func);
	is (sum, 3 + 5 + 7 + 11, "Sum must be as expected");

	footer();
	check_plan();
}

static int
test_func_adapter_c(void)
{
	plan(1);
	header();

	test_numeric();

	footer();
	return check_plan();
}

int
main(void)
{
	memory_init();
	fiber_init(fiber_c_invoke);
	tuple_init(NULL);


	int rc = test_func_adapter_c();

	tuple_free();
	fiber_free();
	memory_free();
	return rc;
}
