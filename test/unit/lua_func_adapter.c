#include "diag.h"
#include "fiber.h"
#include "memory.h"
#include "msgpuck.h"
#include "lua/init.h"
#include "lua/error.h"
#include "lua/utils.h"
#include "lua/msgpack.h"
#include "box/port.h"
#include "box/tuple.h"
#include "box/lua/func_adapter.h"
#include "box/lua/tuple.h"
#include "core/func_adapter.h"

#define UNIT_TAP_COMPATIBLE 1
#include "unit.h"
#include "lua_test_utils.h"

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

/**
 * Generates Lua function from string and returns its index in tarantool_L.
 */
static int
generate_function(const char *function)
{
	int rc = luaT_dostring(tarantool_L, tt_sprintf("return %s", function));
	fail_if(rc != 0);
	return lua_gettop(tarantool_L);
}

static void
test_numeric(void)
{
	plan(6);
	header();

	int idx = generate_function(
		"function(a, b, c, d) "
		"return a * b * c * d, a + b + c + d end");
	const double expected[] = { 3 * 5 * 7 * 11, 3 + 5 + 7 + 11};
	struct func_adapter *func = func_adapter_lua_create(tarantool_L, idx);
	struct port args, ret;
	port_light_create(&args);
	port_light_add_double(&args, 3);
	port_light_add_double(&args, 5);
	port_light_add_double(&args, 7);
	port_light_add_double(&args, 11);
	int rc = func_adapter_call(func, &args, &ret);
	fail_if(rc != 0);

	port_enlight(&ret);
	for (size_t i = 0; i < lengthof(expected); ++i) {
		ok(port_light_is_double(&ret, i), "Expected double");
		double retval = 0;
		port_light_get_double(&ret, i, &retval);
		ok(number_eq(expected[i], retval),
		   "Returned value must be as expected");
	}

	ok(port_light_is_none(&ret, lengthof(expected)), "No values left");
	ok(!port_light_is_null(&ret, lengthof(expected)),
	   "NULL is not absence");
	port_destroy(&args);
	port_destroy(&ret);
	func_adapter_destroy(func);
	lua_settop(tarantool_L, 0);

	footer();
	check_plan();
}

static void
test_tuple(void)
{
	plan(17);
	header();

	int idx = generate_function(
		"function(a, b, tuple) "
		"return box.internal.tuple.new{a, b}, tuple, "
		"box.internal.tuple.new{b, a}, "
		"box.internal.tuple.new{a + b, a - b} end");
	struct func_adapter *func = func_adapter_lua_create(tarantool_L, idx);
	struct port args, ret;
	port_light_create(&args);
	port_light_add_double(&args, 42);
	port_light_add_double(&args, 43);
	static const char *tuple_data = "\x92\x06\x03";
	struct tuple *tuple = tuple_new(tuple_format_runtime, tuple_data,
					tuple_data + strlen(tuple_data));
	tuple_ref(tuple);
	port_light_add_tuple(&args, tuple);
	int rc = func_adapter_call(func, &args, &ret);
	fail_if(rc != 0);

	port_enlight(&ret);
	struct tuple *tuples[4];
	for (size_t i = 0; i < lengthof(tuples); ++i) {
		ok(port_light_is_tuple(&ret, i), "Expected tuple");
		port_light_get_tuple(&ret, i, tuples + i);
		isnt(tuples[i], NULL, "Returned tuple must not be NULL");
	}
	ok(port_light_is_none(&ret, lengthof(tuples)), "No values left");
	port_destroy(&args);
	port_destroy(&ret);
	func_adapter_destroy(func);
	lua_settop(tarantool_L, 0);

	const char *expected_tuples[] = {
		"[42, 43]", "[6, 3]", "[43, 42]", "[85, -1]"};
	for (size_t i = 0; i < lengthof(tuples); ++i) {
		ok(!tuple_is_unreferenced(tuples[i]),
		   "Returned tuple must be referenced");
		const char *str = tuple_str(tuples[i]);
		is(strcmp(expected_tuples[i], str), 0, "Expected %s, got %s",
		   expected_tuples[i], str);
		tuple_unref(tuples[i]);
	}
	tuple_unref(tuple);

	footer();
	check_plan();
}

static void
test_string(void)
{
	plan(7);
	header();

	int idx = generate_function(
		"function(s1, s2) "
		"return s1, s1 .. s2 end");
	struct func_adapter *func = func_adapter_lua_create(tarantool_L, idx);
	struct port args, ret;
	port_light_create(&args);
	/* Not zero-terminated string. */
	const char s1[] = {'a', 'b', 'c'};
	size_t s1_len = lengthof(s1);
	const char *s2 = "42strstr";
	size_t s2_len = strlen(s2);
	port_light_add_str(&args, s1, s1_len);
	port_light_add_str0(&args, s2);
	int rc = func_adapter_call(func, &args, &ret);
	fail_if(rc != 0);

	port_enlight(&ret);
	ok(port_light_is_str(&ret, 0), "Expected string");
	const char *retval = NULL;
	size_t len = 0;
	port_light_get_str(&ret, 0, &retval, &len);
	is(len, s1_len, "Length of popped string must match");
	is(strncmp(retval, s1, s1_len), 0, "Popped string must match");
	ok(port_light_is_str(&ret, 1), "Expected string");
	port_light_get_str(&ret, 1, &retval, &len);
	is(len, s1_len + s2_len, "Len does not match");
	char *buf = tt_static_buf();
	strncpy(buf, s1, s1_len);
	strcpy(buf + s1_len, s2);
	is(strcmp(retval, buf), 0, "Expected %s", buf);
	ok(port_light_is_none(&ret, 2), "No values left");
	port_destroy(&args);
	port_destroy(&ret);
	func_adapter_destroy(func);
	lua_settop(tarantool_L, 0);

	footer();
	check_plan();
}

static void
test_bool(void)
{
	plan(10);
	header();

	int idx = generate_function(
		"function(a, b, c, d) "
		"return a, not b, c, not d end");
	bool arguments[4];
	for (size_t i = 0; i < lengthof(arguments); ++i)
		arguments[i] = rand() % 2 == 0;
	struct func_adapter *func = func_adapter_lua_create(tarantool_L, idx);
	struct port args, ret;
	port_light_create(&args);
	for (size_t i = 0; i < lengthof(arguments); ++i)
		port_light_add_bool(&args, arguments[i]);
	int rc = func_adapter_call(func, &args, &ret);
	fail_if(rc != 0);

	port_enlight(&ret);
	for (size_t i = 0; i < lengthof(arguments); ++i) {
		ok(port_light_is_bool(&ret, i), "Expected double");
		bool retval = false;
		port_light_get_bool(&ret, i, &retval);
		bool is_odd = i % 2 == 0;
		bool equal = arguments[i] == retval;
		is(is_odd, equal, "Only odd elements are equal");
	}

	ok(!port_light_is_bool(&ret, lengthof(arguments)),
	   "No values left - no bool");
	ok(port_light_is_none(&ret, lengthof(arguments)), "No values left");
	port_destroy(&args);
	port_destroy(&ret);
	func_adapter_destroy(func);
	lua_settop(tarantool_L, 0);

	footer();
	check_plan();
}

static void
test_null(void)
{
	plan(7);
	header();

	int idx = generate_function(
		"function(a, b, c) return a, box.NULL, nil, c, b end");
	const size_t null_count = 4;
	const double double_val = 42;
	struct func_adapter *func = func_adapter_lua_create(tarantool_L, idx);
	struct port args, ret;
	port_light_create(&args);
	port_light_add_null(&args);
	port_light_add_double(&args, double_val);
	int rc = func_adapter_call(func, &args, &ret);
	fail_if(rc != 0);

	port_enlight(&ret);
	for (size_t i = 0; i < null_count; ++i)
		ok(port_light_is_null(&ret, i), "Expected null");
	ok(port_light_is_double(&ret, null_count), "Expected double");
	double double_retval = 0;
	port_light_get_double(&ret, null_count, &double_retval);
	ok(port_light_is_none(&ret, null_count + 1), "No values left");
	port_destroy(&args);
	port_destroy(&ret);
	func_adapter_destroy(func);
	lua_settop(tarantool_L, 0);

	is(double_retval, double_val, "Returned value must be as expected");

	footer();
	check_plan();
}

static void
test_msgpack(void)
{
	plan(7);
	header();

	#define MP_BUF_LEN 64
	char mp_buf[MP_BUF_LEN];
	char *mp = mp_buf;
	mp = mp_encode_map(mp, 2);
	mp = mp_encode_str0(mp, "key");
	mp = mp_encode_str0(mp, "value");
	mp = mp_encode_uint(mp, 42);
	mp = mp_encode_uint(mp, 64);
	fail_unless(mp < mp_buf + MP_BUF_LEN);
	#undef MP_BUF_LEN

	int idx = generate_function(
		"function(a) "
		"  local mp = require('msgpack') "
		"  assert(mp.is_object(a)) "
		"  return a.key, a[42] "
		"end");

	struct func_adapter *func = func_adapter_lua_create(tarantool_L, idx);
	struct port args, ret;
	port_light_create(&args);
	port_light_add_mp(&args, mp_buf, mp);
	int rc = func_adapter_call(func, &args, &ret);
	is(rc, 0, "Function must return successfully");

	port_enlight(&ret);
	ok(port_light_is_str(&ret, 0), "A string must be returned");
	const char *str = NULL;
	size_t str_len = 0;
	port_light_get_str(&ret, 0, &str, &str_len);
	is(str_len, strlen("value"), "Returned value must be as expected");
	is(strncmp(str, "value", str_len), 0,
	   "Returned value must be as expected");
	ok(port_light_is_double(&ret, 1), "A double must be returned");
	double val = 0.0;
	port_light_get_double(&ret, 1, &val);
	ok(number_eq(64, val), "Returned value must be as expected");
	ok(port_light_is_none(&ret, 2), "No values left");
	port_destroy(&args);
	port_destroy(&ret);
	func_adapter_destroy(func);

	footer();
	check_plan();
}

static void
test_error(void)
{
	plan(2);
	header();

	const char *functions[] = {
		"function() error('lua error') end",
		"function() box.error('tnt error') end"
	};

	for (size_t i = 0; i < lengthof(functions); ++i) {
		int idx = generate_function(functions[i]);
		struct func_adapter *func = func_adapter_lua_create(tarantool_L,
								    idx);
		int rc = func_adapter_call(func, NULL, NULL);
		is(rc, -1, "Call must fail");
		func_adapter_destroy(func);
		lua_settop(tarantool_L, 0);
	}

	footer();
	check_plan();
}

static void
test_get_func(void)
{
	plan(1);
	header();

	struct lua_State *L = tarantool_L;
	int idx = generate_function("function(a) return a end");
	struct func_adapter *func = func_adapter_lua_create(L, idx);

	func_adapter_lua_get_func(func, L);
	is(lua_equal(L, -1, idx), 1, "Actual function must be returned");

	func_adapter_destroy(func);

	lua_settop(L, 0);

	footer();
	check_plan();
}

static void
test_callable(void)
{
	plan(4);
	header();

	const int table_value = 42;
	const int argument = 19;
	struct lua_State *L = tarantool_L;
	lua_createtable(L, 1, 0);
	lua_pushinteger(L, table_value);
	lua_rawseti(L, -2, 1);
	lua_createtable(L, 0, 1);
	generate_function("function(self, a) return self[1] - a end");
	lua_setfield(L, -2, "__call");
	lua_setmetatable(L, -2);
	int idx = lua_gettop(L);

	struct func_adapter *func = func_adapter_lua_create(L, idx);
	struct port args, ret;
	port_light_create(&args);
	port_light_add_double(&args, argument);
	int rc = func_adapter_call(func, &args, &ret);
	ok(rc == 0, "Callable table must be called successfully");

	port_enlight(&ret);
	ok(port_light_is_double(&ret, 0), "Expected double");
	double retval = 0;
	port_light_get_double(&ret, 0, &retval);
	ok(number_eq(retval, table_value - argument),
	   "Returned value must be as expected");
	port_destroy(&args);
	port_destroy(&ret);
	func_adapter_lua_get_func(func, L);
	is(lua_equal(L, -1, idx), 1, "Actual table must be returned");
	func_adapter_destroy(func);
	lua_settop(L, 0);

	footer();
	check_plan();
}

/**
 * Iterator state for the test.
 */
struct test_iterator_state {
	/**
	 * Current value, is incremented after every yield.
	 */
	double current;
	/**
	 * Maximum value, after which iterator yields nothing.
	 */
	double limit;
};

/**
 * Yields 3 sequentially growing values, stops when the iterator is exhausted.
 */
static int
test_iterator_next(void *state, struct port *out)
{
	struct test_iterator_state *test_state =
		(struct test_iterator_state *)state;
	port_light_create(out);
	for (int i = 0; i < 3; ++i) {
		if (test_state->current > test_state->limit)
			break;
		port_light_add_double(out, test_state->current++);
	}
	return 0;
}

static void
test_iterator(void)
{
	plan(3 * 2 + 1);
	header();

	struct test_iterator_state state = {.current = 1.0, .limit = 20.0};

	int idx = generate_function(
		"function(iter) "
		"  local res1 = 0 "
		"  local res2 = 0 "
		"  local res3 = 0 "
		"  for v1, v2, v3 in iter() do "
		"    if v1 ~= nil then res1 = res1 + v1 end"
		"    if v2 ~= nil then res2 = res2 + v2 end"
		"    if v3 ~= nil then res3 = res3 + v3 end"
		"  end "
		"  return res1, res2, res3 "
		"end");

	double results[3] = {0.0, 0.0, 0.0};
	int i = 0;
	for (int v = state.current; v <= state.limit; ++v, i = (i + 1) % 3)
		results[i] += v;

	struct func_adapter *func = func_adapter_lua_create(tarantool_L, idx);
	struct port args, rets;
	port_light_create(&args);
	port_light_add_iterator(&args, &state, test_iterator_next);
	int rc = func_adapter_call(func, &args, &rets);
	fail_if(rc != 0);

	port_enlight(&rets);
	for (size_t i = 0; i < 3; ++i) {
		ok(port_light_is_double(&rets, i), "Expected double");
		double val;
		port_light_get_double(&rets, i, &val);
		ok(number_eq(val, results[i]),
		   "Function result must match expected one");
	}
	ok(port_light_is_none(&rets, 3), "Func adapter is empty");
	port_destroy(&args);
	port_destroy(&rets);
	func_adapter_destroy(func);

	footer();
	check_plan();
}

/**
 * Message of error returned by iterator next.
 */
static const char *iterator_next_errmsg = "My error in iterator next";

/**
 * Iterator next that returns an error.
 */
static int
test_iterator_next_error(void *state, struct port *out)
{
	diag_set(ClientError, ER_PROC_C, iterator_next_errmsg);
	return -1;
}

/**
 * Check if errors in iterator_next are handled correctly.
 */
static void
test_iterator_error(void)
{
	plan(2);
	header();

	struct test_iterator_state state;

	int idx = generate_function(
		"function(iter) "
		"  local res = 0 "
		"  for i in iter() do res = res + i end "
		"  return res "
		"end");

	struct func_adapter *func = func_adapter_lua_create(tarantool_L, idx);
	struct port args;
	port_light_create(&args);
	port_light_add_iterator(&args, &state, test_iterator_next_error);
	int rc = func_adapter_call(func, &args, NULL);
	fail_unless(rc != 0);
	struct error *e = diag_last_error(diag_get());
	is(e->cause, NULL, "Thrown error has no cause");
	is(strcmp(e->errmsg, iterator_next_errmsg), 0,
	   "Expected errmsg: %s, got: %s", iterator_next_errmsg, e->errmsg);

	port_destroy(&args);
	func_adapter_destroy(func);

	footer();
	check_plan();
}

static int
test_lua_func_adapter(void)
{
	plan(11);
	header();

	test_numeric();
	test_tuple();
	test_string();
	test_bool();
	test_null();
	test_msgpack();
	test_error();
	test_get_func();
	test_callable();
	test_iterator();
	test_iterator_error();

	footer();
	return check_plan();
}

int
main(void)
{
	memory_init();
	fiber_init(fiber_c_invoke);
	tuple_init(NULL);
	port_init();

	lua_State *L = luaT_newteststate();
	tarantool_L = L;

	tarantool_lua_error_init(L);
	tarantool_lua_error_init(L);
	tarantool_lua_utils_init(L);
	luaopen_msgpack(L);
	box_lua_tuple_init(L);
	/*
	 * luaT_newmodule() assumes that tarantool has a special
	 * loader for built-in modules. That's true, when all the
	 * initialization code is executed. However, in the unit
	 * test we don't do that.
	 *
	 * In particular, tarantool_lua_init() function is not
	 * called in a unit test.
	 *
	 * Assign the module into package.loaded directly instead.
	 *
	 *  | local mod = loaders.builtin['msgpack']
	 *  | package.loaded['msgpack'] = mod
	 */
	lua_getfield(L, LUA_REGISTRYINDEX, "_LOADED");
	lua_getfield(L, LUA_REGISTRYINDEX, "_TARANTOOL_BUILTIN");
	lua_getfield(L, -1, "msgpack");
	lua_setfield(L, -3, "msgpack");
	lua_pop(L, 2);

	fail_unless(luaT_dostring(
			L, "mp = require('msgpack')") == 0);

	int rc = test_lua_func_adapter();

	lua_close(L);
	tarantool_L = NULL;
	port_free();
	tuple_free();
	fiber_free();
	memory_free();
	return rc;
}
