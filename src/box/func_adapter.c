/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2023, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "box/func_adapter.h"
#include "box/tuple.h"

#define FUNC_ADAPTER_C_ARG_CAPACITY 4

/**
 * Context for func_adapter_c.
 */
struct func_adapter_c_ctx {
	func_adapter_c_f func;
	struct func_adapter_c_value values[FUNC_ADAPTER_C_ARG_CAPACITY];
	int idx;
};

static_assert(sizeof(struct func_adapter_c_ctx) <= sizeof(struct func_adapter_ctx),
	      "sizeof(func_adapter_c_ctx) must be <= "
	      "sizeof(func_adapter_ctx)");

/**
 * Specialization of func_adapter for Lua functions and other callable objects.
 */
struct func_adapter_c {
	/**
	 * Virtual table.
	 */
	const struct func_adapter_vtab *vtab;
	/**
	 * Pointer to the function itself.
	 */
	func_adapter_c_f func;
};

static void
func_adapter_c_begin(struct func_adapter *base,
		     struct func_adapter_ctx *base_ctx)
{
	struct func_adapter_c *func = (struct func_adapter_c *)base;
	struct func_adapter_c_ctx *ctx =
		(struct func_adapter_c_ctx *)base_ctx;
	ctx->idx = 0;
	ctx->func = func->func; 
}

static void
func_adapter_c_end(struct func_adapter_ctx *base)
{
	struct func_adapter_c_ctx *ctx = (struct func_adapter_c_ctx *)base;
	ctx->idx = 0;
	ctx->func = NULL;
}

/**
 * Call the function with arguments that were passed before.
 */
static int
func_adapter_c_call(struct func_adapter_ctx *base_ctx)
{
	struct func_adapter_c_ctx *ctx = (struct func_adapter_c_ctx *)base_ctx;
	return ctx->func(ctx->values, ctx->idx);
}

static void
func_adapter_c_push_double(struct func_adapter_ctx *base_ctx, double val)
{
	struct func_adapter_c_ctx *ctx = (struct func_adapter_c_ctx *)base_ctx;
	ctx->values[ctx->idx].type = FUNC_ADAPTER_TYPE_DOUBLE;
	ctx->values[ctx->idx].value.number = val;
	ctx->idx++;
}

static void
func_adapter_c_push_str(struct func_adapter_ctx *base_ctx,
		       const char *str, size_t len)
{
	struct func_adapter_c_ctx *ctx = (struct func_adapter_c_ctx *)base_ctx;
	ctx->values[ctx->idx].type = FUNC_ADAPTER_TYPE_STRING;
	ctx->values[ctx->idx].value.str.data = str;
	ctx->values[ctx->idx].value.str.data_end = str + len;
	ctx->idx++;
}

static void
func_adapter_c_push_tuple(struct func_adapter_ctx *base_ctx,
			  struct tuple *tuple)
{
	struct func_adapter_c_ctx *ctx = (struct func_adapter_c_ctx *)base_ctx;
	ctx->values[ctx->idx].type = FUNC_ADAPTER_TYPE_TUPLE;
	ctx->values[ctx->idx].value.tuple = tuple;
	ctx->idx++;
}

static void
func_adapter_c_push_bool(struct func_adapter_ctx *base_ctx, bool val)
{
	struct func_adapter_c_ctx *ctx = (struct func_adapter_c_ctx *)base_ctx;
	ctx->values[ctx->idx].type = FUNC_ADAPTER_TYPE_BOOL;
	ctx->values[ctx->idx].value.boolean = val;
	ctx->idx++;
}

static void
func_adapter_c_push_null(struct func_adapter_ctx *base_ctx)
{
	struct func_adapter_c_ctx *ctx = (struct func_adapter_c_ctx *)base_ctx;
	ctx->values[ctx->idx].type = FUNC_ADAPTER_TYPE_NULL;
	ctx->idx++;
}

static void
func_adapter_c_push_msgpack(struct func_adapter_ctx *base_ctx, const char *data,
			      const char *data_end)
{
	struct func_adapter_c_ctx *ctx = (struct func_adapter_c_ctx *)base_ctx;
	ctx->values[ctx->idx].type = FUNC_ADAPTER_TYPE_MSGPACK;
	ctx->values[ctx->idx].value.msgpack.data = data;
	ctx->values[ctx->idx].value.msgpack.data_end = data_end;
	ctx->idx++;
}

static bool
func_adapter_c_is_double(struct func_adapter_ctx *base)
{
	(void)base;
	return false;
}

static void
func_adapter_c_pop_double(struct func_adapter_ctx *base, double *out)
{
	(void)base;
	(void)out;
}

static bool
func_adapter_c_is_str(struct func_adapter_ctx *base)
{
	(void)base;
	return false;
}

static void
func_adapter_c_pop_str(struct func_adapter_ctx *base, const char **str,
			 size_t *len)
{
	(void)base;
	(void)str;
	(void)len;
}

/**
 * Check if the next value is a cdata tuple.
 */
static bool
func_adapter_c_is_tuple(struct func_adapter_ctx *base)
{
	(void)base;
	return false;
}

/**
 * Pops cdata tuple. Does not cast Lua tables to tuples.
 */
static void
func_adapter_c_pop_tuple(struct func_adapter_ctx *base, struct tuple **out)
{
	(void)base;
	(void)out;
}

static bool
func_adapter_c_is_bool(struct func_adapter_ctx *base)
{
	(void)base;
	return false;
}

static void
func_adapter_c_pop_bool(struct func_adapter_ctx *base, bool *val)
{
	(void)base;
	(void)val;
}

/**
 * Null in Lua can be represented in two ways: nil or box.NULL.
 * The function checks both cases.
 */
static bool
func_adapter_c_is_null(struct func_adapter_ctx *base)
{
	(void)base;
	return false;
}

static void
func_adapter_c_pop_null(struct func_adapter_ctx *base)
{
	(void)base;
}

static bool
func_adapter_c_is_empty(struct func_adapter_ctx *base)
{
	(void)base;
	return true;
}

/**
 * Virtual destructor.
 */
static void
func_adapter_c_destroy(struct func_adapter *func_base)
{
	free(func_base);
}

struct func_adapter *
func_adapter_c_create(func_adapter_c_f f)
{
	static const struct func_adapter_vtab vtab = {
		.begin = func_adapter_c_begin,
		.end = func_adapter_c_end,
		.call = func_adapter_c_call,

		.push_double = func_adapter_c_push_double,
		.push_str = func_adapter_c_push_str,
		.push_tuple = func_adapter_c_push_tuple,
		.push_bool = func_adapter_c_push_bool,
		.push_null = func_adapter_c_push_null,
		.push_msgpack = func_adapter_c_push_msgpack,

		.is_double = func_adapter_c_is_double,
		.pop_double = func_adapter_c_pop_double,
		.is_str = func_adapter_c_is_str,
		.pop_str = func_adapter_c_pop_str,
		.is_tuple = func_adapter_c_is_tuple,
		.pop_tuple = func_adapter_c_pop_tuple,
		.is_bool = func_adapter_c_is_bool,
		.pop_bool = func_adapter_c_pop_bool,
		.is_null = func_adapter_c_is_null,
		.pop_null = func_adapter_c_pop_null,
		.is_empty = func_adapter_c_is_empty,

		.destroy = func_adapter_c_destroy,
	};
	struct func_adapter_c *func = xmalloc(sizeof(*func));
	func->vtab = &vtab;
	func->func = f;
	return (struct func_adapter *)func;
}
