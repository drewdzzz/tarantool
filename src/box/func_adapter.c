/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2023, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "box/port.h"
#include "box/func.h"

#include "core/func_adapter.h"

/**
 * Specialization of func_adapter for persistent functions.
 */
struct func_adapter_func {
	/**
	 * Virtual table.
	 */
	const struct func_adapter_vtab *vtab;
	/**
	 * Reference to the function itself.
	 */
	struct func *func;
};


/**
 * Call the function with ports.
 */
static int
func_adapter_func_call(struct func_adapter *base, struct port *args, struct port *ret)
{
	struct func_adapter_func *func = (struct func_adapter_func *)base;
	return func_call_no_access_check(func->func, args, ret);
}

/**
 * Virtual destructor.
 */
static void
func_adapter_func_destroy(struct func_adapter *func_base)
{
	struct func_adapter_func *func = (struct func_adapter_func *)func_base;
	free(func);
}

struct func_adapter *
func_adapter_func_create(struct func *pfunc)
{
	static const struct func_adapter_vtab vtab = {
		.call = func_adapter_func_call,
		.destroy = func_adapter_func_destroy,
	};
	struct func_adapter_func *func = xmalloc(sizeof(*func));
	func->func = pfunc;
	func->vtab = &vtab;
	return (struct func_adapter *)func;
}
