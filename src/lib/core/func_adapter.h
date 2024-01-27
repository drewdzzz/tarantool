/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2023, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "trivia/util.h"
#include "port.h"

#ifdef __cplusplus
extern "C" {
#endif

struct func_adapter;

/**
 * Virtual table for func_adapter class.
 */
struct func_adapter_vtab {
	/**
	 * Calls the function. Both ports can be NULL.
	 * Argument ret is guaranteed to be an initialized in case of
	 * success and uninitialized in case of fail.
	 */
	int (*call)(struct func_adapter *func, struct port *args,
		    struct port *ret);
	/**
	 * Virtual destructor of the class.
	 */
	void (*destroy)(struct func_adapter *func);
};

/**
 * Base class for all function adapters. Instance of this class should not
 * be created.
 */
struct func_adapter {
	/**
	 * Virtual table.
	 */
	const struct func_adapter_vtab *vtab;
};

static inline int
func_adapter_call(struct func_adapter *func, struct port *args, struct port *ret)
{
	return func->vtab->call(func, args, ret);
}

static inline void
func_adapter_destroy(struct func_adapter *func)
{
	func->vtab->destroy(func);
}

#ifdef __cplusplus
} /* extern "C" */
#endif
