/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2023, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#include <stdbool.h>
#include <stddef.h>
#include "core/func_adapter.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

enum func_adapter_type {
	FUNC_ADAPTER_TYPE_TUPLE,
	FUNC_ADAPTER_TYPE_STRING,
	FUNC_ADAPTER_TYPE_BOOL,
	FUNC_ADAPTER_TYPE_NULL,
	FUNC_ADAPTER_TYPE_DOUBLE,
	FUNC_ADAPTER_TYPE_MSGPACK,
};

struct
func_adapter_c_data_payload {
	const char *data;
	const char *data_end;
};

struct func_adapter_c_value {
	enum func_adapter_type type;
	union {
		bool boolean;
		struct func_adapter_c_data_payload str;
		struct tuple *tuple;
		struct func_adapter_c_data_payload msgpack;
		double number;
	} value;
};

typedef int
(*func_adapter_c_f)(struct func_adapter_c_value *args, int argc);

/**
 * Creates func_adapter_c. Never returns NULL.
 */
struct func_adapter *
func_adapter_c_create(func_adapter_c_f func);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
