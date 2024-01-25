/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2023, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/** Func adapter for persistent function */

struct func;
struct func_adapter;

struct func_adapter *
func_adapter_func_create(struct func *func);

#ifdef __cplusplus
} /* extern "C" */
#endif
