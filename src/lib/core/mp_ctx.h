/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2023, Tarantool AUTHORS, please see AUTHORS file.
 */

#pragma once

#include "trivia/util.h"

#include <stddef.h>

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

struct mh_strnu32_t;

/**
 * Base context for MsgPack encoding/decoding.
 */
struct mp_ctx {
	/**
	 * If a  first-level `MP_MAP` key has `MP_STRING` type, the key is
	 * looked up and replaced with a translation, if found. The translation
	 * table must use `lua_hash` as the hash function.
	 *
	 * Can be `NULL`.
	 */
	struct mh_strnu32_t *translation;
	/** 'Virtual' destructor. Can be `NULL` in which case it is a no-op. */
	void (*destroy)(struct mp_ctx *ctx);
	/**
	 * 'Virtual' move. Moves ownership from @a src to @a dst.
	 *
	 * Cannot be `NULL`.
	 */
	void (*move)(struct mp_ctx *dst, struct mp_ctx *src);
	/**
	 * 'Virtual' copy. Copies @a src to @a dst.
	 *
	 * Cannot be `NULL`.
	 */
	void (*copy)(struct mp_ctx *dst, struct mp_ctx *src);
	/**
	 * Implementation dependent content. Needed to declare an abstract
	 * MsgPack context instance on stack.
	 */
	char padding[80];
};

static inline void
mp_ctx_create(struct mp_ctx *ctx, struct mh_strnu32_t *translation,
	      void (*destroy)(struct mp_ctx *),
	      void (*move)(struct mp_ctx *, struct mp_ctx *),
	      void (*copy)(struct mp_ctx *, struct mp_ctx *))
{
	ctx->translation = translation;
	ctx->destroy = destroy;
	ctx->move = move;
	ctx->copy = copy;
}

/**
 * Default 'virtual' move: moves the contents of @a dst to @a src.
 */
void
mp_ctx_move_default(struct mp_ctx *dst, struct mp_ctx *src);

/**
 * Default 'virtual' copy: copies the contents of @a dst to @a src.
 */
void
mp_ctx_copy_default(struct mp_ctx *dst, struct mp_ctx *src);

/**
 * Create @a ctx with default virtual methods (i.e., `NULL` destructor,
 * `mp_ctx_move_default` move and `mp_ctx_copy_default` copy).
 */
static inline void
mp_ctx_create_default(struct mp_ctx *ctx, struct mh_strnu32_t *translation)
{
	ctx->translation = translation;
	ctx->destroy = NULL;
	ctx->move = mp_ctx_move_default;
	ctx->copy = mp_ctx_copy_default;
}

static inline void
mp_ctx_destroy(struct mp_ctx *ctx)
{
	if (ctx->destroy != NULL)
		ctx->destroy(ctx);
	TRASH(ctx);
}

/**
 * 'Virtual' move. Provides move constructor semantics, @dst is supposed not to
 * own any resources, hence it is not destroyed.
 */
static inline void
mp_ctx_move(struct mp_ctx *dst, struct mp_ctx *src)
{
	assert(src->move != NULL);
	src->move(dst, src);
}

/**
 * 'Virtual' copy. Provides copy constructor semantics, @dst is supposed not to
 * own any resources, hence it is not destroyed.
 */
static inline void
mp_ctx_copy(struct mp_ctx *dst, struct mp_ctx *src)
{
	assert(src->copy != NULL);
	src->copy(dst, src);
}

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
