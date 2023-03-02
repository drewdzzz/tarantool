/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "memtx_pgdm.h"
#include "say.h"
#include "fiber.h"
#include "index.h"
#include "tuple.h"
#include "txn.h"
#include "memtx_tx.h"
#include "memtx_engine.h"
#include "memtx_tuple_compression.h"
#include "read_view.h"
#include "space.h"
#include "schema.h" /* space_by_id(), space_cache_find() */
#include "errinj.h"
#include "trivia/config.h"

#include <small/mempool.h>
#include <salad/pgdm.hpp>

static const unsigned EPS = 14;
static const unsigned DELTA = 4;

struct memtx_pgdm_index {
	struct index base;
	/**
	 * We store "hash" of tuple as a key and tuple itself as a value.
	 * Now only integer values are supported, so number value
	 * is a hash itself.
	 */
	pgdm_map<long long, struct tuple *, EPS, DELTA> *pgdm = nullptr;
	struct key_def *cmp_def;
	struct memtx_gc_task gc_task;
};

/* {{{ MemtxPgdm -- implementation. **********************/

static void
memtx_pgdm_index_free(struct memtx_pgdm_index *index)
{
	delete(index->pgdm);
	/* TODO: destroy PGDM. */
	free(index);
}

static void
memtx_pgdm_index_destroy(struct index *base)
{
	struct memtx_pgdm_index *index = (struct memtx_pgdm_index *)base;
	assert(base->def->iid != 0);
	/*
	 * Secondary index. Destruction is fast, no need to
	 * hand over to background fiber.
	 */
	memtx_pgdm_index_free(index);
}

static void
memtx_pgdm_index_update_def(struct index *base)
{
	struct memtx_pgdm_index *index = (struct memtx_pgdm_index *)base;
	assert(!index->base.def->key_def->is_nullable);
	assert(!index->base.def->key_def->is_multikey);
	assert(index->base.def->opts.is_unique);;
	index->cmp_def = index->base.def->key_def;
}

static ssize_t
memtx_pgdm_index_size(struct index *base)
{
	(void)base;
	abort();
	return -1;
}

static ssize_t
memtx_pgdm_index_bsize(struct index *base)
{
	(void)base;
	abort();
	return -1;
}

static int
memtx_pgdm_index_random(struct index *base, uint32_t rnd, struct tuple **result)
{
	(void)base;
	(void)rnd;
	(void)result;
	abort();
	return -1;
}

static ssize_t
memtx_pgdm_index_count(struct index *base, enum iterator_type type,
		       const char *key, uint32_t part_count)
{
	(void)base;
	(void)type;
	(void)key;
	(void)part_count;
	abort();
	return -1;
}

static int
memtx_pgdm_index_get_internal(struct index *base, const char *key,
			      uint32_t part_count, struct tuple **result)
{
	struct memtx_pgdm_index *index = (struct memtx_pgdm_index *)base;

	assert(base->def->opts.is_unique &&
	       part_count == base->def->key_def->part_count);
	(void) part_count;

	struct space *space = space_by_id(base->def->space_id);
	struct txn *txn = in_txn();
	*result = NULL;

	assert(part_count == 1);
	assert(mp_typeof(*key) == MP_UINT);
	const char **key_ptr = &key;
	long long int_key = mp_decode_uint(key_ptr);
	struct tuple *tuple = NULL;
	bool found = index->pgdm->find(int_key, &tuple);
	if (found) {
		*result = memtx_tx_tuple_clarify(txn, space, tuple, base, 0);
/********MVCC TRANSACTION MANAGER STORY GARBAGE COLLECTION BOUND START*********/
		memtx_tx_story_gc();
/*********MVCC TRANSACTION MANAGER STORY GARBAGE COLLECTION BOUND END**********/
	} else {
/********MVCC TRANSACTION MANAGER STORY GARBAGE COLLECTION BOUND START*********/
		memtx_tx_track_point(txn, space, base, key);
/*********MVCC TRANSACTION MANAGER STORY GARBAGE COLLECTION BOUND END**********/
	}
	return 0;
}

static int
memtx_pgdm_index_replace(struct index *base, struct tuple *old_tuple,
			 struct tuple *new_tuple, enum dup_replace_mode mode,
			 struct tuple **result, struct tuple **successor)
{
	assert(old_tuple != NULL || new_tuple != NULL);
	(void)mode;
	struct memtx_pgdm_index *index = (struct memtx_pgdm_index *)base;
	(void)index;

	/* PGDM index doesn't support ordering yet. */
	*successor = NULL;
	uint32_t key_size;
	const char *key = tuple_extract_key(new_tuple, base->def->key_def, MULTIKEY_NONE, &key_size);
	assert(mp_typeof(*key) == MP_ARRAY);
	const char **key_ptr = &key;
	size_t elem_num = mp_decode_array(key_ptr);
	assert(elem_num == 1);
	assert(mp_typeof(*key) == MP_UINT);
	long long int_key = mp_decode_uint(key_ptr);

	if (old_tuple != NULL && new_tuple == NULL) {
		abort();
	}

	if (new_tuple != NULL) {
		index->pgdm->insert(int_key, new_tuple);
	}

	*result = old_tuple;
	return 0;
}

/** Implementation of create_iterator for memtx hash index. */
static struct iterator *
memtx_pgdm_index_create_iterator(struct index *base, enum iterator_type type,
				 const char *key, uint32_t part_count,
				 const char *pos)
{
	(void)base;
	(void)type;
	(void)key;
	(void)part_count;
	(void)pos;
	abort();
	return NULL;
}

/** Implementation of create_read_view index callback. */
static struct index_read_view *
memtx_pgdm_index_create_read_view(struct index *base,
				  const struct read_view_opts *opts)
{
	(void)base;
	(void)opts;
	abort();
	return NULL;
}

static const struct index_vtab memtx_pgdm_index_vtab = {
	/* .destroy = */ memtx_pgdm_index_destroy,
	/* .commit_create = */ generic_index_commit_create,
	/* .abort_create = */ generic_index_abort_create,
	/* .commit_modify = */ generic_index_commit_modify,
	/* .commit_drop = */ generic_index_commit_drop,
	/* .update_def = */ memtx_pgdm_index_update_def,
	/* .depends_on_pk = */ generic_index_depends_on_pk,
	/* .def_change_requires_rebuild = */
		memtx_index_def_change_requires_rebuild,
	/* .size = */ memtx_pgdm_index_size,
	/* .bsize = */ memtx_pgdm_index_bsize,
	/* .min = */ generic_index_min,
	/* .max = */ generic_index_max,
	/* .random = */ memtx_pgdm_index_random,
	/* .count = */ memtx_pgdm_index_count,
	/* .get_internal = */ memtx_pgdm_index_get_internal,
	/* .get = */ memtx_index_get,
	/* .replace = */ memtx_pgdm_index_replace,
	/* .create_iterator = */ memtx_pgdm_index_create_iterator,
	/* .create_read_view = */ memtx_pgdm_index_create_read_view,
	/* .stat = */ generic_index_stat,
	/* .compact = */ generic_index_compact,
	/* .reset_stat = */ generic_index_reset_stat,
	/* .begin_build = */ generic_index_begin_build,
	/* .reserve = */ generic_index_reserve,
	/* .build_next = */ generic_index_build_next,
	/* .end_build = */ generic_index_end_build,
};

struct index *
memtx_pgdm_index_new(struct memtx_engine *memtx, struct index_def *def)
{
	struct memtx_pgdm_index *index =
		(struct memtx_pgdm_index *)calloc(1, sizeof(*index));
	if (index == NULL) {
		diag_set(OutOfMemory, sizeof(*index),
			 "malloc", "struct memtx_hash_index");
		return NULL;
	}
	index->pgdm = new pgdm_map<long long, struct tuple *, EPS, DELTA>;
	if (index_create(&index->base, (struct engine *)memtx,
			 &memtx_pgdm_index_vtab, def) != 0) {
		free(index);
		return NULL;
	}

	/* Init PGDM. */
	return &index->base;
}

/* }}} */
