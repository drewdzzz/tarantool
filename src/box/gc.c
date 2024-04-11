/*
 * Copyright 2010-2017, Tarantool AUTHORS, please see AUTHORS file.
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
 * THIS SOFTWARE IS PROVIDED BY AUTHORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "gc.h"

#include <trivia/util.h>

#include <assert.h>
#include <limits.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#define RB_COMPACT 1
#include <small/rb.h>
#include <small/rlist.h>
#include <small/slab_cache.h>
#include <tarantool_ev.h>

#include "diag.h"
#include "errcode.h"
#include "fiber.h"
#include "fiber_cond.h"
#include "latch.h"
#include "say.h"
#include "cbus.h"
#include "engine.h"		/* engine_collect_garbage() */
#include "wal.h"		/* wal_collect_garbage() */
#include "checkpoint_schedule.h"
#include "txn_limbo.h"
#include "tt_uuid.h"
#include "txn.h"
#include "replication.h"
#include "box.h"
#include "alter.h"
#include "tuple.h"
#include "session.h"
#include "space_cache.h"

struct gc_state gc;

static int
gc_cleanup_fiber_f(va_list);
static int
gc_checkpoint_fiber_f(va_list);
static int
gc_consumers_persist_fiber_f(va_list);

/**
 * Comparator used for ordering gc_consumer objects
 * lexicographically by their vclock in a binary tree.
 */
static inline int
gc_consumer_cmp(const struct gc_consumer *a, const struct gc_consumer *b)
{
	int rc = vclock_lex_compare(&a->vclock, &b->vclock);
	if (rc != 0)
		return rc;
	if ((intptr_t)a < (intptr_t)b)
		return -1;
	if ((intptr_t)a > (intptr_t)b)
		return 1;
	return 0;
}

rb_gen(MAYBE_UNUSED static inline, gc_tree_, gc_tree_t,
       struct gc_consumer, node, gc_consumer_cmp);

/** Free a consumer object. */
static void
gc_consumer_delete(struct gc_consumer *consumer)
{
	TRASH(consumer);
	mempool_free(&gc.gc_consumer_pool, consumer);
}

/** Free a checkpoint object. */
static void
gc_checkpoint_delete(struct gc_checkpoint *checkpoint)
{
	TRASH(checkpoint);
	free(checkpoint);
}

void
gc_init(on_garbage_collection_f on_garbage_collection)
{
	/* Don't delete any files until recovery is complete. */
	gc.min_checkpoint_count = INT_MAX;

	gc.wal_cleanup_delay = TIMEOUT_INFINITY;
	gc.delay_ref = 0;
	gc.is_paused = true;
	say_info("wal/engine cleanup is paused");

	mempool_create(&gc.gc_consumer_pool, &cord()->slabc,
		       sizeof(struct gc_consumer));
	vclock_create(&gc.vclock);
	rlist_create(&gc.checkpoints);
	gc_tree_new(&gc.consumers);
	rlist_create(&gc.all_consumers);
	fiber_cond_create(&gc.cleanup_cond);
	checkpoint_schedule_cfg(&gc.checkpoint_schedule, 0, 0);

	gc.cleanup_fiber = fiber_new_system("gc", gc_cleanup_fiber_f);
	if (gc.cleanup_fiber == NULL)
		panic("failed to start garbage collection fiber");
	fiber_set_joinable(gc.cleanup_fiber, true);

	gc.checkpoint_fiber = fiber_new_system("checkpoint_daemon",
					       gc_checkpoint_fiber_f);
	if (gc.checkpoint_fiber == NULL)
		panic("failed to start checkpoint daemon fiber");
	fiber_set_joinable(gc.checkpoint_fiber, true);

	gc.persist_fiber = fiber_new_system("gc_consumers_persist_daemon",
					       gc_consumers_persist_fiber_f);
	if (gc.persist_fiber == NULL)
		panic("failed to start gc consumers persist daemon fiber");
	fiber_set_joinable(gc.persist_fiber, true);

	gc.on_garbage_collection = on_garbage_collection;

	fiber_start(gc.cleanup_fiber);
	fiber_start(gc.checkpoint_fiber);
	fiber_start(gc.persist_fiber);
}

void
gc_shutdown(void)
{
	fiber_cancel(gc.checkpoint_fiber);
	fiber_cancel(gc.cleanup_fiber);
	fiber_cancel(gc.persist_fiber);
	fiber_join(gc.checkpoint_fiber);
	gc.checkpoint_fiber = NULL;
	fiber_join(gc.cleanup_fiber);
	gc.cleanup_fiber = NULL;
	fiber_join(gc.persist_fiber);
	gc.persist_fiber = NULL;
}

void
gc_free(void)
{
	/*
	 * Can't clear the WAL watcher as the event loop isn't
	 * running when this function is called.
	 */

	/* Free checkpoints. */
	struct gc_checkpoint *checkpoint, *next_checkpoint;
	rlist_foreach_entry_safe(checkpoint, &gc.checkpoints, in_checkpoints,
				 next_checkpoint) {
		gc_checkpoint_delete(checkpoint);
	}
	/* Free all registered consumers. */
	struct gc_consumer *consumer = gc_tree_first(&gc.consumers);
	while (consumer != NULL) {
		struct gc_consumer *next = gc_tree_next(&gc.consumers,
							consumer);
		gc_tree_remove(&gc.consumers, consumer);
		gc_consumer_delete(consumer);
		consumer = next;
	}
	mempool_destroy(&gc.gc_consumer_pool);
}

/**
 * Invoke garbage collection in order to remove files left
 * from old checkpoints. The number of checkpoints saved by
 * this function is specified by box.cfg.checkpoint_count.
 */
static void
gc_run_cleanup(void)
{
	bool run_wal_gc = false;
	bool run_engine_gc = false;

	/*
	 * Find the oldest checkpoint that must be preserved.
	 * We have to preserve @min_checkpoint_count oldest
	 * checkpoints, plus we can't remove checkpoints that
	 * are still in use.
	 */
	struct gc_checkpoint *checkpoint = NULL;
	while (true) {
		checkpoint = rlist_first_entry(&gc.checkpoints,
				struct gc_checkpoint, in_checkpoints);
		if (gc.checkpoint_count <= gc.min_checkpoint_count)
			break;
		if (!rlist_empty(&checkpoint->refs))
			break; /* checkpoint is in use */
		rlist_del_entry(checkpoint, in_checkpoints);
		gc_checkpoint_delete(checkpoint);
		gc.checkpoint_count--;
		run_engine_gc = true;
	}

	/* At least one checkpoint must always be available. */
	assert(checkpoint != NULL);

	/* Find the vclock of the oldest WAL row to keep. */
	struct vclock min_vclock;
	struct gc_consumer *consumer = gc_tree_first(&gc.consumers);
	/*
	 * Vclock of the oldest WAL row to keep is a by-component
	 * minimum of all consumer vclocks and the oldest
	 * checkpoint vclock. This ensures that all rows needed by
	 * at least one consumer are kept.
	 * Note, we must keep all WALs created after the
	 * oldest checkpoint, even if no consumer needs them.
	 */
	vclock_copy(&min_vclock, &checkpoint->vclock);
	while (consumer != NULL) {
		/*
		 * Consumers will never need rows signed
		 * with a zero instance id (local rows).
		 */
		vclock_min_ignore0(&min_vclock, &consumer->vclock);
		consumer = gc_tree_next(&gc.consumers, consumer);
	}

	/*
	 * Acquire minimum vclock of a file, which is protected from garbage
	 * collection by wal_retention_period option.
	 */
	struct vclock retention_vclock;
	wal_get_retention_vclock(&retention_vclock);
	if (vclock_is_set(&retention_vclock))
		vclock_min(&min_vclock, &retention_vclock);

	if (vclock_sum(&min_vclock) > vclock_sum(&gc.vclock)) {
		vclock_copy(&gc.vclock, &min_vclock);
		run_wal_gc = true;
	}

	if (!run_engine_gc && !run_wal_gc)
		return; /* nothing to do */

	/*
	 * Run garbage collection.
	 *
	 * It may occur that we proceed to deletion of WAL files
	 * and other engine files after having failed to delete
	 * a memtx snap file. If this happens, the corresponding
	 * checkpoint won't be removed from box.info.gc(), because
	 * we use snap files to build the checkpoint list, but
	 * it won't be possible to back it up or recover from it.
	 * This is OK as unlink() shouldn't normally fail. Besides
	 * we never remove the last checkpoint and the following
	 * WALs so this may only affect backup checkpoints.
	 */
	if (run_engine_gc)
		engine_collect_garbage(&checkpoint->vclock);
	if (run_wal_gc)
		wal_collect_garbage(&min_vclock);
	gc.on_garbage_collection();
}

static int
gc_cleanup_fiber_f(va_list ap)
{
	(void)ap;

	/*
	 * Stage 1 (optional): in case if we're booting
	 * up with cleanup disabled lets do wait in a
	 * separate cycle to minimize branching on stage 2.
	 */
	if (gc.is_paused) {
		double start_time = fiber_clock();
		double timeout = gc.wal_cleanup_delay;
		while (!fiber_is_cancelled()) {
			if (fiber_yield_timeout(timeout)) {
				say_info("wal/engine cleanup is resumed "
					 "due to timeout expiration");
				gc.is_paused = false;
				gc.delay_ref = 0;
				break;
			}

			/*
			 * If a last reference is dropped
			 * we can exit out early.
			 */
			if (!gc.is_paused) {
				say_info("wal/engine cleanup is resumed");
				break;
			}

			/*
			 * Woken up to update the timeout.
			 */
			double elapsed = fiber_clock() - start_time;
			if (elapsed >= gc.wal_cleanup_delay) {
				say_info("wal/engine cleanup is resumed "
					 "due to timeout manual update");
				gc.is_paused = false;
				gc.delay_ref = 0;
				break;
			}
			timeout = gc.wal_cleanup_delay - elapsed;
		}
	}

	/*
	 * Stage 2: a regular cleanup cycle.
	 */
	while (!fiber_is_cancelled()) {
		fiber_check_gc();
		int64_t delta = gc.cleanup_scheduled - gc.cleanup_completed;
		if (delta == 0) {
			/* No pending garbage collection. */
			fiber_sleep(TIMEOUT_INFINITY);
			continue;
		}
		assert(delta > 0);
		gc_run_cleanup();
		gc.cleanup_completed += delta;
		fiber_cond_signal(&gc.cleanup_cond);
	}
	return 0;
}

void
gc_set_wal_cleanup_delay(double wal_cleanup_delay)
{
	gc.wal_cleanup_delay = wal_cleanup_delay;
	/*
	 * This routine may be called at arbitrary
	 * moment thus we must be sure the cleanup
	 * fiber is paused to not wake up it when
	 * it is already in a regular cleanup stage.
	 */
	if (gc.is_paused)
		fiber_wakeup(gc.cleanup_fiber);
}

void
gc_delay_ref(void)
{
	if (gc.is_paused) {
		assert(gc.delay_ref >= 0);
		gc.delay_ref++;
	}
}

void
gc_delay_unref(void)
{
	if (gc.is_paused) {
		assert(gc.delay_ref > 0);
		gc.delay_ref--;
		if (gc.delay_ref == 0) {
			gc.is_paused = false;
			fiber_wakeup(gc.cleanup_fiber);
		}
	}
}

/**
 * Trigger asynchronous garbage collection.
 */
static void
gc_schedule_cleanup(void)
{
	/*
	 * Do not wake up the background fiber if it's executing
	 * the garbage collection procedure right now, because
	 * it may be waiting for a cbus message, which doesn't
	 * tolerate spurious wakeups. Just increment the counter
	 * then - it will rerun garbage collection as soon as
	 * the current round completes.
	 */
	if (gc.cleanup_scheduled++ == gc.cleanup_completed)
		fiber_wakeup(gc.cleanup_fiber);
}

/**
 * Wait for background garbage collection scheduled prior
 * to this point to complete.
 */
static void
gc_wait_cleanup(void)
{
	int64_t scheduled = gc.cleanup_scheduled;
	while (gc.cleanup_completed < scheduled)
		fiber_cond_wait(&gc.cleanup_cond);
}

/**
 * Returns true iff consumer is outdated and should be deactivated.
 */
static bool
gc_consumer_is_outdated(struct gc_consumer *consumer)
{
	/*
	 * Consumer is outdated if its vclock is either less than
	 * or incomparable with the wal gc vclock.
	 */
	return vclock_compare_ignore0(&gc.vclock, &consumer->vclock) > 0;
}

void
gc_advance(const struct vclock *vclock)
{
	/*
	 * In case of emergency ENOSPC, the WAL thread may delete
	 * WAL files needed to restore from backup checkpoints,
	 * which would be kept by the garbage collector otherwise.
	 * Bring the garbage collector vclock up to date.
	 */
	vclock_copy(&gc.vclock, vclock);

	struct gc_consumer *consumer = gc_tree_first(&gc.consumers);
	while (consumer != NULL) {
		struct gc_consumer *next = gc_tree_next(&gc.consumers,
							consumer);
		/*
		 * Remove all the consumers whose vclocks are
		 * either less than or incomparable with the wal
		 * gc vclock.
		 */
		if (!gc_consumer_is_outdated(consumer)) {
			consumer = next;
			continue;
		}
		assert(!consumer->is_inactive);
		consumer->is_inactive = true;
		gc_tree_remove(&gc.consumers, consumer);

		say_crit("deactivated WAL consumer %s at %s", consumer->name,
			 vclock_to_string(&consumer->vclock));

		consumer = next;
	}
	gc_schedule_cleanup();
	gc.on_garbage_collection();
}

void
gc_set_min_checkpoint_count(int min_checkpoint_count)
{
	gc.min_checkpoint_count = min_checkpoint_count;
}

void
gc_set_checkpoint_interval(double interval)
{
	/*
	 * Reconfigure the schedule and wake up the checkpoint
	 * daemon so that it can readjust.
	 *
	 * Note, we must not wake up the checkpoint daemon fiber
	 * if it's waiting for checkpointing to complete, because
	 * checkpointing code doesn't tolerate spurious wakeups.
	 */
	checkpoint_schedule_cfg(&gc.checkpoint_schedule,
				ev_monotonic_now(loop()), interval);
	if (!gc.checkpoint_is_in_progress)
		fiber_wakeup(gc.checkpoint_fiber);
}

void
gc_add_checkpoint(const struct vclock *vclock)
{
	struct gc_checkpoint *last_checkpoint = gc_last_checkpoint();
	if (last_checkpoint != NULL &&
	    vclock_sum(&last_checkpoint->vclock) == vclock_sum(vclock)) {
		/*
		 * box.snapshot() doesn't create a new checkpoint
		 * if no rows has been written since the last one.
		 * Rerun the garbage collector in this case, just
		 * in case box.cfg.checkpoint_count has changed.
		 */
		gc_schedule_cleanup();
		return;
	}
	assert(last_checkpoint == NULL ||
	       vclock_sum(&last_checkpoint->vclock) < vclock_sum(vclock));

	struct gc_checkpoint *checkpoint = calloc(1, sizeof(*checkpoint));
	/*
	 * This function is called after a checkpoint is written
	 * to disk so it can't fail.
	 */
	if (checkpoint == NULL)
		panic("out of memory");

	rlist_create(&checkpoint->refs);
	vclock_copy(&checkpoint->vclock, vclock);
	rlist_add_tail_entry(&gc.checkpoints, checkpoint, in_checkpoints);
	gc.checkpoint_count++;

	gc_schedule_cleanup();
}

static int
gc_do_checkpoint(bool is_scheduled)
{
	int rc;
	struct wal_checkpoint checkpoint;
	int64_t limbo_rollback_count = txn_limbo.rollback_count;

	assert(!gc.checkpoint_is_in_progress);
	gc.checkpoint_is_in_progress = true;

	/*
	 * Rotate WAL and call engine callbacks to create a checkpoint
	 * on disk for each registered engine.
	 */
	rc = engine_begin_checkpoint(is_scheduled);
	if (rc != 0)
		goto out;
	rc = wal_begin_checkpoint(&checkpoint);
	if (rc != 0)
		goto out;
	/*
	 * Check if the checkpoint contains rolled back data. That makes the
	 * checkpoint not self-sufficient - it needs the xlog file with
	 * ROLLBACK. Drop it.
	 */
	if (txn_limbo.rollback_count != limbo_rollback_count) {
		rc = -1;
		diag_set(ClientError, ER_SYNC_ROLLBACK);
		goto out;
	}
	/*
	 * Wait the confirms on all "sync" transactions before
	 * create a snapshot.
	 */
	rc = txn_limbo_wait_confirm(&txn_limbo);
	if (rc != 0)
		goto out;

	rc = engine_commit_checkpoint(&checkpoint.vclock);
	if (rc != 0)
		goto out;
	wal_commit_checkpoint(&checkpoint);

	/*
	 * Finally, track the newly created checkpoint in the garbage
	 * collector state.
	 */
	gc_add_checkpoint(&checkpoint.vclock);
out:
	if (rc != 0)
		engine_abort_checkpoint();

	gc.checkpoint_is_in_progress = false;
	return rc;
}

int
gc_checkpoint(void)
{
	if (gc.checkpoint_is_in_progress) {
		diag_set(ClientError, ER_CHECKPOINT_IN_PROGRESS);
		return -1;
	}

	/*
	 * Since a user invoked a snapshot manually, this may be
	 * because he may be not happy with the current randomized
	 * schedule. Randomize the schedule again and wake up the
	 * checkpoint daemon so that it * can readjust.
	 * It is also a good idea to randomize the interval, since
	 * otherwise many instances running on the same host will
	 * no longer run their checkpoints randomly after
	 * a sweeping box.snapshot() (gh-4432).
	 */
	checkpoint_schedule_cfg(&gc.checkpoint_schedule,
				ev_monotonic_now(loop()),
				gc.checkpoint_schedule.interval);
	fiber_wakeup(gc.checkpoint_fiber);

	if (gc_do_checkpoint(false) != 0)
		return -1;

	/*
	 * Wait for background garbage collection that might
	 * have been triggered by this checkpoint to complete.
	 * Strictly speaking, it isn't necessary, but it
	 * simplifies testing. Same time if GC is paused and
	 * waiting for old XLOGs to be read by replicas the
	 * cleanup won't happen immediately after the checkpoint.
	 */
	if (!gc.is_paused)
		gc_wait_cleanup();
	return 0;
}

void
gc_trigger_checkpoint(void)
{
	if (gc.checkpoint_is_in_progress || gc.checkpoint_is_pending)
		return;

	gc.checkpoint_is_pending = true;
	checkpoint_schedule_reset(&gc.checkpoint_schedule,
				  ev_monotonic_now(loop()));
	fiber_wakeup(gc.checkpoint_fiber);
}

static int
gc_checkpoint_fiber_f(va_list ap)
{
	(void)ap;

	struct checkpoint_schedule *sched = &gc.checkpoint_schedule;
	while (!fiber_is_cancelled()) {
		fiber_check_gc();
		double timeout = checkpoint_schedule_timeout(sched,
					ev_monotonic_now(loop()));
		if (timeout > 0) {
			char buf[128];
			struct tm tm;
			time_t time = (time_t)(ev_now(loop()) + timeout);
			localtime_r(&time, &tm);
			strftime(buf, sizeof(buf), "%c", &tm);
			say_info("scheduled next checkpoint for %s", buf);
		} else {
			/* Periodic checkpointing is disabled. */
			timeout = TIMEOUT_INFINITY;
		}
		bool timed_out = fiber_yield_timeout(timeout);
		if (fiber_is_cancelled())
			break;
		if (!timed_out && !gc.checkpoint_is_pending) {
			/*
			 * The checkpoint schedule has changed or the fiber has
			 * been woken up spuriously.
			 * Reschedule the next checkpoint.
			 */
			continue;
		}
		/* Time to make the next scheduled checkpoint. */
		gc.checkpoint_is_pending = false;
		if (gc.checkpoint_is_in_progress) {
			/*
			 * Another fiber is making a checkpoint.
			 * Skip this one.
			 */
			continue;
		}
		if (gc_do_checkpoint(true) != 0)
			diag_log();
	}
	return 0;
}

void
gc_ref_checkpoint(struct gc_checkpoint *checkpoint,
		  struct gc_checkpoint_ref *ref, const char *format, ...)
{
	va_list ap;
	va_start(ap, format);
	vsnprintf(ref->name, GC_NAME_MAX, format, ap);
	va_end(ap);

	rlist_add_tail_entry(&checkpoint->refs, ref, in_refs);
}

void
gc_unref_checkpoint(struct gc_checkpoint_ref *ref)
{
	rlist_del_entry(ref, in_refs);
	gc_schedule_cleanup();
}

struct gc_consumer *
gc_consumer_register_impl(struct replica *replica, const struct vclock *vclock, const char *format, ...)
{
	struct gc_consumer *consumer = xmempool_alloc(&gc.gc_consumer_pool);

	va_list ap;
	va_start(ap, format);
	vsnprintf(consumer->name, GC_NAME_MAX, format, ap);
	va_end(ap);

	consumer->replica = replica;
	vclock_copy(&consumer->vclock, vclock);
	vclock_copy(&consumer->volatile_vclock, vclock);
	consumer->is_inactive = gc_consumer_is_outdated(consumer);
	if (!consumer->is_inactive)
		gc_tree_insert(&gc.consumers, consumer);
	rlist_add_entry(&gc.all_consumers, consumer, in_all_consumers);
	return consumer;
}

/**
 * Register a dummy consumer - it has an empty vclock and is inactive
 * right from the start (does not pin any xlogs).
 *
 * Returns a pointer to the new consumer object, never fails
 * (panics on memory allocation failure).
 */
static struct gc_consumer *
gc_consumer_register_dummy_impl(struct replica *replica)
{
	struct gc_consumer *consumer = xmempool_alloc(&gc.gc_consumer_pool);

	consumer->replica = replica;
	snprintf(consumer->name, GC_NAME_MAX, "Dummy consumer");
	vclock_create(&consumer->vclock);
	vclock_create(&consumer->volatile_vclock);
	consumer->is_inactive = true;
	rlist_add_entry(&gc.all_consumers, consumer, in_all_consumers);
	return consumer;
}

static void
gc_consumer_deactivate(struct gc_consumer *consumer)
{
	say_info("gc_consumer_deactivate");
	if (!consumer->is_inactive) {
		vclock_create(&consumer->vclock);
		vclock_create(&consumer->volatile_vclock);
		consumer->is_inactive = true;
		gc_tree_remove(&gc.consumers, consumer);
		gc_schedule_cleanup();
	}
}

static void
gc_consumer_advance_impl(struct gc_consumer *consumer, const struct vclock *vclock)
{
	say_info("gc_consumer_advance_impl");
	if (consumer->is_inactive) {
		say_info("gc_consumer_advance_impl consumer->is_inactive");
		say_info("new vclock: %s, gc: %s", vclock_to_string(vclock), vclock_to_string(&gc.vclock));
		vclock_copy(&consumer->vclock, vclock);
		vclock_copy(&consumer->volatile_vclock, vclock);
		if (!gc_consumer_is_outdated(consumer)) {
			say_info("gc_consumer_advance_impl consumer became active");
			gc_tree_insert(&gc.consumers, consumer);
			consumer->is_inactive = false;
		}
		return;
	}

	int64_t signature = vclock_sum(vclock);
	int64_t prev_signature = vclock_sum(&consumer->vclock);

	if (signature == prev_signature)
		return; /* nothing to do */
	int dir = signature > prev_signature ? 1 : -1;

	/*
	 * Do not update the tree unless the tree invariant
	 * is violated.
	 */
	struct gc_consumer *next =
		dir == 1 ? gc_tree_next(&gc.consumers, consumer) :
			   gc_tree_prev(&gc.consumers, consumer);
	bool update_tree =
		next != NULL &&
		vclock_lex_compare(vclock, &next->vclock) * dir >= 0;

	if (update_tree)
		gc_tree_remove(&gc.consumers, consumer);

	vclock_copy(&consumer->vclock, vclock);

	if (update_tree)
		gc_tree_insert(&gc.consumers, consumer);
	
	/* After all, deactivate consumer if it became outdated. */
	if (gc_consumer_is_outdated(consumer))
		gc_consumer_deactivate(consumer);

	gc_schedule_cleanup();
}

void
gc_consumer_unregister_impl(struct gc_consumer *consumer)
{
	if (!consumer->is_inactive) {
		gc_tree_remove(&gc.consumers, consumer);
		gc_schedule_cleanup();
	}
	rlist_del_entry(consumer, in_all_consumers);
	gc_consumer_delete(consumer);
}

struct gc_consumer *
gc_consumer_iterator_next(struct gc_consumer_iterator *it)
{
	if (it->curr != NULL)
		it->curr = gc_tree_next(&gc.consumers, it->curr);
	else
		it->curr = gc_tree_first(&gc.consumers);
	return it->curr;
}

/* TODO: update instead of replace under certain conditions??? */
int
gc_consumer_persist(struct gc_consumer *consumer)
{
	char tuple_buf[VCLOCK_STR_LEN_MAX + UUID_STR_LEN + 30];
	char *data = tuple_buf;
	data = mp_encode_array(data, 2);
	data = mp_encode_str0(data, tt_uuid_str(&consumer->replica->uuid));
	data = mp_encode_vclock_ignore0(data, &consumer->volatile_vclock);
	assert((size_t)(data - tuple_buf) < sizeof(tuple_buf));
	
	return box_replace(BOX_GC_CONSUMERS_ID, tuple_buf, data, NULL);
}

/* Retry timeout, in seconds. */
static double retry_timeout = 2.0;

static bool
gc_consumer_is_dirty(struct gc_consumer *consumer)
{
	return vclock_compare(&consumer->vclock, &consumer->volatile_vclock) != 0;
}

static int
gc_consumer_persist_all(void)
{
	int rc = 0;
	struct txn *txn = txn_begin();
	if (txn == NULL)
		return -1;

	struct gc_consumer *consumer = NULL;
	rlist_foreach_entry(consumer, &gc.all_consumers, in_all_consumers) {
		if (gc_consumer_is_dirty(consumer) &&
		    gc_consumer_persist(consumer) != 0)
			goto fail;
	}
	rc = txn_commit(txn);
	goto out;
fail:
	rc = -1;
	txn_abort(txn);
out:
	return rc;
}

static int
gc_consumers_persist_fiber_f(va_list ap)
{
	(void)ap;
	while (!fiber_is_cancelled()) {
		say_info("gc_consumers_persist_fiber_f: loop");
		fiber_check_gc();

		int64_t delta = gc.persist_scheduled - gc.persist_completed;
		if (delta == 0) {
			/* No pending persist requests. */
			say_info("gc_consumers_persist_fiber_f: no pending requests - sleeping");
			fiber_sleep(TIMEOUT_INFINITY);
			say_info("gc_consumers_persist_fiber_f: wakeup after signal");
			continue;
		}
		assert(delta > 0);

		int rc = gc_consumer_persist_all();
		if (rc != 0) {
			say_info("gc_consumers_persist_fiber_f: scheduling retry");
			/* Retry in the case of fail. */
			fiber_sleep(retry_timeout);
			continue;
		}
		say_info("gc_consumers_persist_fiber_f: success");
		gc.persist_completed += delta;
		continue;
	}
	return 0;
}

bool
gc_consumer_is_persistent(void)
{
	return space_by_id(BOX_GC_CONSUMERS_ID) != NULL;
}

void
gc_consumer_advance(struct gc_consumer *consumer, const struct vclock *vclock)
{	
	if (!gc_consumer_is_persistent()) {
		gc_consumer_advance_impl(consumer, vclock);
		return;
	}
	/*
	 * Advance volatile clock - it will be persisted eventually.
	 */
	vclock_copy(&consumer->volatile_vclock, vclock);
	/*
	 * Do not wake up the background fiber if it's executing
	 * the garbage collection procedure right now, because
	 * it may be waiting for a cbus message, which doesn't
	 * tolerate spurious wakeups. Just increment the counter
	 * then - it will rerun garbage collection as soon as
	 * the current round completes.
	 */
	if (gc.persist_scheduled++ == gc.persist_completed) {
		say_info("gc_consumer_advance: schedule");
		fiber_wakeup(gc.persist_fiber);
	}
}

int
gc_consumer_advance_sync(struct gc_consumer *consumer, const struct vclock *vclock)
{
	if (!gc_consumer_is_persistent()) {
		gc_consumer_advance_impl(consumer, vclock);
		return 0;
	}

	char key_buf[UUID_STR_LEN + 10];
	char *key_end = key_buf;
	key_end = mp_encode_array(key_end, 1);
	key_end = mp_encode_str0(key_end, tt_uuid_str(&consumer->replica->uuid));
	assert((unsigned long)(key_end - key_buf) < sizeof(key_buf));

	char ops_buf[VCLOCK_STR_LEN_MAX + 20];
	char *ops_end = ops_buf;
	ops_end = mp_encode_array(ops_end, 1);
	ops_end = mp_encode_array(ops_end, 3);
	ops_end = mp_encode_str0(ops_end, "=");
	ops_end = mp_encode_uint(ops_end, 1);
	ops_end = mp_encode_vclock_ignore0(ops_end, vclock);
	assert((unsigned long)(ops_end - ops_buf) < sizeof(ops_buf));

	return box_update(BOX_GC_CONSUMERS_ID, 0, key_buf, key_end,
			  ops_buf, ops_end, 0, NULL);	
}

int
gc_consumer_register_dummy_sync(struct replica *replica)
{
	if (!gc_consumer_is_persistent()) {
		if (replica->gc != NULL)
			gc_consumer_unregister_impl(replica->gc);
		replica->gc = gc_consumer_register_dummy_impl(replica);
		return 0;
	}

	return boxk(IPROTO_INSERT, BOX_GC_CONSUMERS_ID, "[%s]",
		    tt_uuid_str(&replica->uuid));
}

int
gc_consumer_unregister_sync(struct replica *replica)
{
	if (!gc_consumer_is_persistent()) {
		if (replica->gc != NULL)
			gc_consumer_unregister_impl(replica->gc);
		replica->gc = NULL;
		return 0;
	}

	return boxk(IPROTO_DELETE, BOX_GC_CONSUMERS_ID, "[%s]",
		    tt_uuid_str(&replica->uuid));
}

/** Persistent state - system on_replace trigger for _gc_consumers. */

/** GC consumer definition. */
struct gc_consumer_def {
	/** Instance UUID. */
	struct tt_uuid uuid;
	/** Instance vclock. */
	struct vclock vclock;
	/** Is set if the consumer has vclock. */
	bool has_vclock;
};

/** Mapping from tuple.opts to fields of gc_consumer_def. */
const struct opt_def gc_consumer_def_opts_reg[] = {
	OPT_END,
};

/**
 * Fill gc_consumer_def with opts from the MsgPack map.
 * Argument map can be NULL - default options are set in this case.
 */
static int
gc_consumer_def_opts_decode(struct gc_consumer_def *def, const char *map,
			    struct region *region)
{
	if (map == NULL)
		return 0;
	return opts_decode(def, gc_consumer_def_opts_reg, &map, region);
}

/** Build gc_consumer definition from a _gc_consumers' tuple. */
static struct gc_consumer_def *
gc_consumer_def_new_from_tuple(struct tuple *tuple, struct region *region)
{
	struct gc_consumer_def *def =
		xregion_alloc_object(region, typeof(*def));
	memset(def, 0, sizeof(*def));
	if (tuple_field_uuid(tuple, BOX_GC_CONSUMERS_FIELD_UUID, &def->uuid) != 0)
		return NULL;
	if (tt_uuid_is_nil(&def->uuid)) {
		diag_set(ClientError, ER_INVALID_UUID, tt_uuid_str(&def->uuid));
		return NULL;
	}
	def->has_vclock =
		!tuple_field_is_nil(tuple, BOX_GC_CONSUMERS_FIELD_VCLOCK);
	if (def->has_vclock) {
		const char *mp_vclock = tuple_field_with_type(
			tuple, BOX_GC_CONSUMERS_FIELD_VCLOCK, MP_MAP);
		if (mp_vclock == NULL)
			return NULL;
		if (mp_decode_vclock_ignore0(&mp_vclock, &def->vclock) != 0) {
			diag_set(ClientError, ER_INVALID_VCLOCK);
			return NULL;
		}
	}
	const char *opts = NULL;
	if (tuple_field(tuple, BOX_GC_CONSUMERS_FIELD_OPTS) != NULL) {
		opts = tuple_field_with_type(tuple, BOX_GC_CONSUMERS_FIELD_OPTS,
					     MP_MAP);
	}
	if (gc_consumer_def_opts_decode(def, opts, region) != 0)
		return NULL;
	return def;
}


/**
 * Data passed to transactional triggers of replace in _gc_consumers.
 */
struct gc_consumers_txn_trigger_data {
	/*
	 * Replica UUID. Is used instead of replica object because it can be
	 * unregistered before the trigger is fired.
	 */
	struct tt_uuid uuid;
	/*
	 * Saved old definition of consumer. 
	 */
	struct gc_consumer_def *old_def;
	/*
	 * Saved new definition of consumer. 
	 */
	struct gc_consumer_def *new_def;
};

static int
on_replace_dd_gc_consumers_commit(struct trigger *trigger, void *event)
{
	(void)event;
	struct gc_consumers_txn_trigger_data *data =
		(struct gc_consumers_txn_trigger_data *)trigger->data;
	struct replica *replica = replica_by_uuid(&data->uuid);
	struct gc_consumer_def *old_def = data->old_def;
	struct gc_consumer_def *new_def = data->new_def;

	/* Set new GC only if the replica is still registered. */
	if (replica == NULL)
		return 0;

	/* Unref GC delay on first non-dummy consumer. */
	if (new_def != NULL && new_def->has_vclock &&
	    (old_def == NULL || !old_def->has_vclock))
		gc_delay_unref();

	struct gc_consumer *gc = replica->gc;	
	if (old_def == NULL) {
		say_info("_gc_consumers insert");
		/* INSERT */
		if (!new_def->has_vclock) {
			if (replica->gc == NULL)
				replica->gc = gc_consumer_register_dummy_impl(replica);
		} else {
			if (replica->gc == NULL)
				replica->gc = gc_consumer_register_impl(replica, &new_def->vclock, "abc");
			else
				gc_consumer_advance_impl(replica->gc, &new_def->vclock);
		}
	} else if (new_def == NULL) {
		say_info("_gc_consumers delete");
		/* DELETE */
		if (gc == NULL)
			panic("Replica has no gc consumer on delete");
		gc_consumer_unregister_impl(gc);
		replica->gc = NULL;
	} else {
		say_info("_gc_consumers update");
		/* UPDATE */
		if (gc == NULL)
			panic("Replica has no gc consumer on update");

		say_info("on_commit - new def has vclock: %d", (int)new_def->has_vclock);
		if (new_def->has_vclock)
			gc_consumer_advance_impl(gc, &new_def->vclock);
		else
			gc_consumer_deactivate(gc);
	}
	return 0;
}

/**
 * Note that due to concurrent nature of transactions it is unsafe
 * to modify replica->gc right here and in on_rollback, so it is
 * modified only in on_commit triggers - it is safe because they
 * are called in order of transactions' serialization.
 */
int
on_replace_dd_gc_consumers(struct trigger *trigger, void *event)
{
	(void)trigger;
	int rc = 0;
	struct txn *txn = (struct txn *)event;
	struct txn_stmt *stmt = txn_current_stmt(txn);
	struct tuple *old_tuple = stmt->old_tuple;
	struct tuple *new_tuple = stmt->new_tuple;
	struct gc_consumer_def *old_def = NULL;
	struct gc_consumer_def *new_def = NULL;
	struct tt_uuid *replica_uuid = NULL;
	if (old_tuple != NULL) {
		old_def =
			gc_consumer_def_new_from_tuple(old_tuple, &in_txn()->region);
		if (old_def == NULL)
			goto fail;
		replica_uuid = &old_def->uuid;
	}
	if (new_tuple != NULL) {
		new_def =
			gc_consumer_def_new_from_tuple(new_tuple, &in_txn()->region);
		if (new_def == NULL)
			goto fail;
		replica_uuid = &new_def->uuid;
	}
	assert(old_def != NULL || new_def != NULL);

	say_info("_gc_consumers replace: %s -> %s", tuple_str(old_tuple), tuple_str(new_tuple));

	/* Just making sure that both tuples have the same uuid. */
	assert(old_def == NULL || new_def == NULL ||
	       tt_uuid_is_equal(&old_def->uuid, &new_def->uuid));

	/*
	 * We cannot rely on the fact that the replica is still registered
	 * in-memory because it can be dropped in the same transaction, and
	 * replica_hash will be updated only on commit, so read row right
	 * from the _cluster. It's important to lookup by uuid, not id,
	 * to correctly handle the case when uuid of replica is updated
	 * and id is the same.
	 */
	char key[UUID_STR_LEN + 10];
	char *key_end = key;
	key_end = mp_encode_array(key_end, 1);
	key_end = mp_encode_str0(key_end, tt_uuid_str(replica_uuid));
	assert((size_t)(key_end - key) < sizeof(key));
	struct tuple *replica_row;
	if (box_index_get(BOX_CLUSTER_ID, 1, key, key_end, &replica_row) != 0)
		goto fail;
	bool replica_is_registered = replica_row != NULL;

	if (!replica_is_registered && new_def != NULL) {
		diag_set(ClientError, ER_UNSUPPORTED, "gc_consumer",
			 "create or update without replica");
		goto fail;
	}
	if (replica_is_registered && new_def == NULL) {
		diag_set(ClientError, ER_UNSUPPORTED, "gc_consumer",
			 "delete while its replica is still registered");
		goto fail;
	}

	struct gc_consumers_txn_trigger_data *trg_data =
		xregion_alloc_object(&in_txn()->region,
				     struct gc_consumers_txn_trigger_data);
	trg_data->uuid = *replica_uuid;
	trg_data->old_def = old_def;
	trg_data->new_def = new_def;
	if (new_def != NULL)
		say_info("new def has vclock: %d", (int)new_def->has_vclock);

	/* Actual work will be done on commit. */
	struct trigger *on_commit = xregion_alloc_object(&in_txn()->region,
							 struct trigger);
	trigger_create(on_commit, on_replace_dd_gc_consumers_commit, trg_data,
		       NULL);
	txn_stmt_on_commit(stmt, on_commit);
	goto out;
fail:
	rc = -1;
out:
	return rc;
}

/**
 * Incorrect???
 */
int
on_create_dd_gc_consumers_primary_index(void)
{
	/* No-op on recovery (both local an remote). */
	if (recovery_state != FINISHED_RECOVERY)
		return 0;
	assert(in_txn() != NULL);
	struct gc_consumer *consumer = NULL;
	rlist_foreach_entry(consumer, &gc.all_consumers, in_all_consumers) {
		if (gc_consumer_persist(consumer) != 0)
			return -1;
	}
	return 0;
}
