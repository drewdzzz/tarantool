/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2022, Tarantool AUTHORS, please see AUTHORS file.
 */
#include <signal.h>
#include <string.h>
#include <say.h>
#include <pthread.h>
#include "clock.h"
#include "clock_lowres.h"
#include "fiber.h"

/**
 * Granularity in seconds.
 */

static const struct timeval LOW_RES_GRANULARITY = {
	.tv_sec = 10,
	.tv_usec = 0,
};

double low_res_monotonic_clock = 0.0;

#ifndef NDEBUG

pthread_t owner;

bool
clock_lowres_thread_is_owner(void)
{
	return cord_is_main();
}

#endif /* NDEBUG */

#include <execinfo.h>

/** A tick of low_res_clock, SIGALRM handler. */
static void
clock_monotonic_lowres_tick(int signum)
{
	(void)signum;
	if (!clock_lowres_thread_is_owner()) {
		say_error(
			"cord handled: %s, cord pthread_t: %llx, pthread_self: %llx",
			cord_name(cord()), (uint64_t)cord()->id,
			(uint64_t)pthread_self());
		void *array[10];
		size_t size;

		// get void*'s for all entries on the stack
		size = backtrace(array, 10);

		// print out all the frames to stderr
		backtrace_symbols_fd(array, size, STDERR_FILENO);
		exit(1);
	}
	assert(clock_lowres_thread_is_owner());
	low_res_monotonic_clock = clock_monotonic();
}

void
clock_lowres_signal_init(void)
{
	owner = pthread_self();
	assert(cord_is_main());
	low_res_monotonic_clock = clock_monotonic();
	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = clock_monotonic_lowres_tick;
	sa.sa_flags = SA_RESTART;
	if (sigaction(SIGALRM, &sa, NULL) == -1)
		panic_syserror("cannot set low resolution clock timer signal");

	struct itimerval timer;
	timer.it_interval = LOW_RES_GRANULARITY;
	timer.it_value = LOW_RES_GRANULARITY;
	if (setitimer(ITIMER_REAL, &timer, NULL) == -1)
		panic_syserror("cannot set low resolution clock timer");
}

void
clock_lowres_signal_reset(void)
{
	struct itimerval timer;
	memset(&timer, 0, sizeof(timer));
	if (setitimer(ITIMER_REAL, &timer, NULL) == -1)
		say_syserror("cannot reset low resolution clock timer");

	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = SIG_DFL;
	if (sigaction(SIGALRM, &sa, 0) == -1)
		say_syserror("cannot reset low resolution clock timer signal");
}
