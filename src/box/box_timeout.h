#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <time.h>
#include <stdbool.h>
#include <stdint.h>
#include <assert.h>
#include <sys/time.h>
#include <string.h>
#include <signal.h>

extern uint64_t end_time;
extern bool timer_is_up;
extern uint64_t current_time;

static const time_t YEAR_IN_SECONDS = 60 * 60 * 24 * 365;

void
box_timeout_init();

static inline void
box_timeout_sig_handler(int signum) {
	(void)signum;
	current_time++;
}

static inline bool
check_box_timeout()
{
	return current_time >= end_time;
};

static inline void
set_box_timeout(uint64_t timeout)
{
//	assert(!timer_is_up);
#ifndef NDEBUG
	timer_is_up = true;
#endif
	end_time = current_time + timeout;
}

static inline void
reset_box_timeout()
{
//	assert(timer_is_up);
#ifndef NDEBUG
	timer_is_up = false;
#endif
	end_time = end_time + YEAR_IN_SECONDS;
}

#ifdef __cplusplus
};
#endif
