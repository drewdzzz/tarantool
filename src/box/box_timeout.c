#include "box_timeout.h"

uint64_t end_time = YEAR_IN_MICROSECONDS * 60;
bool timer_is_up = false;
uint64_t current_time = 0;

void
box_timeout_init()
{
	/** Tick every second */
	struct sigaction sa;
	struct itimerval timer;
	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = box_timeout_sig_handler;
	sigaction(SIGVTALRM, &sa, NULL);
	timer.it_interval.tv_usec = 1000;
	timer.it_interval.tv_sec = 0;
	timer.it_value.tv_usec = 1000;
	timer.it_value.tv_sec = 0;
	setitimer(ITIMER_VIRTUAL, &timer, NULL);
}