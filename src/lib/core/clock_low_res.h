#pragma once

#if __cplusplus
extern "C" {
#endif

void
clock_low_res_clock_tick(int signum);

double
clock_low_res_time(void);

#if __cplusplus
}
#endif
