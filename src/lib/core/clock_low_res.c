
static double low_res_clock = 0.0;

void
clock_low_res_clock_tick(int signum)
{
	(void) signum;
	low_res_clock += 0.1;
}

double
clock_low_res_time(void)
{
	return low_res_clock;
}

