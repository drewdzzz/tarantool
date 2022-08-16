#include "unit.h"
#include "position.h"
#include "msgpuck.h"
#include "random.h"
#include "time.h"

#define KEY_BUF_LEN 50
#define POS_BUF_LEN (KEY_BUF_LEN + 20)

char key_buf[KEY_BUF_LEN];
char pos_buf[POS_BUF_LEN];

uint32_t TEST_SEED;

static void
simple_check()
{
	header();

	uint32_t size = rand() % 8 + 2;
	char *buf = key_buf;
	buf = mp_encode_array(buf, size);
	for (uint32_t i = 0; i < size; ++i) {
		uint32_t key = rand();
		buf = mp_encode_uint(buf, key);
	}
	struct position pos;
	uint32_t key_len = buf - key_buf;
	pos.key = key_buf;
	pos.key_size = key_len;
	uint32_t pack_size = position_pack_size(&pos);
	assert(POS_BUF_LEN >= pack_size);
	position_pack(&pos, pos_buf);
	int rc = position_unpack(pos_buf, &pos);
	ok(rc == 0, "Position must be unpacked");
	ok(pos.key_size == key_len && memcmp(key_buf, pos.key, key_len) == 0,
	   "Keys must match");

	/** Invalidate position and try to unpack. */
	ptrdiff_t offset = pos.key - pos_buf;
	mp_encode_map(pos_buf + offset, size);
	rc = position_unpack(pos_buf, &pos);
	ok(rc != 0, "Invalid position must not be unpacked");
	mp_encode_strl(pos_buf + offset, size);
	ok(rc != 0, "Invalid position must not be unpacked");

	footer();
}

int
main(void)
{
	plan(4);
	TEST_SEED = time(NULL);
	srand(TEST_SEED);
	simple_check();
	check_plan();
}
