#include "position.h"
#include "msgpuck.h"

/*
 * Although the structure in C is very simple, it has a more
 * complex format in MsgPack:
 * +--------+--------+--------------+========================+
 * | MP_BIN | MP_MAP | POSITION_KEY | KEY IN MP_ARRAY FORMAT |
 * +--------+--------+--------------+========================+
 * MP_BIN - needed to make the object opaque to users working
 * directly with IPROTO.
 * MP_MAP - needed for extensibility (at least, we have an idea
 * to generate a digital signature to make sure that user did
 * not modify the object).
 * All the keys of map should be unsigned integer values to
 * minimize the size of the object.
 */

/** Keys for position map. All the keys must be uint. */
enum {
	/** There must be MP_ARRAY after this key, UB otherwise. */
	POSITION_KEY,
	POSITION_MAX,
};

uint32_t
position_pack_size(struct position *pos) {
	assert(pos != NULL);

	uint32_t total = pos->key_size;
	total += mp_sizeof_uint(POSITION_KEY);
	total += mp_sizeof_map(POSITION_MAX);
	total += mp_sizeof_binl(total);
	return total;
}

void
position_pack(struct position *pos, char *buffer) {
	assert(pos != NULL);
	assert(buffer != NULL);

	uint32_t map_len = pos->key_size;
	map_len += mp_sizeof_uint(POSITION_KEY);
	map_len += mp_sizeof_map(POSITION_MAX);
	buffer = mp_encode_binl(buffer, map_len);
	buffer = mp_encode_map(buffer, POSITION_MAX);
	buffer = mp_encode_uint(buffer, POSITION_KEY);
	memcpy(buffer, pos->key, pos->key_size);
}

int
position_unpack(const char *ptr, struct position *pos) {
	const char **cptr = &ptr;
	mp_decode_binl(cptr);
	uint32_t map_len = mp_decode_map(cptr);
	pos->key = NULL;
	pos->key_size = 0;
	for (uint32_t i = 0; i < map_len; ++i) {
		uint32_t key = mp_decode_uint(cptr);
		switch (key) {
		case POSITION_KEY:
			if (mp_typeof(ptr[0]) != MP_ARRAY) {
				/* diag_set */
				return -1;
			}
			pos->key = ptr;
			mp_next(cptr);
			pos->key_size = ptr - pos->key;
			break;
		default:
			/* For compatibility of versions - no-op. */
			break;
		}
	}
	return 0;
}
