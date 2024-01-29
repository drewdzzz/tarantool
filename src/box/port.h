#ifndef INCLUDES_TARANTOOL_BOX_PORT_H
#define INCLUDES_TARANTOOL_BOX_PORT_H
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
#include "trivia/util.h"
#include "small/obuf.h"
#include <port.h>
#include <stdbool.h>
#include "diag.h"
#include "error.h"
#include "tuple.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

struct tuple;

extern const struct port_vtab port_c_vtab;

/** Port implementation used for storing raw data. */
struct port_msgpack {
	const struct port_vtab *vtab;
	const char *data;
	uint32_t data_sz;
	/**
	 * Buffer for dump_plain() function. It is created during
	 * dump on demand and is deleted together with the port.
	 */
	char *plain;
	/**
	 * Context for decoding MsgPack data. Owned by port, ownership can be
	 * acquired by calling `port_get_msgpack`.
	 */
	struct mp_ctx *ctx;
};

static_assert(sizeof(struct port_msgpack) <= sizeof(struct port),
	      "sizeof(struct port_msgpack) must be <= sizeof(struct port)");

/** Initialize a port to dump raw data. */
void
port_msgpack_create_with_ctx(struct port *port, const char *data,
			     uint32_t data_sz, struct mp_ctx *ctx);

static inline void
port_msgpack_create(struct port *port, const char *data, uint32_t data_sz)
{
	port_msgpack_create_with_ctx(port, data, data_sz, NULL);
}

/** Destroy a MessagePack port. */
void
port_msgpack_destroy(struct port *base);

/**
 * Set plain text version of data in the given port. It is copied.
 */
int
port_msgpack_set_plain(struct port *base, const char *plain, uint32_t len);

/** Port for storing the result of a Lua CALL/EVAL. */
struct port_lua {
	const struct port_vtab *vtab;
	/** Lua state that stores the result. */
	struct lua_State *L;
	/** Reference to L in tarantool_L. */
	int ref;
	/** Number of entries dumped to the port. */
	int size;
};

static_assert(sizeof(struct port_lua) <= sizeof(struct port),
	      "sizeof(struct port_lua) must be <= sizeof(struct port)");

/** Initialize a port to dump from Lua. */
void
port_lua_create(struct port *port, struct lua_State *L);

/** Port implementation used with vdbe memory variables. */
struct port_vdbemem {
    const struct port_vtab *vtab;
    struct Mem *mem;
    uint32_t mem_count;
};

static_assert(sizeof(struct port_vdbemem) <= sizeof(struct port),
	      "sizeof(struct port_vdbemem) must be <= sizeof(struct port)");

/** Initialize a port to dump data in sql vdbe memory. */
void
port_vdbemem_create(struct port *base, struct Mem *mem,
		    uint32_t mem_count);

struct port_c_entry {
	struct port_c_entry *next;
	union {
		/** Valid if mp_size is 0. */
		struct tuple *tuple;
		/**
		 * Valid if mp_size is > 0. MessagePack is
		 * allocated either on heap or on the port entry
		 * mempool, if it fits into a pool object.
		 */
		char *mp;
	};
	uint32_t mp_size;
	/**
	 * Optional format of MsgPack data (that must be MP_ARR in that case).
	 * Is NULL if format is not specified.
	 */
	struct tuple_format *mp_format;
};

/**
 * C port is used by C functions from the public API. They can
 * return tuples and arbitrary MessagePack.
 * Warning: this structure is exposed in FFI, so any change in it must be
 * replicated if FFI cdef, see schema.lua.
 */
struct port_c {
	const struct port_vtab *vtab;
	struct port_c_entry *first;
	struct port_c_entry *last;
	struct port_c_entry first_entry;
	int size;
};

static_assert(sizeof(struct port_c) <= sizeof(struct port),
	      "sizeof(struct port_c) must be <= sizeof(struct port)");

/** Create a C port object. */
void
port_c_create(struct port *base);

/** Append a tuple to the port. Tuple is referenced. */
int
port_c_add_tuple(struct port *port, struct tuple *tuple);

/** Append raw MessagePack to the port. It is copied. */
int
port_c_add_mp(struct port *port, const char *mp, const char *mp_end);

struct tuple_format;

/**
 * Append raw msgpack array to the port with given format.
 * Msgpack is copied, the format is referenced for port's lifetime.
 */
int
port_c_add_formatted_mp(struct port *port, const char *mp, const char *mp_end,
			struct tuple_format *format);

/** Append a string to the port. The string is copied as msgpack string. */
int
port_c_add_str(struct port *port, const char *str, uint32_t len);

/** Method get_msgpack for struct port_c. */
const char *
port_c_get_msgpack(struct port *base, uint32_t *size);

void
port_init(void);

void
port_free(void);

/**
 * Encodes the port's content into the msgpack array.
 * Returns 1 (amount of results) in the case of success,
 * -1 otherwise.
 */
int
port_c_dump_msgpack_wrapped(struct port *port, struct obuf *out,
			    struct mp_ctx *ctx);

/**
 * Type of value in port_light.
 */
enum port_light_value_type {
	TNT_NULL,
	TNT_DOUBLE,
	TNT_TUPLE,
	TNT_STR,
	TNT_BOOL,
	TNT_MP,
	TNT_ITER,
};

/**
 * Type of function that can be used as an iterator_next method in port_light.
 * The state of an iterator is passed as the first argument.
 * The port to yield values is passed as the second value.
 * Function must return 0 and initialize port out in the case of success.
 * In the case of error, port out must not be initialized, diag must be set
 * and -1 must be returned.
 */
typedef int
(*port_light_iterator_next_f)(void *state, struct port *out);

/**
 * Value of port_light - a variant of pre-defined types.
 */
struct port_light_cell {
	/** Type of underlying value. */
	enum port_light_value_type type;
	/** Value itself. */
	union {
		/** Floating point number. */
		double number;
		/** Tuple. Is referenced. */
		struct tuple *tuple;
		/**
		 * String. No ownership.
		 * Is not guaranteed to be zero-terminated.
		 */
		struct {
			/** String itself. */
			const char *data;
			/** Length of string. */
			size_t len;
		} str;
		/** Boolean value. */
		bool boolean;
		/** MsgPack packet. No ownership. */
		struct {
			/** Start of packet. */
			const char *data;
			/** End of packet. */
			const char *data_end;
		} mp;
		/** Iterator. */
		struct {
			/** State of iterator. */
			void *state;
			/** Pointer to iterator_next function. */
			port_light_iterator_next_f next;
		} iter;
	} value;
};

enum {
	PORT_LIGHT_CAPACITY = 6,
};

/**
 * Lightweight port with limited capacity.
 * Can be dumped as many times as you need.
 * Does not own objects, strings and MsgPack packets are not copied.
 * However, tuples are referenced.
 */
struct port_light {
	/** Virtual table pointer. */
	const struct port_vtab *vtab;
	/** Pointer to array with data, has limited capacity. */
	struct port_light_cell *data;
	/** Number of values in data. */
	uint32_t size;
};

static_assert(sizeof(struct port_light) <= sizeof(struct port),
	      "sizeof(struct port_light) must be <= sizeof(struct port)");

void
port_light_create(struct port *base);

static inline void
port_light_add_null(struct port *base)
{
	struct port_light *port = (struct port_light *)base;
	int i = port->size;
	port->data[i].type = TNT_NULL;

	port->size++;
	assert(port->size <= PORT_LIGHT_CAPACITY);
}

static inline void
port_light_add_double(struct port *base, double val)
{
	struct port_light *port = (struct port_light *)base;
	int i = port->size;
	port->data[i].type = TNT_DOUBLE;
	port->data[i].value.number = val;

	port->size++;
	assert(port->size <= PORT_LIGHT_CAPACITY);
}

static inline void
port_light_add_tuple(struct port *base, struct tuple *t)
{
	struct port_light *port = (struct port_light *)base;
	int i = port->size;
	port->data[i].type = TNT_TUPLE;
	port->data[i].value.tuple = t;
	tuple_ref(t);

	port->size++;
	assert(port->size <= PORT_LIGHT_CAPACITY);
}

static inline void
port_light_add_str(struct port *base, const char *data, size_t len)
{
	struct port_light *port = (struct port_light *)base;
	int i = port->size;
	port->data[i].type = TNT_STR;
	port->data[i].value.str.data = data;
	port->data[i].value.str.len = len;

	port->size++;
	assert(port->size <= PORT_LIGHT_CAPACITY);
}

static inline void
port_light_add_str0(struct port *base, const char *data)
{
	port_light_add_str(base, data, strlen(data));
}

static inline void
port_light_add_bool(struct port *base, bool val)
{
	struct port_light *port = (struct port_light *)base;
	int i = port->size;
	port->data[i].type = TNT_BOOL;
	port->data[i].value.boolean = val;

	port->size++;
	assert(port->size <= PORT_LIGHT_CAPACITY);
}

static inline void
port_light_add_mp(struct port *base, const char *data, const char *data_end)
{
	struct port_light *port = (struct port_light *)base;
	int i = port->size;
	port->data[i].type = TNT_MP;
	port->data[i].value.mp.data = data;
	port->data[i].value.mp.data_end = data_end;

	port->size++;
	assert(port->size <= PORT_LIGHT_CAPACITY);
}

static inline void
port_light_add_iterator(struct port *base, void *state,
			port_light_iterator_next_f next)
{
	struct port_light *port = (struct port_light *)base;
	int i = port->size;
	port->data[i].type = TNT_ITER;
	port->data[i].value.iter.state = state;
	port->data[i].value.iter.next = next;

	port->size++;
	assert(port->size <= PORT_LIGHT_CAPACITY);
}

static inline bool
port_light_is_none(struct port *base, size_t idx)
{
	struct port_light *port = (struct port_light *)base;
	return idx >= port->size;
}

static inline bool
port_light_is_null(struct port *base, size_t idx)
{
	struct port_light *port = (struct port_light *)base;
	return idx < port->size && port->data[idx].type == TNT_NULL;
}

static inline bool
port_light_is_bool(struct port *base, size_t idx)
{
	struct port_light *port = (struct port_light *)base;
	return idx < port->size && port->data[idx].type == TNT_BOOL;
}

static inline void
port_light_get_bool(struct port *base, size_t idx, bool *val)
{
	assert(port_light_is_bool(base, idx));
	struct port_light *port = (struct port_light *)base;
	*val = port->data[idx].value.boolean;
}

static inline bool
port_light_is_double(struct port *base, size_t idx)
{
	struct port_light *port = (struct port_light *)base;
	return idx < port->size && port->data[idx].type == TNT_DOUBLE;
}

static inline void
port_light_get_double(struct port *base, size_t idx, double *val)
{
	assert(port_light_is_double(base, idx));
	struct port_light *port = (struct port_light *)base;
	*val = port->data[idx].value.number;
}

static inline bool
port_light_is_tuple(struct port *base, size_t idx)
{
	struct port_light *port = (struct port_light *)base;
	return idx < port->size && port->data[idx].type == TNT_TUPLE;
}

static inline void
port_light_get_tuple(struct port *base, size_t idx, struct tuple **tuple)
{
	assert(port_light_is_tuple(base, idx));
	struct port_light *port = (struct port_light *)base;
	*tuple = port->data[idx].value.tuple;
	tuple_ref(*tuple);
}

static inline bool
port_light_is_str(struct port *base, size_t idx)
{
	struct port_light *port = (struct port_light *)base;
	return idx < port->size && port->data[idx].type == TNT_STR;
}

static inline void
port_light_get_str(struct port *base, size_t idx, const char **str, size_t *len)
{
	assert(port_light_is_str(base, idx));
	struct port_light *port = (struct port_light *)base;
	*str = port->data[idx].value.str.data;
	*len = port->data[idx].value.str.len;
}

static inline bool
port_light_is_mp(struct port *base, size_t idx)
{
	struct port_light *port = (struct port_light *)base;
	return idx < port->size && port->data[idx].type == TNT_MP;
}

static inline void
port_light_get_mp(struct port *base, size_t idx, const char **data,
		  const char **data_end)
{
	assert(port_light_is_mp(base, idx));
	struct port_light *port = (struct port_light *)base;
	*data = port->data[idx].value.mp.data;
	*data_end = port->data[idx].value.mp.data_end;
}

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined __cplusplus */

#endif /* INCLUDES_TARANTOOL_BOX_PORT_H */
