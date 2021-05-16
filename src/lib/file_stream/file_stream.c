/*
 * Copyright 2010-2018, Tarantool AUTHORS, please see AUTHORS file.
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
#include <file_stream/file_stream.h>
#include <assert.h>
#include <string.h>
#include <fio.h>
#include <fiber.h>
#include <diag.h>

struct file_stream {
	void (*deallocate)(void *);
	size_t free_space_size;
	size_t buffer_capacity;
	size_t region_used;
	int fd;
	char *data_end;
	char buffer_begin[0];
};

struct file_stream *
file_stream_new(int fd, size_t size, void *(*allocate)(size_t),
		void (*deallocate)(void *))
{
	assert(fd >= 0);
	assert(size > 0);

	size_t used = region_used(&fiber()->gc);
	struct file_stream *new_buffer = (struct file_stream *)allocate(
		sizeof(struct file_stream) + size);
	if (new_buffer == NULL) {
		diag_set(SystemError, "Cannot allocate memory with size %u",
			 sizeof(struct file_stream) + size);
		return NULL;
	}
	new_buffer->region_used = used;
	new_buffer->data_end = new_buffer->buffer_begin;
	new_buffer->buffer_capacity = size;
	new_buffer->free_space_size = size;
	new_buffer->fd = fd;
	new_buffer->deallocate = deallocate;
	return new_buffer;
}

bool
file_stream_has(struct file_stream *fstream, size_t size)
{
	return fstream->free_space_size >= size;
}

int
file_stream_append(struct file_stream *fstream, const void *new_data,
		   const size_t new_data_size)
{
	assert(fstream != NULL);
	assert(new_data != NULL);
	assert(new_data_size > 0);

	if (!file_stream_has(fstream, new_data_size)) {
		return -1;
	}
	memcpy(fstream->data_end, new_data, new_data_size);
	fstream->data_end += new_data_size;
	fstream->free_space_size -= new_data_size;

	return 0;
}

void
file_stream_reset(struct file_stream *fstream)
{
	assert(fstream != NULL);

	fstream->data_end = fstream->buffer_begin;
	fstream->free_space_size = fstream->buffer_capacity;
}

int
file_stream_flush(struct file_stream *fstream)
{
	assert(fstream != NULL);
	assert(fstream->fd >= 0);

	size_t wrote_size = fstream->buffer_capacity - fstream->free_space_size;
	int err_code = fio_writen(fstream->fd, fstream->buffer_begin,
				  wrote_size);
	file_stream_reset(fstream);
	return err_code;
}

void
file_stream_delete(struct file_stream *fstream)
{
	assert(fstream != NULL);
	assert(fstream->deallocate != NULL);

	fstream->deallocate(fstream);
	TRASH(fstream);
}

int
file_stream_write(struct file_stream *fstream, const void *new_data,
		  const size_t new_data_size)
{
	assert(fstream->fd >= 0);
	assert(fstream != NULL);
	assert(new_data != NULL);
	assert(new_data_size > 0);

	int err_code = -1;
	if (!file_stream_has(fstream, new_data_size)) {
		file_stream_flush(fstream);
	}

	if (file_stream_has(fstream, new_data_size)) {
		err_code = file_stream_append(fstream, new_data, new_data_size);
	} else {
		/**
	 * If data size is more than buffer_capacity, write data directly
	 */
		err_code = fio_writen(fstream->fd, new_data, new_data_size);
	}
	return err_code;
}

int
file_stream_write_string(struct file_stream *fstream, const char *str)
{
	assert(fstream != NULL);
	assert(str != NULL);

	return file_stream_write(fstream, str, strlen(str));
}
