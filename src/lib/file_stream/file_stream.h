#ifndef TARANTOOL_FILE_STREAM_H
#define TARANTOOL_FILE_STREAM_H
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

#include <stddef.h>
#include <stdbool.h>

struct file_stream;

/**
 * Creates a file_stream object
 * @param fd File descriptor to write
 * @param size Size of file_stream buffer
 * @return pointer to new file_stream object
 */
struct file_stream *
file_stream_new(int fd, size_t size, void *(*allocate)(size_t),
		void (*deallocate)(void *));

/**
 * Clears buffer without flushing it
 * @param fstream Pointer to file_stream object
 */
void
file_stream_reset(struct file_stream *fstream);

/**
 * Flush file_stream buffer to file
 * @param fstream Pointer to file_stream object
 * @retval 0 Success
 * @retval -1 Write to file error
 */
int
file_stream_flush(struct file_stream *fstream);

/**
 * Delete file_stream object
 * @param fstream Pointer to file_stream object
 */
void
file_stream_delete(struct file_stream *fstream);

/**
 * Write data to file_stream object
 * @param fstream Pointer to file_stream object
 * @param new_data Data to write
 * @param new_data_size Size of data to write
 * @return 0 on success, -1 on fail
 */
int
file_stream_write(struct file_stream *fstream, const void *new_data,
		  const size_t new_data_size);

/**
 * Write data to file_stream object
 * @param fstream Pointer to file_stream object
 * @param str Pointer to string to write
 * @return 0 on success, -1 on fail
 */
int
file_stream_write_string(struct file_stream *fstream, const char *str);

/**
 * Check if there is at least size free space in file_stream
 * @param fstream Pointer to file_stream object
 * @param size Size to check
 */
bool
file_stream_has(struct file_stream *fstream, size_t size);

#endif //TARANTOOL_FILE_STREAM_H
