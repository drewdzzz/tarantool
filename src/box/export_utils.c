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

#include <string.h>
#include <fcntl.h>
#include <assert.h>
#include <fiber.h>
#include <diag.h>
#include <space.h>
#include <schema.h>
#include <box.h>
#include <tuple.h>
#include <tuple_convert.h>
#include <export_utils.h>
#include <say.h>
#include <fk_constraint.h>
#include <ck_constraint.h>
#include <file_stream/file_stream.h>

static const int EXPORT_BUFFER_SIZE = 4096;

static const char sql_stmt_create_table[] = "CREATE TABLE";
static const char sql_stmt_insert_into[] = "INSERT INTO";
static const char sql_stmt_values[] = "VALUES";
static const char sql_stmt_primary_key[] = "PRIMARY KEY";
static const char sql_stmt_create[] = "CREATE";
static const char sql_stmt_index[] = "INDEX";
static const char sql_stmt_on[] = "ON";
static const char sql_stmt_alter_table[] = "ALTER TABLE";
static const char sql_stmt_add_foreign_key[] = "ADD FOREIGN KEY";
static const char sql_stmt_constraint[] = "CONSTRAINT";
static const char sql_stmt_check[] = "CHECK";
static const char sql_stmt_references[] = "REFERENCES";
static const char sql_stmt_not_null[] = "NOT NULL";
static const char sql_stmt_default[] = "DEFAULT";
static const char sql_stmt_unique[] = "UNIQUE";
static const char sql_stmt_auto_increment[] = "AUTOINCREMENT";
static const char SPACE[] = " ";
static const char NEW_LINE[] = "\n";
static const char COMMA[] = ",";
static const char TAB[] = "\t";
static const char SEMICOLON[] = ";";
static const char BRACKET_OPEN[] = "(";
static const char BRACKET_CLOSE[] = ")";
static const char *field_type_sql_strs[] = {
	/* [FIELD_TYPE_ANY]      = */ "",
	/* [FIELD_TYPE_UNSIGNED] = */ "UNSIGNED",
	/* [FIELD_TYPE_STRING]   = */ "TEXT",
	/* [FIELD_TYPE_NUMBER]   = */ "FLOAT",
	/* [FIELD_TYPE_DOUBLE]   = */ "FLOAT",
	/* [FIELD_TYPE_INTEGER]  = */ "INTEGER",
	/* [FIELD_TYPE_BOOLEAN]  = */ "BOOLEAN",
	/* [FIELD_TYPE_VARBINARY] = */ "VARBINARY",
	/* [FIELD_TYPE_SCALAR]   = */ "",
	/* [FIELD_TYPE_DECIMAL]  = */ "",
	/* [FIELD_TYPE_UUID]     = */ "",
	/* [FIELD_TYPE_ARRAY]    = */ "",
	/* [FIELD_TYPE_MAP]      = */ "",
};

/**
 * A function for space_foreach visitor which writes scripts
 * of creation table and its indexes to struct file_stream,
 * pointer to file_stream must be passed using void *udata
 * @param sp Space to work with
 * @param udata Must be pointer to struct file_stream
 * @return 0 on success, -1 on fail
 */
static int
space_export_create_table_and_indexes(struct space *sp, void *udata);

/**
 * A function for space_foreach visitor which writes
 * foreign keys creation scripts to struct file_stream,
 * pointer to file_stream must be passed using void *udata
 * @param sp Space to work with
 * @param udata Must be pointer to struct file_stream
 * @return 0 on success, -1 on fail
 */
static int
space_export_foreign_keys(struct space *sp, void *udata);

/**
 * A function for space_foreach visitor which writes
 * view creation scripts to struct file_stream,
 * pointer to file_stream must be passed using void *udata
 * @param sp Space to work with
 * @param udata Must be pointer to struct file_stream
 * @return 0 on success, -1 on fail
 */
static int
space_export_create_view(struct space *sp, void *udata);

/**
 * A function for space_foreach visitor which writes insert list
 * of space to struct file_stream,
 * pointer to file_stream must be passed using void *udata
 * @param sp Space to work with
 * @param udata Must be pointer to struct file_stream
 * @return 0 on success, -1 on fail
 */
static int
space_export_insert_list(struct space *sp, void *udata);

/**
 * Write scripts to create all the indexes of space
 * @param sp Space to work with
 * @param fstream File stream to write
 * @return 0 on success, -1 on fail
 */
static int
space_export_indexes(struct space *sp, struct file_stream *fstream);

/**
 * Write format of space for sql creation script
 * @param sp Space to work with
 * @param fstream File stream to write
 * @return 0 on success, -1 on fail
 */
static int
space_export_table_format(struct space *sp, struct file_stream *fstream);

/**
 * Write primary key of space
 * @param sp Space to work with
 * @param fstream File stream to write
 * @return 0 on success, -1 on fail
 */
static int
space_export_primary_key(struct space *sp, struct file_stream *fstream);

/**
 * Write check constraints of space
 * @param sp Space to work with
 * @param fstream File stream to write
 * @return 0 on success, -1 on fail
 */
static int
space_export_ck_constraints(struct space *sp, struct file_stream *fstream);

/**
 * Write fields linked with foreign key of child table or parent table,
 * depends on is_child param
 * @param sp Space to work with
 * @param fk Foreign key of space
 * @param is_child True if need to write fields of child table,
 *                 False if need to write fields of parent table
 * @param fstream File stream to write
 * @return 0 on success, -1 on fail
 */
static int
export_fk_linked_fields(struct space *sp, struct fk_constraint *fk,
			bool is_child, struct file_stream *fstream);

/**
 * Check if space is suitable for export to sql
 */
static bool
space_is_sql_exportable(struct space *sp);

/**
 * Check if space format is suitable for export to sql
 */
static bool
space_format_is_sql_compatible(struct space_def *sp_def);

void *
file_stream_allocator(size_t size)
{
	size_t used = region_used(&fiber()->gc);
	size_t *new_file_stream = region_alloc(&fiber()->gc,
					       size + sizeof(size_t));
	/*
	 * Write region_used in front of file_stream
	 */
	*new_file_stream = used;
	return (new_file_stream + 1);
	return calloc(1, size);
}

void
file_stream_deallocator(void *file_stream_ptr)
{
	size_t *ptr = file_stream_ptr;
	/*
	 * Region_used has been written in front of file_stream
	 */
	size_t used = *(ptr - 1);
	region_truncate(&fiber()->gc, used);
}

int
export_database(const char *filename)
{
	assert(filename != NULL);

	int fd = open(filename, O_WRONLY | O_TRUNC | O_CREAT, 0644);
	if (fd < 0) {
		diag_set(SystemError, "Cannot create file with error: %s",
			 strerror(errno));
		return -1;
	}

	struct file_stream *fstream = file_stream_new(fd, EXPORT_BUFFER_SIZE,
						      file_stream_allocator,
						      file_stream_deallocator);
	if (fstream == NULL)
		return -1;

	if (space_foreach(space_export_create_table_and_indexes, fstream) != 0)
		goto err;
	if (space_foreach(space_export_foreign_keys, fstream) != 0)
		goto err;
	if (space_foreach(space_export_create_view, fstream) != 0)
		goto err;
	if (space_foreach(space_export_insert_list, fstream) != 0)
		goto err;

	file_stream_flush(fstream);
	file_stream_delete(fstream);
	close(fd);
	return 0;

err:
	file_stream_delete(fstream);
	close(fd);
	remove(filename);
	return -1;
}

int
space_export_create_table_and_indexes(struct space *sp, void *udata)
{
	assert(sp != NULL);
	assert(udata != NULL);

	if (!space_is_sql_exportable(sp))
		return 0;

	struct index *pk = sp->index[0];
	assert(pk != NULL);

	int err_code = 0;

	struct file_stream *fstream = (struct file_stream *)udata;
	const char *name = sp->def->name;
	err_code = file_stream_write_string(fstream, NEW_LINE);
	if (err_code != 0)
		return err_code;
	err_code = file_stream_write_string(fstream, sql_stmt_create_table);
	if (err_code != 0)
		return err_code;
	err_code = file_stream_write_string(fstream, SPACE);
	if (err_code != 0)
		return err_code;
	err_code = file_stream_write_string(fstream, name);
	if (err_code != 0)
		return err_code;
	err_code = file_stream_write_string(fstream, SPACE);
	if (err_code != 0)
		return err_code;
	err_code = file_stream_write_string(fstream, BRACKET_OPEN);
	if (err_code != 0)
		return err_code;
	err_code = file_stream_write_string(fstream, NEW_LINE);
	if (err_code != 0)
		return err_code;
	err_code = space_export_table_format(sp, fstream);
	if (err_code != 0)
		return err_code;
	err_code = space_export_primary_key(sp, fstream);
	if (err_code != 0)
		return err_code;
	err_code = space_export_ck_constraints(sp, fstream);
	if (err_code != 0)
		return err_code;
	err_code = file_stream_write_string(fstream, NEW_LINE);
	if (err_code != 0)
		return err_code;
	err_code = file_stream_write_string(fstream, BRACKET_CLOSE);
	if (err_code != 0)
		return err_code;
	err_code = file_stream_write_string(fstream, SEMICOLON);
	if (err_code != 0)
		return err_code;
	err_code = file_stream_write_string(fstream, NEW_LINE);
	if (err_code != 0)
		return err_code;
	space_export_indexes(sp, fstream);
	say_info("Scripts of table %s creation and its indexes were written",
		 name);
	return 0;
}

int
space_export_primary_key(struct space *sp, struct file_stream *fstream)
{
	assert(sp != NULL);
	assert(fstream != NULL);
	struct index *pk = sp->index[0];
	assert(pk != NULL);

	int err_code = 0;
	err_code = file_stream_write_string(fstream, TAB);
	if (err_code != 0)
		return err_code;
	err_code = file_stream_write_string(fstream, sql_stmt_primary_key);
	if (err_code != 0)
		return err_code;
	err_code = file_stream_write_string(fstream, SPACE);
	if (err_code != 0)
		return err_code;
	err_code = file_stream_write_string(fstream, BRACKET_OPEN);
	if (err_code != 0)
		return err_code;
	size_t pk_size = pk->def->key_def->part_count;
	for (size_t i = 0; i < pk_size; ++i) {
		size_t field_idx = pk->def->key_def->parts[i].fieldno;
		assert(sp->def->field_count > field_idx);
		const char *field_name = sp->def->fields[field_idx].name;
		err_code = file_stream_write_string(fstream, field_name);
		if (err_code != 0)
			return err_code;
		if (i != pk_size - 1) {
			err_code = file_stream_write_string(fstream, COMMA);
			if (err_code != 0)
				return err_code;
			err_code = file_stream_write_string(fstream, SPACE);
			if (err_code != 0)
				return err_code;
		}
	}
	err_code = file_stream_write_string(fstream, BRACKET_CLOSE);
	if (err_code != 0)
		return err_code;
	say_info("Primary key of table %s was exported", sp->def->name);
	return 0;
}

int
space_export_ck_constraints(struct space *sp, struct file_stream *fstream)
{
	assert(sp != NULL);
	assert(fstream != NULL);

	int err_code = 0;
	struct ck_constraint *ck = NULL;
	rlist_foreach_entry (ck, &sp->ck_constraint, link) {
		if (!ck->def->is_enabled) {
			continue;
		}
		err_code = file_stream_write_string(fstream, COMMA);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, NEW_LINE);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, TAB);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream,
						    sql_stmt_constraint);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, SPACE);
		if (err_code != 0)
			return err_code;
		const char *ck_name = ck->def->name;
		err_code = file_stream_write_string(fstream, ck_name);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, SPACE);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, sql_stmt_check);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, SPACE);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, BRACKET_OPEN);
		if (err_code != 0)
			return err_code;
		const char *ck_expression = ck->def->expr_str;
		err_code = file_stream_write_string(fstream, ck_expression);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, BRACKET_CLOSE);
		if (err_code != 0)
			return err_code;
	}
	say_info("Check constraints of table %s were exported", sp->def->name);
	return 0;
}

int
space_export_table_format(struct space *sp, struct file_stream *fstream)
{
	assert(sp != NULL);
	assert(fstream != NULL);

	uint32_t field_count = sp->def->field_count;
	struct field_def *attr = sp->def->fields;
	int err_code = 0;
	for (uint32_t i = 0; i < field_count; ++i) {
		err_code = file_stream_write_string(fstream, TAB);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, attr[i].name);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, TAB);
		if (err_code != 0)
			return err_code;
		const char *field_type = field_type_sql_strs[attr[i].type];
		assert(field_type[0] != '\0');
		err_code = file_stream_write_string(fstream, field_type);
		if (err_code != 0)
			return err_code;
		if (!attr[i].is_nullable) {
			err_code = file_stream_write_string(fstream, SPACE);
			if (err_code != 0)
				return err_code;
			err_code = file_stream_write_string(fstream,
							    sql_stmt_not_null);
			if (err_code != 0)
				return err_code;
		}
		const char *sql_default_value_expr = attr[i].default_value;
		if (sql_default_value_expr != NULL) {
			err_code = file_stream_write_string(fstream, SPACE);
			if (err_code != 0)
				return err_code;
			err_code = file_stream_write_string(fstream,
							    sql_stmt_default);
			if (err_code != 0)
				return err_code;
			err_code = file_stream_write_string(fstream, SPACE);
			if (err_code != 0)
				return err_code;
			err_code = file_stream_write_string(
				fstream, sql_default_value_expr);
			if (err_code != 0)
				return err_code;
		}
		if (sp->sequence != NULL) {
			if (i == sp->sequence_fieldno) {
				err_code = file_stream_write_string(fstream,
								    SPACE);
				if (err_code != 0)
					return err_code;
				err_code = file_stream_write_string(
					fstream, sql_stmt_auto_increment);
				if (err_code != 0)
					return err_code;
			}
		}
		err_code = file_stream_write_string(fstream, COMMA);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, NEW_LINE);
		if (err_code != 0)
			return err_code;
	}
	say_info("Format of table %s was exported", sp->def->name);
	return 0;
}

int
space_export_indexes(struct space *sp, struct file_stream *fstream)
{
	assert(sp != NULL);
	assert(fstream != NULL);

	int err_code = 0;
	uint32_t index_count = sp->index_count;
	const char *sp_name = sp->def->name;
	for (uint32_t i = 1; i < index_count; ++i) {
		struct index *cur_index = sp->index[i];
		err_code = file_stream_write_string(fstream, NEW_LINE);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, sql_stmt_create);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, SPACE);
		if (err_code != 0)
			return err_code;
		if (cur_index->def->opts.is_unique) {
			err_code = file_stream_write_string(fstream,
							    sql_stmt_unique);
			if (err_code != 0)
				return err_code;
			err_code = file_stream_write_string(fstream, SPACE);
			if (err_code != 0)
				return err_code;
		}
		err_code = file_stream_write_string(fstream, sql_stmt_index);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, SPACE);
		if (err_code != 0)
			return err_code;
		const char *index_name = sp->index[i]->def->name;
		err_code = file_stream_write_string(fstream, index_name);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, NEW_LINE);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, sql_stmt_on);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, SPACE);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, sp_name);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, SPACE);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, BRACKET_OPEN);
		if (err_code != 0)
			return err_code;

		size_t index_size = cur_index->def->key_def->part_count;
		for (size_t field_num = 0; field_num < index_size;
		     ++field_num) {
			size_t field_idx = cur_index->def->key_def
						   ->parts[field_num]
						   .fieldno;
			assert(sp->def->field_count > field_idx);
			const char *field_name =
				sp->def->fields[field_idx].name;
			err_code = file_stream_write_string(fstream,
							    field_name);
			if (err_code != 0)
				return err_code;
			if (field_num != index_size - 1) {
				err_code = file_stream_write_string(fstream,
								    COMMA);
				if (err_code != 0)
					return err_code;
				err_code = file_stream_write_string(fstream,
								    SPACE);
				if (err_code != 0)
					return err_code;
			}
		}
		err_code = file_stream_write_string(fstream, BRACKET_CLOSE);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, SEMICOLON);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, NEW_LINE);
		if (err_code != 0)
			return err_code;
	}
	say_info("Indexes of table %s were exported", sp->def->name);
	return 0;
}

int
space_export_foreign_keys(struct space *sp, void *udata)
{
	assert(sp != NULL);
	assert(udata != NULL);

	if (!space_is_sql_exportable(sp))
		return 0;

	struct file_stream *fstream = (struct file_stream *)udata;
	struct fk_constraint *fk;

	rlist_foreach_entry (fk, &sp->child_fk_constraint, in_child_space) {
		assert(fk != NULL);

		if (fk->def->is_deferred) {
			say_info(
				"Foreign key %s of space %s is deferred and is not exported",
				fk->def->name, sp->def->name);
			continue;
		}
		int err_code = 0;
		err_code = file_stream_write_string(fstream, NEW_LINE);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream,
						    sql_stmt_alter_table);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, SPACE);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, sp->def->name);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, NEW_LINE);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream,
						    sql_stmt_add_foreign_key);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, SPACE);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, BRACKET_OPEN);
		if (err_code != 0)
			return err_code;
		err_code = export_fk_linked_fields(sp, fk, true, fstream);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, BRACKET_CLOSE);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, SPACE);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream,
						    sql_stmt_references);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, SPACE);
		if (err_code != 0)
			return err_code;
		struct space *parent_sp = space_by_id(fk->def->parent_id);
		err_code = file_stream_write_string(fstream,
						    parent_sp->def->name);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, BRACKET_OPEN);
		if (err_code != 0)
			return err_code;
		err_code = export_fk_linked_fields(parent_sp, fk, false,
						   fstream);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, BRACKET_CLOSE);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, SEMICOLON);
		if (err_code != 0)
			return err_code;
		err_code = file_stream_write_string(fstream, NEW_LINE);
		if (err_code != 0)
			return err_code;
	}
	say_info("Foreign keys with child table %s were written",
		 sp->def->name);
	return 0;
}

int
export_fk_linked_fields(struct space *sp, struct fk_constraint *fk,
			bool is_child, struct file_stream *fstream)
{
	int err_code = 0;
	int linked_field_idx = (is_child ? 1 : 0);
	size_t fk_field_count = fk->def->field_count;
	size_t sp_field_count = sp->def->field_count;
	for (size_t i = 0; i < fk_field_count; ++i) {
		size_t field_idx = fk->def->links[i].fields[linked_field_idx];
		assert(field_idx < sp_field_count);
		const char *field_name = sp->def->fields[field_idx].name;
		err_code = file_stream_write_string(fstream, field_name);
		if (err_code != 0)
			return err_code;
		if (i != fk_field_count - 1) {
			err_code = file_stream_write_string(fstream, COMMA);
			if (err_code != 0)
				return err_code;
			err_code = file_stream_write_string(fstream, SPACE);
			if (err_code != 0)
				return err_code;
		}
	}
	return 0;
}

int
space_export_create_view(struct space *sp, void *udata)
{
	assert(sp != NULL);
	assert(udata != NULL);
	if (!sp->def->opts.is_view) {
		return 0;
	}

	int err_code = 0;
	struct file_stream *fstream = (struct file_stream *)udata;
	assert(sp->def->opts.sql != NULL);
	err_code = file_stream_write_string(fstream, NEW_LINE);
	if (err_code != 0)
		return err_code;
	err_code = file_stream_write_string(fstream, sp->def->opts.sql);
	if (err_code != 0)
		return err_code;
	err_code = file_stream_write_string(fstream, SEMICOLON);
	if (err_code != 0)
		return err_code;
	return 0;
}

int
space_export_insert_list(struct space *sp, void *udata)
{
	assert(sp != NULL);
	assert(udata != NULL);

	if (!space_is_sql_exportable(sp))
		return 0;

	int err_code = 0;
	struct file_stream *fstream = (struct file_stream *)udata;
	struct index *pk = sp->index[0];
	struct iterator *iterator = pk->vtab->create_iterator(pk, ITER_ALL,
							      NULL, 0);
	if (iterator == NULL)
		return -1;

	size_t tuple_ptr_used = region_used(&fiber()->gc);
	struct tuple **tuple_ptr = region_alloc(&fiber()->gc,
						sizeof(struct tuple *));
	if (tuple_ptr == NULL) {
		diag_set(OutOfMemory, sizeof(struct tuple *), "calloc",
			 "export tuple_ptr");
		return -1;
	}
	for (iterator->next(iterator, tuple_ptr); *tuple_ptr != NULL;
	     iterator->next(iterator, tuple_ptr)) {
		err_code = file_stream_write_string(fstream, NEW_LINE);
		if (err_code != 0)
			goto err;
		err_code = file_stream_write_string(fstream,
						    sql_stmt_insert_into);
		if (err_code != 0)
			goto err;
		err_code = file_stream_write_string(fstream, SPACE);
		if (err_code != 0)
			goto err;
		err_code = file_stream_write_string(fstream, sp->def->name);
		if (err_code != 0)
			goto err;
		err_code = file_stream_write_string(fstream, NEW_LINE);
		if (err_code != 0)
			goto err;
		err_code = file_stream_write_string(fstream, sql_stmt_values);
		if (err_code != 0)
			goto err;
		struct tuple *tuple = *tuple_ptr;
		size_t used = region_used(&fiber()->gc);
		char *content = tuple_to_yaml(tuple);
		if (content == NULL) {
			region_truncate(&fiber()->gc, used);
			return -1;
		}
		content[0] = '(';
		content[strlen(content) - 1] = ')';
		err_code = file_stream_write_string(fstream, content);
		if (err_code != 0)
			goto err;
		content = NULL;
		region_truncate(&fiber()->gc, used);
		err_code = file_stream_write_string(fstream, SEMICOLON);
		if (err_code != 0)
			goto err;
		err_code = file_stream_write_string(fstream, NEW_LINE);
		if (err_code != 0)
			goto err;
	}

	region_truncate(&fiber()->gc, tuple_ptr_used);
	iterator_delete(iterator);
	say_info("Insert list for table %s was exported", sp->def->name);
	return 0;
err:
	region_truncate(&fiber()->gc, tuple_ptr_used);
	iterator_delete(iterator);
	return -1;
}

bool
space_is_sql_exportable(struct space *sp)
{
	assert(sp != NULL);

	if (space_is_system(sp))
		return false;
	if (space_is_temporary(sp))
		return false;
	if (sp->def->opts.is_view)
		return false;
	if (!space_format_is_sql_compatible(sp->def)) {
		say_info(
			"Space %s have not suitable data types and is not exported",
			sp->def->name);
		return false;
	}
	struct index *pk = sp->index[0];
	if (pk == NULL) {
		say_info("Space %s have no primary index and is not exported",
			 sp->def->name);
		return false;
	}
	return true;
}

bool
space_format_is_sql_compatible(struct space_def *sp_def)
{
	assert(sp_def != NULL);
	bool is_suitable = true;
	size_t field_count = sp_def->field_count;
	for (size_t i = 0; i < field_count; ++i) {
		int field_type = sp_def->fields[i].type;
		assert(field_type >= 0);
		if (field_type_sql_strs[field_type][0] == '\0') {
			is_suitable = false;
			break;
		}
	}
	return is_suitable;
}