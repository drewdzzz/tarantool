#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "unit.h"
#include "file_stream/file_stream.h"

static const char *TEST_FILE_NAME = "file_stream_test";
static const long BUFFER_SIZE = 4096;

void *
file_stream_allocator(size_t size)
{
	return calloc(1, size);
}

void
file_stream_deallocator(void *file_stream_ptr)
{
	free(file_stream_ptr);
}

static int
check_content(const char *expected_content, int fd)
{
	long int expected_len = strlen(expected_content);
	if (expected_len != lseek(fd, 0, SEEK_END)) {
		return -1;
	}
	lseek(fd, 0, SEEK_SET);
	if (expected_len > BUFFER_SIZE - 1) {
		return -1;
	}
	char buffer[BUFFER_SIZE];
	ssize_t readed = read(fd, buffer, BUFFER_SIZE);
	if (readed < 0) {
		return -1;
	}
	buffer[readed] = '\0';
	if (strcmp(expected_content, buffer) == 0) {
		return 0;
	}
	return -1;
}

static void
complete_write_test(const char *msg, size_t buffer_size,
		    const char *test_description)
{
	int fd = open("file_stream_test", O_TRUNC | O_RDWR | O_CREAT, 0777);
	fail_if(fd == -1);
	struct file_stream *fstream = file_stream_new(fd, buffer_size,
						      file_stream_allocator,
						      file_stream_deallocator);
	file_stream_write(fstream, msg, strlen(msg));
	file_stream_flush(fstream);
	ok(check_content(msg, fd) == 0,
	   "complete write test with buffer size = %lu and description: %s",
	   buffer_size, test_description);
	file_stream_delete(fstream);
	close(fd);
}

static void
random_parts_write_test(const char *msg, size_t buffer_size,
			size_t max_part_size, const char *test_description)
{
	int fd = open("file_stream_test", O_TRUNC | O_RDWR | O_CREAT, 0777);
	fail_if(fd == -1);
	struct file_stream *fstream = file_stream_new(fd, buffer_size,
						      file_stream_allocator,
						      file_stream_deallocator);
	size_t len = strlen(msg);
	const char *cur_pos = msg;
	while (len > 0) {
		size_t piece_size = (size_t)rand() % max_part_size + 1;
		if (piece_size > len) {
			piece_size = len;
		}
		len -= piece_size;
		file_stream_write(fstream, cur_pos, piece_size);
		cur_pos += piece_size;
	}
	file_stream_flush(fstream);
	ok(check_content(msg, fd) == 0,
	   "random sized parts write test with buffer size = %lu,"
	   " max part size = %lu and description: %s",
	   buffer_size, max_part_size, test_description);
	file_stream_delete(fstream);
	close(fd);
}

int
main()
{
	srand(time(NULL));
	plan(7);
	complete_write_test("simple test", 20, "simple");
	complete_write_test("sdgdsgsdfdsgdsg", 3, "simple with small buffer");
	complete_write_test("qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm",
			    3, "big");
	complete_write_test("qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm"
			    "qwertyuiopasdfghjklzxcvbnm",
			    1, "big with min buffer size");
	random_parts_write_test("qwertyuiopasdfghjklzxcvbnm"
				"qwertyuiopasdfghjklzxcvbnm"
				"qwertyuiopasdfghjklzxcvbnm"
				"qwertyuiopasdfghjklzxcvbnm"
				"qwertyuiopasdfghjklzxcvbnm"
				"qwertyuiopasdfghjklzxcvbnm"
				"qwertyuiopasdfghjklzxcvbnm"
				"qwertyuiopasdfghjklzxcvbnm"
				"qwertyuiopasdfghjklzxcvbnm"
				"qwertyuiopasdfghjklzxcvbnm"
				"qwertyuiopasdfghjklzxcvbnm"
				"qwertyuiopasdfghjklzxcvbnm"
				"qwertyuiopasdfghjklzxcvbnm"
				"qwertyuiopasdfghjklzxcvbnm"
				"qwertyuiopasdfghjklzxcvbnm"
				"qwertyuiopasdfghjklzxcvbnm",
				3, 25, "big by random parts");
	random_parts_write_test("Simple random parts test", 1, 10,
				"min buffer size");

	random_parts_write_test("glkdsgsldghsdfghsdfuighdsfulighdflu"
				"gdsalkgdsaulighdsuighdasuigdsaghuiasdhgdsagiu"
				"gsdagfrikdsahgfuldshguidshguidhguidsahgui"
				"MINPARTSIZEMINPARTSIZEMINPARTSIZE",
				15, 1, "min part size");
	return check_plan();
}
