#include <iostream>
#include <salad/gblock.hpp>
#include <set>
#include <map>
#include <time.h>
#include "memory.h"

#define UNIT_TAP_COMPATIBLE 1
#include "unit.h"

#define TEST_CHECK(expr) {                                                                                    \
        if (!static_cast<bool>(expr)) {                                                                       \
                std::cerr << "Test faied in " << __FILE__ << " on " << __LINE__ << ", expr: " << #expr << "\n"; \
		abort();                                                                                      \
	}                                                                                                     \
}

#define TEST_CHECK_EQ(a, b) {                                               \
        if (!(a == b)) {                                                    \
                std::cerr << "Equality test faied in " << __FILE__ << " on "  \
		<< __LINE__ << ", a: {" << a << "} != b: {" << b << "}\n";  \
		abort();                                                    \
	}                                                                   \
}

#define TEST_CHECK_NEQ(a, b) {                                                    \
        if (a == b) {                                                          \
                std::cerr << "Non-equality test faied in " << __FILE__ << " on "  \
		<< __LINE__ << ", a: {" << a << "} == b: {" << b << "}\n";        \
		abort();                                                          \
	}                                                                         \
}

#define TEST_CHECK_RANGE(x, a, b) {                                                      \
        if (x < a || x > b) {                                                            \
                std::cerr << "Range test faied in " << __FILE__ << "on"                  \
		<< __LINE__ << ", x: {" << x << "} not in [" << a << ", " << b << "]\n"; \
		abort();                                                                 \
	}                                                                                \
}

#define TEST_CHECK_LE(x, a) {                                                      \
        if (x > a) {                                                            \
                std::cerr << "Range test faied in " << __FILE__ << "on"                  \
		<< __LINE__ << ", x: {" << x << "} more than {" << a << "}\n";             \
		abort();                                                                 \
	}                                                                                \
}

static int extents_count = 0;
struct matras matras;
struct matras_view view;
#define EXTENT_SIZE (32 * 1024)
#define BLOCK_SIZE (1024)

static void *
extent_alloc(void *ctx)
{
	int *p_extents_count = (int *)ctx;
	assert(p_extents_count == &extents_count);
	++*p_extents_count;
	return malloc(EXTENT_SIZE);
}

static void
extent_free(void *ctx, void *extent)
{
	int *p_extents_count = (int *)ctx;
	assert(p_extents_count == &extents_count);
	--*p_extents_count;
	free(extent);
}

static void
test_linear_infinity(void)
{
	GeometricBlock<int, 16, 1, BLOCK_SIZE> block(matras);
	for (int i = 0; i < 2048; ++i) {
		void *v = static_cast<void *>(new int(i));
		auto res = block.insert(i, v);
		TEST_CHECK_EQ(res, NULL);
	}
}

static void
test_linear_infinity_with_offset(void)
{
	GeometricBlock<int, 16, 1, BLOCK_SIZE> block(matras);
	for (int i = 0; i < 2048; ++i) {
		void *v = static_cast<void *>(new int(i));
		auto res = block.insert(i + 8144, v);
		TEST_CHECK_EQ(res, NULL);
	}
}

static void
test_guaranteed_capacity_impl(void)
{
	auto *block = new GeometricBlock<int, 16, 1, BLOCK_SIZE>(matras);
	for (int i = 0; i < 16 * 2; ++i) {
		int k = rand() % 65536;
		void *v = static_cast<void *>(new int(i));
		auto res = block->insert(k, v);
		TEST_CHECK(res == NULL || res->size == 1);
		if (res != NULL) {
			block = new GeometricBlock<int, 16, 1, BLOCK_SIZE>(matras, res->data[0]);
		}
	}
}

static void
test_guaranteed_capacity()
{
	static const int iter_num = 100;
	for (int i = 0; i < iter_num; ++i)
		test_guaranteed_capacity_impl();
}

static void
test_replaces_impl()
{
	auto *block = new GeometricBlock<int, 16, 1, BLOCK_SIZE>(matras);
	for (int i = 0; i < 16 * 2; ++i) {
		int k = rand() % 256;
		void *v = static_cast<void *>(new int(i));
		auto res = block->insert(k, v);
		TEST_CHECK(res == NULL || res->size == 1);
		if (res != NULL) {
			block = new GeometricBlock<int, 16, 1, BLOCK_SIZE>(matras, res->data[0]);
		}
	}
}

static void
test_replaces()
{
	static const int iter_num = 50;
	for (int i = 0; i < iter_num; ++i)
		test_replaces_impl();
}

static void
test_linear_replaces()
{
	GeometricBlock<int, 2, 1, BLOCK_SIZE> block(matras);	
	for (int i = 0; i < 16; ++i) {
		void *v = static_cast<void *>(new int(i));
		auto res = block.insert(i * 40, v);
		TEST_CHECK_EQ(res, NULL);
	}
	for (int i = 0; i < 16; ++i) {
		void *v = static_cast<void *>(new int(i));
		auto res = block.insert(i * 40, v);
		TEST_CHECK_EQ(res, NULL);
	}
}

static void
test_find_linear()
{
	GeometricBlock<int, 16, 1, BLOCK_SIZE> block(matras);
	for (int i = 0; i < 256; i += 2) {
		void *v = static_cast<void *>(new int(i));
		auto res = block.insert(i, v);
	}
	void *v;
	for (int i = 0; i < 256; i += 2) {
		bool found = block.find(i, &v);
		TEST_CHECK(found);
		TEST_CHECK_EQ(*(int *)v, i);
	}
	for (int i = 1; i < 256; i += 2) {
		bool found = block.find(i, &v);
		TEST_CHECK(!found);
	}
	for (int i = 256; i < 512; i++) {
		bool found = block.find(i, &v);
		TEST_CHECK(!found);
	}
}

static void
test_find_impl()
{
	auto *block = new GeometricBlock<int, 16, 2, BLOCK_SIZE>(matras);
	std::map<int, int> used;
	std::set<int> unused;
	for (int i = 0; i < 4; ++i) {
		int k = rand() % 2048;
		unused.insert(k);
	}
	for (int i = 0; i < 32; ++i) {
		int k = rand() % 2048;
		if (unused.count(k) > 0)
			continue;
		used[k] = i;
		void *v = new int(i);
		auto res = block->insert(k, v);
		TEST_CHECK(res == NULL || res->size == 1);
		if (res != NULL) {
			block = new GeometricBlock<int, 16, 2, BLOCK_SIZE>(matras, res->data[0]);
		}
	}

	for (auto &u : unused) {
		void *v;
		bool found = block->find(u, &v);
		TEST_CHECK(!found);
	}
	for (auto &p : used) {
		void *v;
		bool found = block->find(p.first, &v);
		TEST_CHECK(found);
		TEST_CHECK_EQ(*(int *)v, p.second);
	}
}

static void
test_find()
{
	int iter_num = 50;
	for (int i = 0; i < iter_num; ++i)
		test_find_impl();
}

static void
test_sparse()
{
	auto *block = new GeometricBlock<int, 8, 1, BLOCK_SIZE>(matras);
	std::map<int, int> used;
	for (int i = 0; i < 16; ++i) {
		int k = rand() % 2048;
		used[k] = i;
		void *v = new int(i);
		auto res = block->insert(k, v);
		TEST_CHECK(res == NULL || res->size == 1);
		if (res != NULL) {
			block = new GeometricBlock<int, 8, 1, BLOCK_SIZE>(matras, res->data[0]);
		}
	}
}

static void
test_main()
{
	test_linear_replaces();
	test_linear_infinity();
	test_linear_infinity_with_offset();
	test_guaranteed_capacity();
	test_replaces();
	test_find_linear();
	test_find();
	test_sparse();
}

int
main()
{
	srand(time(NULL));
	memory_init();
	fiber_init(fiber_c_invoke);
	matras_create(
		&matras, EXTENT_SIZE, BLOCK_SIZE, extent_alloc, extent_free,
		&extents_count, NULL);
	// matras_head_read_view(&view);

	test_main();

	fiber_free();
	memory_free();
	plan(1);
	ok(true, "ok");
	return check_plan();
}