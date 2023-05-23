#include <iostream>
#include <salad/pgdm.hpp>
#include <time.h>
#include <map>
#include <set>
#include <cassert>
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

#define EXTENT_SIZE (16 * 1024)
#define BLOCK_SIZE (512)
#define EPSILON 16

static void *
extent_alloc(void *ctx)
{
    (void)ctx;
	return malloc(EXTENT_SIZE);
}

static void
extent_free(void *ctx, void *extent)
{
    (void)ctx;
	free(extent);
}

static void
test_linear()
{
	pgdm::pgdm_map<int, EPSILON, 2, EXTENT_SIZE, BLOCK_SIZE> index(extent_alloc, extent_free, NULL, NULL);
	for (int i = 0; i < 1000; ++i) {
		index.insert(7 * i + 13, new int(i));
	}
	for (int i = 0; i < 1000; ++i) {
		void *v;
		bool found = index.find(7 * i + 13, &v);
		TEST_CHECK(found);
		TEST_CHECK_EQ(*(int *)v, i);
	}
	for (int i = 0; i < 1000; ++i) {
		index.insert(7 * i + 13, new int(2 * i));
	}
	for (int i = 0; i < 1000; ++i) {
        void *v;
		bool found = index.find(7 * i + 13, &v);
		TEST_CHECK(found);
		TEST_CHECK_EQ(*(int *)v, 2 * i);
	}
	for (int i = 0; i < 1000; ++i) {
        void *v;
		bool found = index.find(7 * i + 12, &v);
		TEST_CHECK(!found);
	}
}

static void
test_find_impl()
{
	pgdm::pgdm_map<int, EPSILON, 2, EXTENT_SIZE, BLOCK_SIZE> index(extent_alloc, extent_free, NULL, NULL);
	std::map<int, int> used;
	std::set<int> unused;
	for (int i = 0; i < 256; ++i) {
		int k = rand() % 4096;
		unused.insert(k);
	}
	for (int i = 0; i < 8192; ++i) {
		int k = rand() % 4096;
		if (unused.count(k) > 0)
			continue;
		used[k] = i;
		index.insert(k, new int(i));
	}

	for (auto &u : unused) {
		void *v;
		bool found = index.find(u, &v);
		TEST_CHECK(!found);
	}
	for (auto &p : used) {
		void *v;
		bool found = index.find(p.first, &v);
		TEST_CHECK(found);
		TEST_CHECK_EQ(*(int *)v, p.second);
	}
}

static void
test_find()
{
	int iter_num = 10;
	for (int i = 0; i < iter_num; ++i)
		test_find_impl();
}

/**
 * The purpose of this test is to make root of the index
 * have more than one child.
 */
static void
test_big_root_simple()
{
	pgdm::pgdm_map<int, EPSILON, 2, EXTENT_SIZE, BLOCK_SIZE> index(extent_alloc, extent_free, NULL, NULL);
	std::map<int, int> used;
	for (int i = 0; i < 16; ++i) {
		int k = rand() % 256;
		used[k] = i;
		index.insert(k, new int(i));
	}

	for (auto &p : used) {
		void *v;
		bool found = index.find(p.first, &v);
		TEST_CHECK(found);
		TEST_CHECK_EQ(*(int *)v, p.second);
	}
}

/**
 * The purpose of this test is to make root of the index
 * have more than one child.
 */
static void
test_big_root()
{
	pgdm::pgdm_map<int, EPSILON, 2, EXTENT_SIZE, BLOCK_SIZE> index(extent_alloc, extent_free, NULL, NULL);
	std::map<int, int> used;
	for (int i = 0; i < 512; ++i) {
		int k = rand() % 8192;
		used[k] = i;
		index.insert(k, new int(i));
	}

	for (auto &p : used) {
		void *v;
		bool found = index.find(p.first, &v);
		TEST_CHECK(found);
		TEST_CHECK_EQ(*(int *)v, p.second);
	}
}

static void
test_linear_big()
{
	pgdm::pgdm_map<int, EPSILON, 2, EXTENT_SIZE, BLOCK_SIZE> index(extent_alloc, extent_free, NULL, NULL);
	int indsize = 6e5;
	for (int i = 0; i < indsize; ++i) {
		index.insert(i, new int(i));
	}
	for (int i = 0; i < indsize; ++i) {
		void *v;
		bool found = index.find(i, &v);
		TEST_CHECK(found);
		TEST_CHECK_EQ(*(int *)v, i);
	}
}

static void
test_main()
{
	test_linear();
	test_find();
	test_big_root_simple();
	test_big_root();
	test_linear_big();
}

int
main()
{
	memory_init();
	fiber_init(fiber_c_invoke);
	auto seed = time(NULL);
	std::cout << "SEED: " << seed << std::endl;
	srand(seed);
	test_main();

	fiber_free();
	memory_free();

	plan(1);
	ok(true, "ok");
	return check_plan();
}
