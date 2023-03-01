#include <iostream>
#include <salad/gblock.hpp>
#include <set>
#include <map>
#include <time.h>

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

static void
test_linear_infinity()
{
	GeometricBlock<int, int, 16, 1> block;
	for (int i = 0; i < 2048; ++i) {
		auto res = block.insert(i, i);
		TEST_CHECK_EQ(res.size(), 0);
	}
}

static void
test_linear_infinity_with_offset()
{
	GeometricBlock<int, int, 16, 1> block;
	for (int i = 0; i < 2048; ++i) {
		auto res = block.insert(i + 8144, i);
		TEST_CHECK_EQ(res.size(), 0);
	}
}

static void
test_guaranteed_capacity_impl()
{
	GeometricBlock<int, int, 256, 1> block;
	for (int i = 0; i < 256 * 2 + 1; ++i) {
		int k = rand() % 65536;
		auto res = block.insert(k, i);
		TEST_CHECK_RANGE(res.size(), 0, 1);
		if (res.size() == 1) {
			block = std::move(*res[0]);
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
	GeometricBlock<int, int, 256, 1> block;
	for (int i = 0; i < 256 * 2 + 1; ++i) {
		int k = rand() % 256;
		auto res = block.insert(k, i);
		TEST_CHECK_RANGE(res.size(), 0, 1);
		if (res.size() == 1) {
			block = std::move(*res[0]);
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
	GeometricBlock<int, int, 2, 1> block;	
	for (int i = 0; i < 16; ++i) {
		auto res = block.insert(i * 40, i);
		TEST_CHECK_EQ(res.size(), 0);
	}
	for (int i = 0; i < 16; ++i) {
		auto res = block.insert(i * 40, i + 1);
		TEST_CHECK_EQ(res.size(), 0);
	}
}

static void
test_find_linear()
{
	GeometricBlock<int, int, 16, 1> block;
	for (int i = 0; i < 256; i += 2) {
		auto res = block.insert(i, i);
	}
	int v;
	for (int i = 0; i < 256; i += 2) {
		bool found = block.find(i, &v);
		TEST_CHECK(found);
		TEST_CHECK_EQ(v, i);
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
	GeometricBlock<int, int, 2048, 32> block;
	std::map<int, int> used;
	std::set<int> unused;
	for (int i = 0; i < 256; ++i) {
		int k = rand() % 2048;
		unused.insert(k);
	}
	for (int i = 0; i < 4096; ++i) {
		int k = rand() % 2048;
		if (unused.count(k) > 0)
			continue;
		used[k] = i;
		auto res = block.insert(k, i);
		TEST_CHECK_RANGE(res.size(), 0, 1);
		if (res.size() == 1) {
			block = std::move(*res[0]);
		}
	}

	for (auto &u : unused) {
		int v;
		bool found = block.find(u, &v);
		TEST_CHECK(!found);
	}
	for (auto &p : used) {
		int v;
		bool found = block.find(p.first, &v);
		TEST_CHECK(found);
		TEST_CHECK_EQ(v, p.second);
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
	GeometricBlock<int, int, 8, 1> block;
	std::map<int, int> used;
	for (int i = 0; i < 16; ++i) {
		int k = rand() % 2048;
		used[k] = i;
		auto res = block.insert(k, i);
		TEST_CHECK_RANGE(res.size(), 0, 1);
		if (res.size() == 1)
			block = *res[0];
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
	test_main();

	plan(1);
	ok(true, "ok");
	return check_plan();
}