// #include <iostream>
// #include <salad/pgdm.hpp>
// #include <time.h>
// #include <map>
// #include <set>

// #define UNIT_TAP_COMPATIBLE 1
// #include "unit.h"

// #define TEST_CHECK(expr) {                                                                                    \
//         if (!static_cast<bool>(expr)) {                                                                       \
//                 std::cerr << "Test faied in " << __FILE__ << " on " << __LINE__ << ", expr: " << #expr << "\n"; \
// 		abort();                                                                                      \
// 	}                                                                                                     \
// }

// #define TEST_CHECK_EQ(a, b) {                                               \
//         if (!(a == b)) {                                                    \
//                 std::cerr << "Equality test faied in " << __FILE__ << " on "  \
// 		<< __LINE__ << ", a: {" << a << "} != b: {" << b << "}\n";  \
// 		abort();                                                    \
// 	}                                                                   \
// }

// #define TEST_CHECK_NEQ(a, b) {                                                    \
//         if (a == b) {                                                          \
//                 std::cerr << "Non-equality test faied in " << __FILE__ << " on "  \
// 		<< __LINE__ << ", a: {" << a << "} == b: {" << b << "}\n";        \
// 		abort();                                                          \
// 	}                                                                         \
// }

// static void
// test_linear()
// {
// 	pgdm_map<int, int, 8, 2> index;
// 	for (int i = 0; i < 1000; ++i) {
// 		index.insert(7 * i + 13, i);
// 	}
// 	for (int i = 0; i < 1000; ++i) {
// 		int v;
// 		bool found = index.find(7 * i + 13, &v);
// 		TEST_CHECK(found);
// 		TEST_CHECK_EQ(v, i);
// 	}
// 	for (int i = 0; i < 1000; ++i) {
// 		index.insert(7 * i + 13, 2 * i);
// 	}
// 	for (int i = 0; i < 1000; ++i) {
// 		int v;
// 		bool found = index.find(7 * i + 13, &v);
// 		TEST_CHECK(found);
// 		TEST_CHECK_EQ(v, 2 * i);
// 	}
// 	for (int i = 0; i < 1000; ++i) {
// 		int v;
// 		bool found = index.find(7 * i + 12, &v);
// 		TEST_CHECK(!found);
// 	}
// }

// static void
// test_find_impl()
// {
// 	pgdm_map<int, int, 256, 8> index;
// 	std::map<int, int> used;
// 	std::set<int> unused;
// 	for (int i = 0; i < 256; ++i) {
// 		int k = rand() % 4096;
// 		unused.insert(k);
// 	}
// 	for (int i = 0; i < 8192; ++i) {
// 		int k = rand() % 4096;
// 		if (unused.count(k) > 0)
// 			continue;
// 		used[k] = i;
// 		index.insert(k, i);
// 	}

// 	for (auto &u : unused) {
// 		int v;
// 		bool found = index.find(u, &v);
// 		TEST_CHECK(!found);
// 	}
// 	for (auto &p : used) {
// 		int v;
// 		bool found = index.find(p.first, &v);
// 		TEST_CHECK(found);
// 		TEST_CHECK_EQ(v, p.second);
// 	}
// }

// static void
// test_find()
// {
// 	int iter_num = 10;
// 	for (int i = 0; i < iter_num; ++i)
// 		test_find_impl();
// }

// /**
//  * The purpose of this test is to make root of the index
//  * have more than one child.
//  */
// static void
// test_big_root_simple()
// {
// 	pgdm_map<int, int, 2, 1> index;
// 	std::map<int, int> used;
// 	for (int i = 0; i < 16; ++i) {
// 		int k = rand() % 256;
// 		used[k] = i;
// 		index.insert(k, i);
// 	}

// 	for (auto &p : used) {
// 		int v;
// 		bool found = index.find(p.first, &v);
// 		TEST_CHECK(found);
// 		TEST_CHECK_EQ(v, p.second);
// 	}
// }

// /**
//  * The purpose of this test is to make root of the index
//  * have more than one child.
//  */
// static void
// test_big_root()
// {
// 	pgdm_map<int, int, 2, 1> index;
// 	std::map<int, int> used;
// 	for (int i = 0; i < 512; ++i) {
// 		int k = rand() % 8192;
// 		used[k] = i;
// 		index.insert(k, i);
// 	}

// 	for (auto &p : used) {
// 		int v;
// 		bool found = index.find(p.first, &v);
// 		TEST_CHECK(found);
// 		TEST_CHECK_EQ(v, p.second);
// 	}
// }

// static void
// test_main()
// {
// 	test_linear();
// 	test_find();
// 	test_big_root_simple();
// 	test_big_root();
// }

// int
// main()
// {
// 	auto seed = time(NULL);
// 	std::cout << "SEED: " << seed << std::endl;
// 	srand(seed);
// 	test_main();

// 	plan(1);
// 	ok(true, "ok");
// 	return check_plan();
// }

int main() {}