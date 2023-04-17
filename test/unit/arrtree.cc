#include <iostream>
#include <salad/arrtree.hpp>
#include <set>
#include <map>
#include <time.h>
#include "memory.h"
#include "fiber.h"

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
#define EXTENT_SIZE (16 * 1024)
#define BLOCK_SIZE (512)

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
test_basic()
{
	internal::ArrayTree<long long, BLOCK_SIZE> arr(matras);
	for (int i = 0; i < 16; i += 2)
		arr.append(i, NULL);
	for (int i = 16; i < 1024 * 512; i += 2) {
		arr.append(i, NULL);
		int k = i - 2;
		size_t a = i / 2 - 8;
		size_t b = i / 2;
		internal::Cell<long long> *it = arr.find(k, a, b);
		TEST_CHECK_NEQ(it, NULL);
		TEST_CHECK_EQ(it->k, i - 2);
		k = i - 1;
		it = arr.find(k, a, b);
		TEST_CHECK_EQ(it, NULL);
	}
}

static void
test_main()
{
	test_basic();
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