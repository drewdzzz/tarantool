#include <cstdint>
#include "small/matras.h"

namespace internal {
	template<class Key>
	struct Cell {
		void *v;
		Key k;
		Cell() = default;
		Cell(const Key &k, void *v) : v(v), k(k) {}
		bool operator<(const Cell<Key>& other) const
		{
			return k < other.k;
		}
		bool operator==(const Cell<Key>& other) const
		{
			return k == other.k;
		}
		bool operator!=(const Cell<Key>& other) const
		{
			return k != other.k;
		}
		void
		del(void)
		{
			v = (void *)-1;
		}
		bool
		is_deleted(void)
		{
			return v == (void *)-1;
		}
	};

	template<class Key, unsigned BLOCK_SIZE>
	class ArrayTree {
	private:
		using Cell = Cell<Key>;
		static const uint64_t lvl3_size = BLOCK_SIZE / sizeof(Cell);
		static const uint64_t lvl2_size = BLOCK_SIZE / sizeof(matras_id_t);
		static const uint64_t lvl1_size = BLOCK_SIZE / sizeof(matras_id_t);
		static const uint64_t lvl23_size = lvl3_size * lvl2_size;
		static const uint64_t overall_size = lvl1_size * lvl2_size * lvl3_size;
	public:
		ArrayTree() = delete;
		ArrayTree(struct matras &matras) : matras_(matras) {
			void *ptr = matras_alloc(&matras_, &arr_);
			assert(ptr != NULL);
			memset(ptr, 0, BLOCK_SIZE);
		}
		bool
		append(const Key &k, void *v)
		{
			assert(v != (void *)-1);
			if (size == overall_size)
				return false;
			matras_id_t *ptr = (matras_id_t *)matras_touch(&matras_, arr_);
			size_t idx = size++;
			size_t idx1 = idx / lvl23_size;
			assert(idx1 < lvl1_size);
			if (ptr[idx1] == 0) {
				matras_id_t *new_ptr = (matras_id_t *)matras_alloc(&matras_, &ptr[idx1]);
				assert(new_ptr != NULL);
				memset(new_ptr, 0, BLOCK_SIZE);
				ptr = new_ptr;
			} else {
				ptr = (matras_id_t *)matras_touch(&matras_, ptr[idx1]);
			}
			size_t idx2 = idx % lvl23_size;
			idx2 /= lvl3_size;
			Cell *cell_ptr = NULL;
			if (ptr[idx2] == 0) {
				Cell *new_ptr = (Cell *)matras_alloc(&matras_, &ptr[idx2]);
				assert(new_ptr != NULL);
				memset(new_ptr, 0, BLOCK_SIZE);
				cell_ptr = new_ptr;
			} else {
				cell_ptr = (Cell *)matras_touch(&matras_, ptr[idx2]);
			}
			size_t idx3 = idx % lvl3_size;
			cell_ptr[idx3].k = k;
			cell_ptr[idx3].v = v;
			return true;
		}

		/*
		 * Lower bound in [a, b), segment is placed in one block.
		 */
		Cell *
		lower_bound_impl(const Key &k, size_t a, size_t b)
		{
			assert(a / lvl3_size == (b - 1) / lvl3_size);
			size_t idx = a;
			size_t idx1 = idx / lvl23_size;
			size_t idx2 = idx % lvl23_size;
			idx2 /= lvl3_size;
			size_t idx3 = idx % lvl3_size;
			matras_id_t *ptr = (matras_id_t *)matras_touch(&matras_, arr_);
			ptr = (matras_id_t *)matras_touch(&matras_, ptr[idx1]);
			Cell *begin = (Cell *)matras_touch(&matras_, ptr[idx2]);
			begin += idx3;
			Cell *end = begin + (b - a);
			Cell *it = std::upper_bound(begin, end, Cell(k, {}));
			if (it == begin)
				return NULL;
			--it;
			for (; it->is_deleted() && it != begin; it--) {}
			if (it->is_deleted())
				return NULL;
			return it;
		}

		/*
		 * The greatest elem which is less or equal to k.
		 * Lower bound with respect to deleted elements in [a, b).
		 */
		Cell *
		lower_bound(const Key &k, size_t a, size_t b)
		{
			assert(a < b);
			assert(b <= size);
			assert(b - a <= lvl3_size + 1);
			if (a / lvl3_size == (b - 1) / lvl3_size) {
				return lower_bound_impl(k, a, b);
			} else {
				Cell *it = lower_bound_impl(k, (b / lvl3_size) * lvl3_size, b);
				if (it == NULL)
					it = lower_bound_impl(k, a, (a / lvl3_size) * lvl3_size + lvl3_size);
				return it;
			}
		}

		Cell *
		find(const Key &k, size_t a, size_t b)
		{
			Cell *it = lower_bound(k, a, b);
			if (it != NULL && it->k != k)
				it = NULL;
			return it;
		}

		void
		del(const Key &k, size_t a, size_t b)
		{
			Cell *it = find(k, a, b);
			if (it != NULL)
				it->del();
		}

		void
		delete_checked(const Key &k, size_t a, size_t b)
		{
			Cell *it = find(k, a, b);
			assert(it != NULL && !it->is_deleted());
			if (it != NULL)
				it->del();
		}

	private:
		/** ID of root */
		matras_id_t arr_;
		size_t size = 0;
		struct matras &matras_;
	};
};