#include <cstdint>
#include "small/matras.h"
namespace pgdm {
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

	template<class Key, class Cell, unsigned BLOCK_SIZE>
	class ArrayTree {
	private:
		static const uint64_t lvl3_size = BLOCK_SIZE / sizeof(Cell);
		static const uint64_t lvl2_size = BLOCK_SIZE / sizeof(matras_id_t);
		static const uint64_t lvl1_size = BLOCK_SIZE / sizeof(matras_id_t);
		static const uint64_t lvl23_size = lvl3_size * lvl2_size;
		static const uint64_t overall_size = lvl1_size * lvl2_size * lvl3_size;

		static_assert(sizeof(Cell) == 16, "Expected key size");
	private:

		Cell *
		get_impl(size_t idx)
		{
			assert(idx < size_);
			size_t idx1 = idx / lvl23_size;
			size_t idx2 = idx % lvl23_size;
			idx2 /= lvl3_size;
			size_t idx3 = idx % lvl3_size;
			matras_id_t *ptr = (matras_id_t *)matras_touch(matras_, arr_);
			ptr = (matras_id_t *)matras_touch(matras_, ptr[idx1]);
			Cell *c = (Cell *)matras_touch(matras_, ptr[idx2]);
			c += idx3;
			return c;
		}
	
		/*
		 * Lower bound in [a, b), segment is placed in one block.
		 */
		Cell *
		lower_bound_impl(const Key &k, size_t a, size_t b, bool return_deleted)
		{
			assert(a <= b);
			if (a == b)
				return NULL;
			assert(a / lvl3_size == (b - 1) / lvl3_size);
			Cell *begin = get_impl(a);
			Cell *end = begin + (b - a);
			Cell *it = std::upper_bound(begin, end, Cell(k, {}));
			if (it == begin)
				return NULL;
			--it;
			if (return_deleted)
				return it;
			for (; it->is_deleted() && it != begin; it--) {}
			if (it->is_deleted())
				return NULL;
			return it;
		}

	public:
		ArrayTree() = delete;
		ArrayTree(struct matras &matras) : matras_(&matras) {
			void *ptr = matras_alloc(matras_, &arr_);
			assert(ptr != NULL);
			memset(ptr, 0, BLOCK_SIZE);
		}

		bool
		append(Cell c)
		{
			matras_id_t *ptr = (matras_id_t *)matras_touch(matras_, arr_);
			size_t idx = size_++;
			size_t idx1 = idx / lvl23_size;
			assert(idx1 < lvl1_size);
			if (ptr[idx1] == 0) {
				matras_id_t *new_ptr = (matras_id_t *)matras_alloc(matras_, &ptr[idx1]);
				assert(new_ptr != NULL);
				memset(new_ptr, 0, BLOCK_SIZE);
				ptr = new_ptr;
			} else {
				ptr = (matras_id_t *)matras_touch(matras_, ptr[idx1]);
			}
			size_t idx2 = idx % lvl23_size;
			idx2 /= lvl3_size;
			Cell *cell_ptr = NULL;
			if (ptr[idx2] == 0) {
				Cell *new_ptr = (Cell *)matras_alloc(matras_, &ptr[idx2]);
				assert(new_ptr != NULL);
				memset(new_ptr, 0, BLOCK_SIZE);
				cell_ptr = new_ptr;
			} else {
				cell_ptr = (Cell *)matras_touch(matras_, ptr[idx2]);
			}
			size_t idx3 = idx % lvl3_size;
			cell_ptr[idx3] = c;
			return true;
		}

		bool
		append(const Key &k, void *v)
		{
			assert(v != (void *)-1);
			return append(Cell{k, v});
		}

		Cell &
		operator[](size_t idx)
		{
			Cell *c = get_impl(idx);
			return *c;
		}

		/*
		 * The greatest elem which is less or equal to k.
		 * Lower bound with respect to deleted elements in [a, b).
		 */
		Cell *
		lower_bound(const Key &k, size_t a, size_t b, bool return_deleted = false)
		{
			assert(a < b);
			assert(b <= size_);
			assert(b - a <= 2 * lvl3_size + 1);
			if (a / lvl3_size == (b - 1) / lvl3_size) {
				return lower_bound_impl(k, a, b, return_deleted);
			} else if (a / lvl3_size + 1 == (b - 1) / lvl3_size) {
				/* [a, b) is in two blocks. */
				size_t c = (a / lvl3_size + 1) * lvl3_size;
				Cell *it = lower_bound_impl(k, c, b, return_deleted);
				if (it == NULL || it->k > k)
					it = lower_bound_impl(k, a, c, return_deleted);
				return it;
			} else {
				/* [a, b) is in three blocks. */
				assert(a / lvl3_size + 2 == (b - 1) / lvl3_size);
				size_t c1 = (a / lvl3_size + 1) * lvl3_size;
				size_t c2 = c1 + lvl3_size;
				Cell *it = lower_bound_impl(k, c2, b, return_deleted);
				if (it == NULL || it->k > k) {
					it = lower_bound_impl(k, c1, c2, return_deleted);
					if (it == NULL || it->k > k)
						it = lower_bound_impl(k, a, c1, return_deleted);	
				}
				return it;
			}
		}

		/**
		 * Find cell.
		 */
		Cell *
		find(const Key &k, size_t a, size_t b)
		{
			Cell *it = lower_bound(k, a, b);
			if (it != NULL && it->k != k)
				it = NULL;
			return it;
		}

		/**
		 * Just strips prefix.
		 */
		void
		resize(size_t new_size)
		{
			assert(size_ >= new_size);
			size_ = new_size;
		}
		/** With no respect to deleted elems. */
		size_t size(void) { return size_; }
		bool empty(void) { return size_ == 0; }

	private:
		/** ID of root */
		matras_id_t arr_;
		/** With no respect to deleted elems. */
		uint32_t size_ = 0;
		struct matras *matras_;
	};
};
};