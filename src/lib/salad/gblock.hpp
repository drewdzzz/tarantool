#pragma once

#include <type_traits>
#include <vector>
#include <cstdlib>
#include <cassert>
#include <algorithm>
#include <iostream>
#include <cmath>
#include "core/fiber.h"
#include "small/matras.h"
#include "small/region.h"

#define LOW_EPS 1e-5

namespace internal {
	template<class Key>
	struct Cell {
		Key k;
		void *v;
		bool del = false;
		Cell() = default;
		Cell(const Key &k, void *v) : k(k), v(v) {}
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
	};

	template<class Key, unsigned Capacity>
	struct CellArray {
		Cell<Key> arr[Capacity];
		size_t last = 0;
		struct CellArray *next;
	};

	template<class Key, unsigned Capacity>
	struct DataHolder {
		using CellArray = CellArray<Key, Capacity>;
		using Cell = Cell<Key>;

		CellArray *begin;
		CellArray *curr;
		size_t data_size;
		void
		append(const Cell& c, matras &matras) {
			if (curr->last == Capacity) {
				uint32_t block_num;
				curr->next = (CellArray *)matras_alloc(&matras, &block_num);
				memset(curr->next, 0, sizeof(CellArray));
				curr = curr->next;
			}
			curr->arr[curr->last++] = c;
			data_size++;
		}
		bool
		empty(void)
		{
			assert(data_size != 0 || begin->last == 0);
			return data_size == 0;
		}
		DataHolder(matras &matras)
		{
			uint32_t block_num;
			begin = (CellArray *)matras_alloc(&matras, &block_num);
			memset(begin, 0, sizeof(CellArray));
			curr = begin;
			data_size = 0;
		}
		Cell &
		operator[](size_t idx) {
			assert(idx < data_size);
			CellArray *curr_block = begin;
			while (idx >= Capacity) {
				curr_block = curr_block->next;
				idx -= Capacity;
			}
			return curr_block->arr[idx];
		}
		size_t
		size(void)
		{
			return data_size;
		}

		Cell *
		lower_bound_impl(Cell *arr, const Key &k, size_t a, size_t b)
		{
			assert(a < b);
			assert(b <= Capacity);
			Cell *it = std::upper_bound(arr + a, arr + b, Cell(k, {}));
			if (it == arr + a)
				return NULL;
			--it;
			for (;it->del && it != arr + a; it--) {}
			if (it->del)
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
			assert(a < data_size);
			assert(b <= data_size);
			CellArray *curr_block = begin;
			while (a >= Capacity) {
				curr_block = curr_block->next;
				a -= Capacity;
				b -= Capacity;
			}
			if (b <= Capacity) {
				/* [a, b) is placed in one block. */
				Cell *res = lower_bound_impl(curr_block->arr, k, a, b);
				assert(res != NULL);
				return res;
			} else {
				/* [a, c) in one block and [c, b) in another one. */
				b -= Capacity;
				CellArray *next_block = curr_block->next;
				Cell *res = NULL;
				if (next_block->arr[0].k <= k)
					res = lower_bound_impl(next_block->arr, k, 0, b);
				if (res != NULL)
					return res;
				res = lower_bound_impl(curr_block->arr, k, a, Capacity);
				assert(res != NULL);
				return res;
			}
		}
	};
	template<class Key, unsigned Capacity>
	struct DataPayload {
		struct DataHolder<Key, Capacity> *data;
		size_t size = 0;
	};
};

template<class Key, unsigned EPS, unsigned DELTA, unsigned BLOCK_SIZE>
class GeometricBlock {
	static_assert(DELTA > 0U, "Delta must not be zero");
	static_assert(EPS > 0U, "Epsilon must not be zero");
	static_assert(std::is_trivially_copyable<Key>::value, "Key must be trivially copyable");
	/* TODO: check if Key is a numeric type. */

	template<class T>
	using Collection = std::vector<T>;
	using Cell = internal::Cell<Key>;

	struct Point {
		/** Just a key. */
		Key x;
		/** Position in data_ array. */
		ssize_t y;
	};

	/** Array of 2 EPS cells. */
	using CellArray = internal::CellArray<Key, 2 * EPS>;
	static_assert(sizeof(CellArray) <= BLOCK_SIZE, "Cell array per block");

	using DataHolder = internal::DataHolder<Key, 2 * EPS>;
	using DataPayload = internal::DataPayload<Key, 2 * EPS>;

	struct ExtraHolder {
		Cell extra[DELTA];
		size_t last = 0;
		size_t
		size(void)
		{
			return last;
		}
		void
		append(const Cell &c)
		{
			assert(last < DELTA);
			extra[last++] = c;
		}
		Cell &
		operator[](size_t idx) {
			assert(idx < last);
			return extra[idx];
		}
	};

public:
	GeometricBlock(struct matras &matras) : matras_(matras), data_(matras), extra_()
	{
		check_invariants();
	}
	GeometricBlock(struct matras &matras, DataHolder& data) : matras_(matras), data_(data), extra_()
	{
		check_invariants();
	}

private:

	/** 
	 * Compare slopes between a1->a2 and b1->b2:
	 * retval > 0 iff a1->a2 > b1->b2.
	 */
	int
	vec_cmp(const Point& a1, const Point& a2, const Point& b1, const Point &b2)
	{
		check_invariants();
		auto a_dy = a2.y - a1.y;
		auto a_dx = a2.x - a1.x;
		auto b_dy = b2.y - b1.y;
		auto b_dx = b2.x - b1.x;
		/* 
		 * Slope of a is less than slope of b iff
		 * a_dy / a_dx < b_dy / b_dx - we use tg angles
		 * formed by the vector and horizontal axis.
		 */
		return a_dy * b_dx - a_dx * b_dy;
	}

	/**
	 * A helper that returns predicted position in data_.
	 */
	size_t
	find_approx_pos(const Key& key)
	{
		check_invariants();
		/* Rectangle is not built yet if there is less than 2 elems. */
		if (data_.size() < 2)
			return 0;

		long double slope = 0.0;
		long double offset = 0.0;
		const auto& p0 = rectangle_[0];
		const auto& p1 = rectangle_[1];
		const auto& p2 = rectangle_[2];
		const auto& p3 = rectangle_[3];

		long double min_slope =
			static_cast<long double>(p2.y - p0.y) /
			static_cast<long double>(p2.x - p0.x);
		long double max_slope =
			static_cast<long double>(p3.y - p1.y) /
			static_cast<long double>(p3.x - p1.x);
		slope = (max_slope + min_slope) / 2.0;

		long double a = (p1.x - p0.x) * (p3.y - p1.y) -
			(p1.y - p0.y) * (p3.x - p1.x);
		long double b = (p2.x - p0.x) * (p3.y - p1.y) -
			(p2.y - p0.y) * (p3.x - p1.x);
		long double k = fabs(b) < LOW_EPS ? 0 : a / b;
		long double i_x = p0.x + k * (p2.x - p0.x);
		long double i_y = p0.y + k * (p2.y - p0.y);
		const Key &start_key = data_[0].k;
		offset = i_y - (i_x - start_key) * slope;
		long double pos = (key - start_key) * slope + offset;
		if (pos < 0)
			pos = 0;
		return pos;
	}

	/**
	 * A linear check that data_ contains a key. 
	 * NB: Use for debug only!
	 */
	bool
	data_has_key_linear(const Key& k)
	{
		check_invariants();
		for (size_t i = 0; i < data_.size(); ++i)
			if (data_[i].k == k)
				return true;
		return false;
	}

	/**
	 * A linear check of lower_bound. Pass NULL to check if there is no LB.
	 * With respect to tombstones.
	 * NB: use for debug only!
	 */
	bool
	data_is_lower_bound(const Key &k, const Key *lb)
	{
		check_invariants();
		size_t i = 0;
		for (; i < data_.size() && data_[i].k <= k; i++) {}
		if (i == 0) {
			return lb == NULL;
		}
		if (i == data_.size() && data_[i - 1].k > k) {
			return lb == NULL;
		}
		--i;
		/** Rewind to the last deleted elem. */
		for (; i > 0 && data_[i].del; --i) {}
		return (lb != NULL && data_[i].k == *lb && !data_[i].del) ||
		       (lb == NULL && data_[i].del);
	}

	/*
	 * The greatest elem which is less or equal to k.
	 * NB: with respect to tombstones.
	 */
	void
	lower_bound_impl(const Key &k, Cell **c)
	{
		check_invariants();
		*c = NULL;
		if (data_.empty() || k < data_[0].k)
			return;
		size_t approx_pos = find_approx_pos(k);
		size_t eps = EPS + bias_;
		/** We need semi-interval [a, b) of possible positions. */
		size_t a = approx_pos > eps ? approx_pos - eps : 0;
		size_t b = std::min(approx_pos + eps, data_.size() - 1) + 1;
		/** Set to data_.end() if the element is the highest. */
		if (b < a) {
			assert(data_is_lower_bound(k, NULL));
			*c = NULL;
			return;
		}
		assert(b >= a);
		assert(b - a <= 2 * eps + 1);
		*c = data_.lower_bound(k, a, b);
		assert(data_is_lower_bound(k, *c == NULL ? NULL : &((*c)->k)));
	}

	void
	find_impl(const Key &k, Cell **c)
	{
		lower_bound_impl(k, c);
		if (*c == NULL) {
			assert(!data_has_key_linear(k));
			return;
		}
		if ((*c)->k != k) {
			assert(!data_has_key_linear(k));
			*c = NULL;
			return;
		}
		assert(data_has_key_linear(k));
	}

	/**
	 * A helper that replaces a value of existing key.
	 * Returns false in the case key is not present in the index.
	 */
	bool
	try_replace(const Key& k, void *v)
	{
		check_invariants();
		Cell *cell;
		find_impl(k, &cell);
		if (cell != NULL) {
			cell->v = v;
			cell->del = false;
			return true;
		}
		return false;
	}

	/**
	 * A helper that tries to append new (k, v) to data_.
	 * Returns true iff all following conditions are met:
	 * 1. k is greater than all keys from data_,
	 * 2. appending (k, v) does not break convex hull constraints.
	 */
	bool
	try_append(const Key& k, void *v)
	{
		check_invariants();
		if (!data_.empty() && k <= data_[data_.size() - 1].k)
			return false;
		ssize_t idx = data_.size();
		Point p1{k, idx + EPS};
		Point p2{k, idx - EPS};
		if (data_.size() == 0) {
			assert(extra_.size() == 0);
			assert(upper_.size() == 0);
			assert(lower_.size() == 0);
			rectangle_[0] = p1;
			rectangle_[1] = p2;
			upper_.push_back(p1);
			lower_.push_back(p2);
			upper_start_ = 0;
			lower_start_ = 0;
			data_.append(Cell(k, v), matras_);
			return true;
		} else if (data_.size() == 1) {
			rectangle_[2] = p2;
			rectangle_[3] = p1;
			upper_.push_back(p1);
			lower_.push_back(p2);
			data_.append(Cell(k, v), matras_);
			return true;
		}

		assert(data_.size() >= 2);
		bool outside_line1 = vec_cmp(rectangle_[2], p1, rectangle_[0],
					     rectangle_[2]) < 0;
		bool outside_line2 = vec_cmp(rectangle_[3], p2, rectangle_[1],
					     rectangle_[3]) > 0;

		/*
		 * Invariant will be broken
		 * TODO: understand why
		 */
		if (outside_line1 || outside_line2)
			return false;

		data_.append(Cell(k, v), matras_);

		if (vec_cmp(rectangle_[1], p1, rectangle_[1], rectangle_[3]) < 0) {
			/* Find extreme slope. */
			auto min_i = lower_start_;
			for (size_t i = lower_start_ + 1; i < lower_.size(); ++i) {
				if (vec_cmp(p1, lower_[i], p1, lower_[min_i]) > 0)
					break;
				min_i = i;
			}
			lower_start_ = min_i;
			rectangle_[1] = lower_[min_i];
			rectangle_[3] = p1;

			/* Update upper part of convex hull. */
			std::size_t end = upper_.size();
			for (; end >= upper_start_ + 2 &&
			     vec_cmp(upper_[end - 2], p1, upper_[end - 2],
				     upper_[end - 1]) <= 0; --end)
				continue;

			upper_.resize(end);
			upper_.push_back(p1);
		}

		if (vec_cmp(rectangle_[0], p2, rectangle_[0], rectangle_[2]) > 0) {
			/* Find extreme slope. */
			auto max_i = upper_start_;
			for (size_t i = upper_start_ + 1; i < upper_.size(); ++i) {
				if (vec_cmp(p2, upper_[i], p2, upper_[max_i]) < 0)
					break;
				max_i = i;
			}
			upper_start_ = max_i;
			rectangle_[0] = upper_[max_i];
			rectangle_[2] = p2;

			/* Update upper part of convex hull. */
			std::size_t end = lower_.size();
			for (; end >= lower_start_ + 2 &&
			     vec_cmp(lower_[end - 2], p2, lower_[end - 2],
				     lower_[end - 1]) >= 0; --end)
				continue;

			lower_.resize(end);
			lower_.push_back(p2);
		}
		return true;
	}

	/**
	 * A helper that tries to replace in extra_.
	 */
	bool
	try_replace_extra(const Key& k, void *v)
	{
		check_invariants();
		for (size_t i = 0; i < extra_.size(); ++i) {
			if (extra_[i].k == k) {
				extra_[i].v = v;
				extra_[i].del = false;
				return true;
			}
		}
		return false;
	}

	/**
	 * A helper that tries to append to extra_.
	 */
	bool
	try_append_extra(const Key& k, void *v)
	{
		check_invariants();
		if (bias_ < DELTA) {
			bias_++;
			extra_.append(Cell(k, v));
			return true;
		}
		return false;
	}

	Cell &
	origin_cell()
	{
		check_invariants();
		assert(!data_.empty());
		Cell *c = NULL;
		size_t i = 0;
		for (i = 0; i < data_.size() && data_[i].del; ++i) {}
		assert(i != data_.size());
		c = &data_[i];
		for (size_t i = 0; i < extra_.last; ++i) {
			Cell &curr = extra_.extra[i];
			if (!curr.del && curr.k < c->k)
				c = &curr;
		}
		return *c;
	}

public:
	/**
	 * Insert (k, v) into gblock (or replace, if the key was inserted before).
	 * If we cannot safely append it, and extra is overflowed, gblock is
	 * shattered to smaller gblocks.
	 */
	DataPayload *
	insert(const Key& k, void *v)
	{
		check_invariants();
		if (try_replace(k ,v)) {
			check_invariants();
			return NULL;
		}
		if (try_replace_extra(k, v)) {
			check_invariants();
			return NULL;
		}
		if (try_append(k, v)) {
			check_invariants();
			return NULL;
		}
		if (try_append_extra(k, v)) {
			check_invariants();
			return NULL;
		}
		check_invariants();
		
		/* Append key to extra not to miss it. */
		size_t extra_len = extra_.last;
		Cell extra[DELTA + 1];
		memcpy(extra, extra_.extra, extra_len * sizeof(Cell));
		extra[extra_len++] = Cell(k, v);
		/*
		 * This node has fallen apart so must not be used anymore.
		 * Set debug flag for this.
		 */
		is_dead_ = true;
		std::sort(extra, extra + extra_len);
		Collection<GeometricBlock<Key, EPS, DELTA, BLOCK_SIZE> *> rebuilt;
		rebuilt.emplace_back(new GeometricBlock<Key, EPS, DELTA, BLOCK_SIZE>(matras_));
		size_t data_i = 0;
		size_t extra_i = 0;
		for (; data_i < data_.size() && extra_i < extra_len;) {
			assert(data_[data_i] != extra[extra_i]);
			bool success;
			if (data_[data_i] < extra[extra_i]) {
				if (data_[data_i].del) {
					data_i++;
					continue;
				}
				success = rebuilt.back()->try_append(data_[data_i].k, data_[data_i].v);
				if (success) {
					data_i++;
				} else {
					rebuilt.emplace_back(new GeometricBlock<Key, EPS, DELTA, BLOCK_SIZE>(matras_));
				}
			} else {
				if (extra[extra_i].del) {
					extra_i++;
					continue;
				}
				success = rebuilt.back()->try_append(extra[extra_i].k, extra[extra_i].v);
				if (success) {
					extra_i++;
				} else {
					rebuilt.emplace_back(new GeometricBlock<Key, EPS, DELTA, BLOCK_SIZE>(matras_));
				}
			}
		}
		for (; data_i < data_.size();) {
			if (data_[data_i].del) {
				data_i++;
				continue;
			}
			bool success;	
			success = rebuilt.back()->try_append(data_[data_i].k, data_[data_i].v);
			if (success) {
				data_i++;
			} else {
				rebuilt.emplace_back(new GeometricBlock<Key, EPS, DELTA, BLOCK_SIZE>(matras_));
			}
		}
		for (; extra_i < extra_len;) {
			if (extra[extra_i].del) {
				extra_i++;
				continue;
			}
			bool success;	
			success = rebuilt.back()->try_append(extra[extra_i].k, extra[extra_i].v);
			if (success) {
				extra_i++;
			} else {
				rebuilt.emplace_back(new GeometricBlock<Key, EPS, DELTA, BLOCK_SIZE>(matras_));
			}
		}
		assert(rebuilt.size() <= DELTA + 1);
		size_t alloc_size = 0;
		DataPayload *payload = region_alloc_object(
			&fiber()->gc, DataPayload,
			&alloc_size
		);
		payload->size = rebuilt.size();
		payload->data = region_alloc_array(
			&fiber()->gc, DataHolder,
			rebuilt.size(), &alloc_size
		);
		for (size_t i = 0; i < rebuilt.size(); ++i) {
			rebuilt[i]->check_invariants();
			assert(rebuilt[i]->extra_.size() == 0);
			payload->data[i] = rebuilt[i]->data_;
		}
		payload->size = rebuilt.size();
		rebuilt.clear();
		return payload;
	}

	bool
	find(const Key& k, void **v)
	{
		check_invariants();
		Cell *cell;
		find_impl(k, &cell);
		if (cell != NULL) {
			if (cell->del)
				return false;
			*v = cell->v;
			return true;
		}
		/* Try to find in extra. */
		for (size_t i = 0; i < extra_.last; ++i) {
			Cell &c = extra_.extra[i];
			if (c.k == k) {
				*v = c.v;
				return true;
			}
		}
		return false;
	}

	bool
	lower_bound(const Key &k, void **v)
	{
		check_invariants();
		Cell *cell;
		lower_bound_impl(k, &cell);
		if (cell != NULL) {
			/*
			 * We've got a maximal element which is LEQ to a given key.
			 * Let's search for a bigger element that is lower than k.
			 */
			for (size_t i = 0; i < extra_.last; ++i) {
				Cell &c = extra_.extra[i];
				if (!c.del && c.k > cell->k && c.k <= k)
					cell = &c;
			}
			*v = cell->v;
			return true;
		} else {
			/*
			 * No elements that LEQ than k in data_.
			 * Let's scan extra_.
			 */
			for (size_t i = 0; i < extra_.last; ++i) {
				Cell &c = extra_.extra[i];
				if (!c.del && c.k <= k && (cell == NULL || c.k > cell->k)) {
					cell = &c;
				}
			}
			if (cell == NULL)
				return false;
			*v = cell->v;
			return true;
		}
	}

	void
	del_impl(const Key &k, bool must_del)
	{
		check_invariants();
		Cell *cell;
		find_impl(k, &cell);
		if (cell != NULL) {
			assert(!must_del || !cell->del);
			if (!cell->del)
				bias_++;
			cell->del = true;
			return;
		}
		for (size_t i = 0; i < extra_.size(); ++i) {
			Cell &c = extra_[i];
			if (c.k == k) {
				assert(!must_del || !c.del);
				c.del = true;
				return;
			}
		}
		assert(!must_del);
		(void)must_del;
	}

	void
	del(const Key &k)
	{
		del_impl(k, false);
	}
	
	void
	del_checked(const Key &k)
	{
		del_impl(k, true);
	}

	bool
	empty()
	{
		check_invariants();
		assert(!data_.empty() || extra_.empty());
		return data_.empty();
	}

	size_t
	size()
	{
		check_invariants();
		return data_.size() + extra_.size();
	}

	/**
	 * NB: use only when block is not empty.
	 */
	const Key &
	origin_key()
	{
		check_invariants();
		Cell &c = origin_cell();
		return c.k;
	}

	/**
	 * NB: use only when block is not empty.
	 */
	void *&
	origin_value()
	{
		check_invariants();
		Cell &c = origin_cell();
		return c.v;
	}

	/**
	 * Print keys from data_ and extra_.
	 * NB: use for debug only.
	 */
	void
	print_keys()
	{
		check_invariants();
		std::cout << "Data with size " << data_.size() << ": ";
		for (size_t i = 0; i < data_.size(); ++i) {
			std::cout << data_[i].k << ", ";
		}
		std::cout << std::endl;	
		std::cout << "Extra: ";
		for (size_t i = 0; i < extra_.size(); ++i) {
			std::cout << extra_[i].k << ", ";
		}
		std::cout << std::endl;	
	}

	Collection<Key>
	get_data()
	{
		check_invariants();
		std::vector<Key> vec;
		for (size_t  i = 0; i < data_.size(); ++i) {
			int factor = 1;
			if (data_[i].del)
				factor = -1;
			vec.push_back(data_[i].k * factor);
		}
		return vec;
	}

	Collection<Key>
	get_extra()
	{
		check_invariants();
		std::vector<Key> vec;
		for (size_t  i = 0; i < extra_.size(); ++i) {
			int factor = 1;
			if (extra_[i].del)
				factor = -1;
			vec.push_back(extra_[i].k * factor);
		}
		return vec;
	}

	Collection<void *>
	get_values()
	{
		check_invariants();
		std::vector<void *> vec;
		for (size_t  i = 0; i < data_.size(); ++i) {
			if (data_[i].del)
				continue;
			vec.push_back(data_[i].v);
		}
		for (size_t  i = 0; i < extra_.size(); ++i) {
			if (extra_[i].del)
				continue;
			vec.push_back(extra_[i].v);
		}
		return vec;
	}

	void
	check_invariants()
	{
		assert(bias_ <= DELTA);
		assert(!is_dead_);
	}

	const Key &
	start_key()
	{
		assert(!data_.empty());
		/* Consiously forgot about tombstone. */
		return data_[0].k;
	}

	bool
	is_leaf()
	{
		return is_leaf_;
	}

	void
	set_leaf()
	{
		is_leaf_ = true;
	}

private:
	struct matras &matras_;
	DataHolder data_;
	ExtraHolder extra_;
	/* Number of tombstones in data + extra elements. */
	size_t bias_ = 0;
	Collection<Point> upper_;
	Collection<Point> lower_;
	size_t upper_start_;
	size_t lower_start_;
	Point rectangle_[4];
	bool is_dead_ = false;
	bool is_leaf_ = false;
};

