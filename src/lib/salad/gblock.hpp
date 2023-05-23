#pragma once

#include <type_traits>
#include <cstdlib>
#include <cassert>
#include <algorithm>
#include <iostream>
#include <cmath>
#include "core/fiber.h"
#include "small/matras.h"
#include "small/region.h"
#include "salad/arrtree.hpp"

#define LOW_EPS 1e-5

namespace pgdm {
namespace internal {

template<class Key>
struct Point {
	/** Just a key. */
	Key x;
	/** Position in data_ array. */
	ssize_t y;
};

template<class Key, unsigned BLOCK_SIZE>
struct DataPayloadUnit {
	ArrayTree<Key, internal::Cell<Key>, BLOCK_SIZE> data;
	ArrayTree<Key, Point<Key>, BLOCK_SIZE> upper_;
	ArrayTree<Key, Point<Key>, BLOCK_SIZE> lower_;
	size_t upper_start_;
	size_t lower_start_;
	Point<Key> rectangle_[4];
};

template<class Key, unsigned BLOCK_SIZE>
struct DataPayload {
	DataPayloadUnit<Key, BLOCK_SIZE> *data;
	size_t size;
};

};
};

template<class Key, unsigned EPS, unsigned DELTA, unsigned BLOCK_SIZE>
class GeometricBlock {
	static_assert(DELTA > 0U, "Delta must not be zero");
	static_assert(EPS > 0U, "Epsilon must not be zero");
	static_assert(DELTA < 2 * EPS, "Extra data must fit into block");
	static_assert(std::is_trivially_copyable<Key>::value, "Key must be trivially copyable");
	/* TODO: check if Key is a numeric type. */

	static_assert(2 * EPS * sizeof(pgdm::internal::Cell<Key>) == BLOCK_SIZE, "EPS is consistent with BLOCK_SIZE");

	using Cell = pgdm::internal::Cell<Key>;
	using ArrayTree = pgdm::internal::ArrayTree<Key, Cell, BLOCK_SIZE>;
	using DataPayloadUnit = pgdm::internal::DataPayloadUnit<Key, BLOCK_SIZE>;
	using DataPayload = pgdm::internal::DataPayload<Key, BLOCK_SIZE>;
	using Point = pgdm::internal::Point<Key>;
	using Arc = pgdm::internal::ArrayTree<Key, Point, BLOCK_SIZE>;
	using Self = GeometricBlock<Key, EPS, DELTA, BLOCK_SIZE>;

	/** Array of DELTA cells. */
	struct ExtraHolder {
		Cell extra[DELTA + 1];
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
	GeometricBlock(struct matras &matras, void **gc) :
		matras_(matras), gc_(gc), data_(matras, gc_), extra_(),
		lower_(matras, gc_), upper_(matras, gc_)
	{
		check_invariants();
	}
	GeometricBlock(struct matras &matras, void **gc, DataPayloadUnit& data) :
		matras_(matras), gc_(gc), data_(data.data), extra_(),
		lower_(std::move(data.lower_)),
		upper_(std::move(data.upper_)),
		lower_start_(data.lower_start_),
		upper_start_(data.upper_start_)
	{
		for (size_t i = 0; i < 4; i++)
			rectangle_[i] = data.rectangle_[i];
	}

private:

	size_t cur_delta(void) { return bias_ + extra_.size(); };

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
	data_is_lower_bound(const Key &k, const Key *lb, bool find_deleted = false)
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
		if (!find_deleted) {
			for (; i > 0 && data_[i].is_deleted(); --i) {}
			return (lb != NULL && data_[i].k == *lb && !data_[i].is_deleted()) ||
			(lb == NULL && data_[i].is_deleted());
		} else {
			return lb != NULL && data_[i].k == *lb;
		}
	}

	/*
	 * The greatest elem which is less or equal to k.
	 * NB: with respect to tombstones.
	 */
	void
	lower_bound_impl(const Key &k, Cell **c, bool find_deleted = false)
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
		if (b <= a) {
			assert(b == data_.size());
			a = b - 1;
		}
		assert(b >= a);
		assert(b - a <= 2 * eps + 1);
		*c = data_.lower_bound(k, a, b, find_deleted);
		assert(data_is_lower_bound(k, *c == NULL ? NULL : &((*c)->k), find_deleted));
	}

	void
	find_impl(const Key &k, Cell **c)
	{
		lower_bound_impl(k, c, true);
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
			if (cell->is_deleted())
				bias_--;
			cell->v = v;
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
		if (data_.size() == ArrayTree::max_size)
			return false;
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
			upper_.append(p1);
			lower_.append(p2);
			upper_start_ = 0;
			lower_start_ = 0;
			data_.append(k, v);
			return true;
		} else if (data_.size() == 1) {
			rectangle_[2] = p2;
			rectangle_[3] = p1;
			upper_.append(p1);
			lower_.append(p2);
			data_.append(k, v);
			return true;
		}

		assert(data_.size() >= 2);
		bool outside_line1 = vec_cmp(rectangle_[2], p1, rectangle_[0],
					     rectangle_[2]) < 0;
		bool outside_line2 = vec_cmp(rectangle_[3], p2, rectangle_[1],
					     rectangle_[3]) > 0;

		/*
		 * Invariant will be broken
		 */
		if (outside_line1 || outside_line2)
			return false;

		data_.append(k, v);

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
			upper_.append(p1);
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
			lower_.append(p2);
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
		if (cur_delta() < DELTA) {
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
		for (i = 0; i < data_.size() && data_[i].is_deleted(); ++i) {}
		assert(i != data_.size());
		c = &data_[i];
		for (size_t i = 0; i < extra_.last; ++i) {
			Cell &curr = extra_.extra[i];
			if (!curr.is_deleted() && curr.k < c->k)
				c = &curr;
		}
		return *c;
	}

	DataPayload *
	fall_apart(void)
	{
		/*
		 * This node has fallen apart so must not be used anymore.
		 * Set debug flag for this.
		 */
		assert(!is_dead_);
		is_dead_ = true;
		Cell *extra = extra_.extra;
		size_t extra_len = extra_.size();
		assert(bias_ <= data_.size());
		size_t alloc_size = 0;
		DataPayload *payload = region_alloc_object(
			&fiber()->gc, DataPayload,
			&alloc_size
		);

		if (data_.size() == bias_ && extra_len == 0) {
			payload->data = NULL;
			payload->size = 0;
			return payload;
		}
		std::sort(extra, extra + extra_len);
		Self *rebuilt = (Self *)xregion_alloc_array(
			&fiber()->gc, Self, DELTA + 1, &alloc_size);
		size_t rebuilt_i = 0;
		new (&rebuilt[0]) Self(matras_, gc_);
		size_t data_i = 0;
		size_t extra_i = 0;
		for (; data_i < data_.size() && extra_i < extra_len;) {
			assert(data_[data_i] != extra[extra_i]);
			assert(rebuilt_i <= DELTA);
			bool success;
			if (data_[data_i] < extra[extra_i]) {
				if (data_[data_i].is_deleted()) {
					data_i++;
					continue;
				}
				success = rebuilt[rebuilt_i].try_append(data_[data_i].k, data_[data_i].v);
				if (success) {
					data_i++;
				} else {
					assert(!rebuilt[rebuilt_i].data_.empty());
					rebuilt_i++;
					new (&rebuilt[rebuilt_i]) Self(matras_, gc_);
				}
			} else {
				if (extra[extra_i].is_deleted()) {
					extra_i++;
					continue;
				}
				success = rebuilt[rebuilt_i].try_append(extra[extra_i].k, extra[extra_i].v);
				if (success) {
					extra_i++;
				} else {
					assert(!rebuilt[rebuilt_i].data_.empty());
					rebuilt_i++;
					new (&rebuilt[rebuilt_i]) Self(matras_, gc_);
				}
			}
		}
		for (; data_i < data_.size();) {
			if (data_[data_i].is_deleted()) {
				data_i++;
				continue;
			}
			bool success;	
			success = rebuilt[rebuilt_i].try_append(data_[data_i].k, data_[data_i].v);
			if (success) {
				data_i++;
			} else {
				assert(!rebuilt[rebuilt_i].data_.empty());
				rebuilt_i++;
				new (&rebuilt[rebuilt_i]) Self(matras_, gc_);
			}
		}
		for (; extra_i < extra_len;) {
			if (extra[extra_i].is_deleted()) {
				extra_i++;
				continue;
			}
			bool success;	
			success = rebuilt[rebuilt_i].try_append(extra[extra_i].k, extra[extra_i].v);
			if (success) {
				extra_i++;
			} else {
				assert(!rebuilt[rebuilt_i].data_.empty());
				rebuilt_i++;
				new (&rebuilt[rebuilt_i]) Self(matras_, gc_);
			}
		}
		assert(rebuilt_i <= DELTA);
		payload->size = rebuilt_i + 1;
		payload->data = region_alloc_array(
			&fiber()->gc, DataPayloadUnit,
			payload->size, &alloc_size
		);
		for (size_t i = 0; i < payload->size; ++i) {
			assert(rebuilt[i].extra_.size() == 0);
			assert(rebuilt[i].data_.size() > 0);
			DataPayloadUnit *unit = &payload->data[i];
			unit->data = rebuilt[i].data_;
			unit->lower_ = rebuilt[i].lower_;
			unit->upper_ = rebuilt[i].upper_;
			unit->lower_start_ = rebuilt[i].lower_start_;
			unit->upper_start_ = rebuilt[i].upper_start_;
			for (size_t j = 0; j < 4; ++j)
				unit->rectangle_[j] = rebuilt[i].rectangle_[j];
		}
		upper_.drop();
		lower_.drop();
		data_.drop();
		return payload;
	}

	DataPayload *
	del_impl(const Key &k, bool must_del)
	{
		check_invariants();
		Cell *cell;
		find_impl(k, &cell);
		if (cell != NULL) {
			assert(!must_del || !cell->is_deleted());
			if (!cell->is_deleted()) {
				bias_++;
				if (cur_delta() > DELTA) {
					cell->del();
					return fall_apart();
				}
			}
			cell->del();
			return NULL;
		}
		for (size_t i = 0; i < extra_.size(); ++i) {
			Cell &c = extra_[i];
			if (c.k == k) {
				assert(!c.is_deleted());
				c.del();
				if (i != extra_.size() - 1)
					std::swap(c, extra_[extra_.size() - 1]);
				extra_.last--;
				return NULL;
			}
		}
		assert(!must_del);
		(void)must_del;
		return NULL;
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
		extra_.extra[extra_.last++] = Cell(k, v);
		return fall_apart();
	}

	bool
	find(const Key& k, void **v)
	{
		check_invariants();
		Cell *cell;
		find_impl(k, &cell);
		if (cell != NULL) {
			if (cell->is_deleted()) {
				*v = NULL;
				return false;
			}
			*v = cell->v;
			return true;
		}
		/* Try to find in extra. */
		for (size_t i = 0; i < extra_.last; ++i) {
			Cell &c = extra_.extra[i];
			if (c.k == k && !c.is_deleted()) {
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
				if (!c.is_deleted() && c.k > cell->k && c.k <= k)
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
				if (!c.is_deleted() && c.k <= k && (cell == NULL || c.k > cell->k)) {
					cell = &c;
				}
			}
			if (cell == NULL)
				return false;
			*v = cell->v;
			return true;
		}
	}

	DataPayload *
	del(const Key &k)
	{
		return del_impl(k, false);
	}
	
	DataPayload *
	del_checked(const Key &k)
	{
		return del_impl(k, true);
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

	// Collection<Key>
	// get_data()
	// {
	// 	check_invariants();
	// 	std::vector<Key> vec;
	// 	for (size_t  i = 0; i < data_.size(); ++i) {
	// 		int factor = 1;
	// 		if (data_[i].is_deleted())
	// 			factor = -1;
	// 		vec.push_back(data_[i].k * factor);
	// 	}
	// 	return vec;
	// }

	// Collection<Key>
	// get_extra()
	// {
	// 	check_invariants();
	// 	std::vector<Key> vec;
	// 	for (size_t  i = 0; i < extra_.size(); ++i) {
	// 		int factor = 1;
	// 		if (extra_[i].is_deleted())
	// 			factor = -1;
	// 		vec.push_back(extra_[i].k * factor);
	// 	}
	// 	return vec;
	// }

	// Collection<void *>
	// get_values()
	// {
	// 	check_invariants();
	// 	std::vector<void *> vec;
	// 	for (size_t  i = 0; i < data_.size(); ++i) {
	// 		if (data_[i].is_deleted())
	// 			continue;
	// 		vec.push_back(data_[i].v);
	// 	}
	// 	for (size_t  i = 0; i < extra_.size(); ++i) {
	// 		if (extra_[i].is_deleted())
	// 			continue;
	// 		vec.push_back(extra_[i].v);
	// 	}
	// 	return vec;
	// }

	void
	check_invariants()
	{
		assert(cur_delta() <= DELTA);
		assert(!is_dead_);
		assert(bias_ <= data_.size());
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
	void **gc_;
	ArrayTree data_;
	ExtraHolder extra_;
	/* Number of tombstones in data + extra elements. */
	size_t bias_ = 0;
	Arc lower_;
	Arc upper_;
	size_t lower_start_;
	size_t upper_start_;
	Point rectangle_[4];
	bool is_dead_ = false;
	bool is_leaf_ = false;
};

