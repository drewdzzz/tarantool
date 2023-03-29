#pragma once

#include <type_traits>
#include <vector>
#include <cstdlib>
#include <cassert>
#include <algorithm>
#include <iostream>
#include <cmath>

#define LOW_EPS 1e-5

template<class Key, class Value, unsigned EPS, unsigned DELTA>
class GeometricBlock {
	static_assert(DELTA > 0U, "Delta must not be zero");
	static_assert(EPS > 0U, "Epsilon must not be zero");
	static_assert(std::is_trivially_copyable<Key>::value, "Key must be trivially copyable");
	static_assert(std::is_trivially_copyable<Value>::value, "Value must be trivialy copyable");
	/* TODO: check if Key is a numeric type. */

	template<class T>
	using Collection = std::vector<T>;
	struct Cell {
		Key k;
		Value v;
		bool del = false;
		Cell() = delete;
		Cell(const Key &k, const Value &v) : k(k), v(v) {}
		bool operator<(const Cell& other) const
		{
			return k < other.k;
		}
		bool operator==(const Cell& other) const
		{
			return k == other.k;
		}
		bool operator!=(const Cell& other) const
		{
			return k != other.k;
		}
	};
	using CellArray = Collection<Cell>;
	using ExtraCellArray = Collection<Cell>;

	struct Point {
		/** Just a key. */
		Key x;
		/** Position in data_ array. */
		ssize_t y;
	};

public:
	GeometricBlock()
	{
		data_.reserve(2 * EPS);
		extra_.reserve(DELTA);
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
		/** We need semi-interval [a, b) of possible positions. */
		size_t a = approx_pos > EPS ? approx_pos - EPS : 0;
		size_t b = std::min(approx_pos + EPS, data_.size() - 1) + 1;
		/** Set to data_.end() if the element is the highest. */
		if (b < a) {
			a = data_.size();
			b = data_.size();
		}
		auto it = std::upper_bound(data_.begin() + a, data_.begin() + b, Cell(k, {}));
		if (it == data_.begin()) {
			assert(data_is_lower_bound(k, NULL));
			return;
		}
		it--;
		for (; it != data_.begin() && it->del; it--) {}
		if (it->del) {
			// std::cout << a << " " << b << std::endl;
			assert(data_is_lower_bound(k, NULL));
			return;
		}
		*c = &(*it);
		assert(data_is_lower_bound(k, &it->k));
	}

	void
	find_impl(const Key &k, Cell **c)
	{
		check_invariants();
		*c = NULL;
		if (data_.empty() || k < data_[0].k || k > data_[data_.size() - 1].k)
			return;
		size_t approx_pos = find_approx_pos(k);
		/*
		 * Extend Epsilon to negate an error.
		 * TODO: investigate if we can reduce additional constant.
		 */
		static const unsigned eps = EPS;
		/* We need semi-interval [a, b) of possible positions. */
		size_t a = approx_pos > eps ? approx_pos - eps : 0;
		size_t b = std::min(approx_pos + eps, data_.size() - 1) + 1;
		if (b == data_.size()) {
			a = 0;
			if (b > 2 * eps)
				a = b - 2 * eps;
		}
		if (b < a) {
			std::cout << "EPS: " << EPS << std::endl;
			std::cout << "approx pos: " << approx_pos << std::endl;
			std::cout << "looking at [" << a << ", " << b << ")" << std::endl;
		}
		assert(b >= a);
		auto it = std::lower_bound(data_.begin() + a, data_.begin() + b, Cell(k, {}));
		if (it == data_.end() || it->k != k) {
			if (data_has_key_linear(k)) {
				std::cout << "Key: " << k << std::endl;
				std::cout << "EPS: " << EPS << std::endl;
				std::cout << "approx pos: " << approx_pos << std::endl;
				std::cout << "looking at [" << a << ", " << b << ")" << std::endl;
				std::cout << "it == data_.end() : " << (it == data_.end()) << ", it->k != k : " << (it->k != k) << std::endl;
				print_keys();
			}
			assert(!data_has_key_linear(k));
			return;
		}
		*c = &(*it);
		assert(data_has_key_linear(k));
	}

	/**
	 * A helper that replaces a value of existing key.
	 * Returns false in the case key is not present in the index.
	 */
	bool
	try_replace(const Key& k, const Value &v)
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
	try_append(const Key& k, const Value &v)
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
			data_.emplace_back(k, v);
			return true;
		} else if (data_.size() == 1) {
			rectangle_[2] = p2;
			rectangle_[3] = p1;
			upper_.push_back(p1);
			lower_.push_back(p2);
			data_.emplace_back(k, v);
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

		data_.emplace_back(k, v);

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
	try_replace_extra(const Key& k, const Value &v)
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
	try_append_extra(const Key& k, const Value &v)
	{
		check_invariants();
		if (extra_.size() < DELTA) {
			extra_.push_back({k, v});
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
		for (Cell &curr : extra_) {
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
	Collection<GeometricBlock<Key, Value, EPS, DELTA> *>
	insert(const Key& k, const Value& v)
	{
		check_invariants();
		if (try_replace(k ,v)) {
			check_invariants();
			return {};
		}
		if (try_replace_extra(k, v)) {
			check_invariants();
			return {};
		}
		if (try_append(k, v)) {
			check_invariants();
			return {};
		}
		if (try_append_extra(k, v)) {
			check_invariants();
			return {};
		}
		check_invariants();
		
		/* Append key to extra not to miss it. */
		extra_.emplace_back(k, v);
		/* The last action breaks invariant so this node must not be used anymore. */
		is_dead_ = true;
		std::sort(extra_.begin(), extra_.end());
		Collection<GeometricBlock<Key, Value, EPS, DELTA> *> rebuilt;
		rebuilt.emplace_back(new GeometricBlock<Key, Value, EPS, DELTA>);
		size_t data_i = 0;
		size_t extra_i = 0;
		for (; data_i < data_.size() && extra_i < extra_.size();) {
			assert(data_[data_i] != extra_[extra_i]);
			bool success;
			if (data_[data_i] < extra_[extra_i]) {
				if (data_[data_i].del) {
					data_i++;
					continue;
				}
				success = rebuilt.back()->try_append(data_[data_i].k, data_[data_i].v);
				if (success) {
					data_i++;
				} else {
					rebuilt.emplace_back(new GeometricBlock<Key, Value, EPS, DELTA>);
				}
			} else {
				if (extra_[extra_i].del) {
					extra_i++;
					continue;
				}
				success = rebuilt.back()->try_append(extra_[extra_i].k, extra_[extra_i].v);
				if (success) {
					extra_i++;
				} else {
					rebuilt.emplace_back(new GeometricBlock<Key, Value, EPS, DELTA>);
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
				rebuilt.emplace_back(new GeometricBlock<Key, Value, EPS, DELTA>);
			}
		}
		for (; extra_i < extra_.size();) {
			if (extra_[extra_i].del) {
				extra_i++;
				continue;
			}
			bool success;	
			success = rebuilt.back()->try_append(extra_[extra_i].k, extra_[extra_i].v);
			if (success) {
				extra_i++;
			} else {
				rebuilt.emplace_back(new GeometricBlock<Key, Value, EPS, DELTA>);
			}
		}
		for (auto p : rebuilt) {
			p->check_invariants();
		}
		return rebuilt;
	}

	bool
	find(const Key& k, Value *v)
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
		for (Cell& c : extra_) {
			if (c.k == k) {
				*v = c.v;
				return true;
			}
		}
		return false;
	}

	bool
	lower_bound(const Key &k, Value *v)
	{
		check_invariants();
		Cell *cell;
		lower_bound_impl(k, &cell);
		if (cell != NULL) {
			/*
			 * We've got a maximal element which is LEQ to a given key.
			 * Let's search for a bigger element that is lower than k.
			 */
			for (Cell &c : extra_) {
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
			for (Cell &c : extra_) {
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
			cell->del = true;
			return;
		}
		for (Cell &c : extra_) {
			if (c.k == k) {
				assert(!must_del || !c.del);
				c.del = true;
				return;
			}
		}
		assert(!must_del);
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
	Value &
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

	Collection<Value>
	get_values()
	{
		check_invariants();
		std::vector<Value> vec;
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
		assert(extra_.size() <= DELTA);
		assert(!is_dead_);
	}

	const Key &
	start_key()
	{
		assert(!data_.empty());
		/* Consiously forgot about tombstone. */
		return data_[0].k;
	}

private:
	CellArray data_;
	ExtraCellArray extra_;
	Collection<Point> upper_;
	Collection<Point> lower_;
	size_t upper_start_;
	size_t lower_start_;
	Point rectangle_[4];
	bool is_dead_ = false;
};

