#pragma once

#include <type_traits>
#include <vector>
#include <cstdlib>
#include <cassert>
#include <algorithm>
#include <iostream>

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
	}

private:

	/** 
	 * Compare slopes between a1->a2 and b1->b2:
	 * retval > 0 iff a1->a2 > b1->b2.
	 */
	int
	vec_cmp(const Point& a1, const Point& a2, const Point& b1, const Point &b2)
	{
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
			static_cast<long double>(rectangle_[2].y - rectangle_[0].y) /
			static_cast<long double>(rectangle_[2].x - rectangle_[0].x);
		long double max_slope =
			static_cast<long double>(rectangle_[3].y - rectangle_[1].y) /
			static_cast<long double>(rectangle_[3].x - rectangle_[1].x);
		slope = (max_slope + min_slope) / 2.0;

		long double a = (p1.x - p0.x) * (p3.y - p1.y) -
				(p1.y - p0.y) * (p3.x - p1.x);
		long double b = (p2.x - p0.x) * (p3.y - p1.y) -
				(p2.y - p0.y) * (p3.x - p1.x);
		long double k = a / b;
		long double i_x = p0.x + k * (p2.x - p0.x);
		long double i_y = p0.y + k * (p2.y - p0.y);
		offset = i_y - (i_x - data_[0].k) * slope;
		long double pos = key * slope + offset;
		if (pos < 0)
			pos = 0;
		// std::cout << "Pos: " << pos << std::endl;
		return pos;
	}

	/**
	 * A linear check that data_ contains a key. 
	 * NB: Use for debug only!
	 */
	bool
	data_has_key_linear(const Key& k)
	{
		for (size_t i = 0; i < data_.size(); ++i)
			if (data_[i].k == k)
				return true;
		return false;
	}

	/*
	 * The greatest elem which is less or equal to k.
	 */
	void
	lower_bound_impl(const Key &k, Cell **c)
	{
		*c = NULL;
		if (data_.empty() || k < data_[0].k)
			return;
		size_t approx_pos = find_approx_pos(k);
		/** We need semi-interval [a, b) of possible positions. */
		size_t a = approx_pos > EPS ? approx_pos - EPS : 0;
		size_t b = std::min(approx_pos + EPS, data_.size() - 1) + 1;
		if (b < a) {
			*c = &data_.back();
			return;
		}
		auto it = std::upper_bound(data_.begin() + a, data_.begin() + b, Cell(k, {}));
		if (it == data_.begin()) {
			return;
		}
		it--;
		*c = &(*it);
	}

	void
	find_impl(const Key &k, Cell **c)
	{
		*c = NULL;
		if (data_.empty() || k < data_[0].k || k > data_[data_.size() - 1].k)
			return;
		size_t approx_pos = find_approx_pos(k);
		/*
		 * Extend Epsilon to negate an error.
		 * TODO: investigate if we can reduce additional constant.
		 */
		static const unsigned eps = EPS + 5;
		/* We need semi-interval [a, b) of possible positions. */
		size_t a = approx_pos > eps ? approx_pos - eps : 0;
		size_t b = std::min(approx_pos + eps, data_.size() - 1) + 1;
		if (b < a) {
			assert(b == data_.size());
			a = 0;
			a = std::max(a, b - 2 * eps);
		}
		auto it = std::lower_bound(data_.begin() + a, data_.begin() + b, Cell(k, {}));
		if (it == data_.end() || it->k != k) {
			if (data_has_key_linear(k)) {
				std::cout << "Key: " << k << std::endl;
				std::cout << "EPS: " << EPS << std::endl;
				std::cout << "approx pos: " << approx_pos << std::endl;
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
	try_append_extra(const Key& k, const Value &v)
	{
		if (extra_.size() < DELTA) {
			extra_.push_back({k, v});
			return true;
		}
		return false;
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
		if (try_replace(k ,v))
			return {};
		if (try_replace_extra(k, v))
			return {};
		if (try_append(k, v))
			return {};
		if (try_append_extra(k, v))
			return {};
		
		/* Append key to extra not to miss it. */
		extra_.emplace_back(k, v);
		std::sort(extra_.begin(), extra_.end());
		Collection<GeometricBlock<Key, Value, EPS, DELTA> *> rebuilt;
		rebuilt.emplace_back(new GeometricBlock<Key, Value, EPS, DELTA>);
		size_t data_i = 0;
		size_t extra_i = 0;
		for (; data_i < data_.size() && extra_i < extra_.size();) {
			assert(data_[data_i] != extra_[extra_i]);
			bool success;
			if (data_[data_i] < extra_[extra_i]) {
				success = rebuilt.back()->try_append(data_[data_i].k, data_[data_i].v);
				if (success) {
					data_i++;
				} else {
					rebuilt.emplace_back(new GeometricBlock<Key, Value, EPS, DELTA>);
				}
			} else {
				success = rebuilt.back()->try_append(extra_[extra_i].k, extra_[extra_i].v);
				if (success) {
					extra_i++;
				} else {
					rebuilt.emplace_back(new GeometricBlock<Key, Value, EPS, DELTA>);
				}
			}
		}
		for (; data_i < data_.size();) {
			bool success;	
			success = rebuilt.back()->try_append(data_[data_i].k, data_[data_i].v);
			if (success) {
				data_i++;
			} else {
				rebuilt.emplace_back(new GeometricBlock<Key, Value, EPS, DELTA>);
			}
		}
		for (; extra_i < extra_.size();) {
			bool success;	
			success = rebuilt.back()->try_append(extra_[extra_i].k, extra_[extra_i].v);
			if (success) {
				extra_i++;
			} else {
				rebuilt.emplace_back(new GeometricBlock<Key, Value, EPS, DELTA>);
			}
		}
		return rebuilt;
	}

	bool
	find(const Key& k, Value *v)
	{
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
		Cell *cell;
		lower_bound_impl(k, &cell);
		if (cell != NULL) {
			/*
			 * We've got a maximal element which is LEQ to a given key.
			 * Let's search for a bigger element that is lower than k.
			 */
			for (Cell &c : extra_) {
				if (c.k > cell->k && c.k <= k)
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
				if (c.k <= k && (cell == NULL || c.k > cell->k)) {
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
	del(const Key &k)
	{
		Cell *cell;
		find_impl(k, &cell);
		if (cell != NULL) {
			cell->del = true;
			return;
		}
		for (Cell &c : extra_) {
			if (c.k == k) {
				c.del = true;
				return;
			}
		}
	}

	bool
	empty()
	{
		assert(!data_.empty() || extra_.empty());
		return data_.empty();
	}

	size_t
	size()
	{
		return data_.size() + extra_.size();
	}

	const Key &
	origin_key()
	{
		return data_[0].k;
	}

	Value &
	origin_value()
	{
		return data_[0].v;
	}

	/**
	 * Print keys from data_ and extra_.
	 * NB: use for debug only.
	 */
	void
	print_keys()
	{
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

private:
	CellArray data_;
	ExtraCellArray extra_;
	Collection<Point> upper_;
	Collection<Point> lower_;
	size_t upper_start_;
	size_t lower_start_;
	Point rectangle_[4];
};

