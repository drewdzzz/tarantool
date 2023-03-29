#pragma once

#include <stack>
#include <cstdlib>
#include "gblock.hpp"

template<class Key, class Value, unsigned EPS, unsigned DELTA>
class pgdm_map {
	static_assert(DELTA > 0U, "Delta must not be zero");
	static_assert(EPS > 0U, "Epsilon must not be zero");
	static_assert(std::is_trivially_copyable<Key>::value, "Key must be trivially copyable");
	static_assert(std::is_trivially_copyable<Value>::value, "Value must be trivialy copyable");
	/* TODO: check if Key is a numeric type. */

	template<class T>
	using Collection = std::vector<T>;

	struct INode {
		virtual Collection<INode *>
		insert(const Key &k, const void *v) = 0;
		virtual bool
		find(const Key &k, void *v) = 0;
		virtual void
		del(const Key &k) = 0;
		virtual void
		del_checked(const Key &k) = 0;
		virtual bool
		lower_bound(const Key &k, void *v) = 0;
		virtual bool
		is_leaf() = 0;
		virtual const Key &
		origin_key() = 0;
		virtual const Key &
		start_key() = 0;
		virtual INode *
		first_node() = 0;
		virtual Collection<INode *>
		get_children() = 0;
		virtual Collection<Key>
		get_data() = 0;
		virtual Collection<Key>
		get_extra() = 0;
	};

	struct Node : INode {
		Node() = default;
		Node(GeometricBlock<Key, INode *, EPS, DELTA> *block) : block_(std::move(*block)) {}
		Collection<INode *>
		insert(const Key &k, const void *v) override
		{
			INode * const *node = static_cast<INode * const *>(v);
			auto res = block_.insert(k, *node);
			Collection<INode *> retval;
			for (size_t i = 0; i < res.size(); ++i) {
				Node *n = new Node(res[i]);
				// std::cout << "New node: " << n << std::endl;
				INode *ptr = static_cast<INode *>(n);
				// std::cout << "After cast: " << ptr << std::endl;
				retval.push_back(ptr);
			}
			return retval;
		}
		bool
		find(const Key &k, void *v) override
		{
			INode **node = static_cast<INode **>(v);
			return block_.find(k, node);
		}
		bool
		lower_bound(const Key &k, void *v) override
		{
			INode **node = static_cast<INode **>(v);
			return block_.lower_bound(k, node);
		}
		bool
		is_leaf() override
		{
			return false;
		}
		const Key &
		origin_key() override
		{
			return block_.origin_key();
		}
		const Key &
		start_key() override
		{
			return block_.start_key();
		}
		INode *
		first_node() override
		{
			return block_.origin_value();
		}
		Collection<INode *>
		get_children() override
		{
			return block_.get_values();
		}
		Collection<Key>
		get_data() override
		{
			return block_.get_data();
		}
		Collection<Key>
		get_extra() override
		{
			return block_.get_extra();
		}
		void
		del(const Key &k) override
		{
			return block_.del(k);
		}
		void
		del_checked(const Key &k) override
		{
			return block_.del_checked(k);
		}
	private:
		GeometricBlock<Key, INode *, EPS, DELTA> block_;
	};

	struct Leaf : INode {
		Leaf() = default;
		Leaf(GeometricBlock<Key, Value, EPS, DELTA> *block) : block_(std::move(*block)) {}
		Collection<INode *>
		insert(const Key &k, const void *v) override
		{
			const Value *val = static_cast<const Value *>(v);
			auto res = block_.insert(k, *val);
			Collection<INode *> retval;
			for (size_t i = 0; i < res.size(); ++i) {
				Leaf *l = new Leaf(res[i]);
				// std::cout << "New leaf: " << l << std::endl;
				INode *ptr = static_cast<INode *>(l);
				// std::cout << "After cast: " << ptr << std::endl;
				retval.push_back(ptr);
			}
			return retval;
		}
		bool
		find(const Key &k, void *v) override
		{
			Value *val = static_cast<Value *>(v);
			return block_.find(k, val);
		}
		bool
		lower_bound(const Key &k, void *v) override
		{
			Value *val = static_cast<Value *>(v);
			return block_.lower_bound(k, val);
		}
		bool
		is_leaf() override
		{
			return true;
		}
		const Key &
		origin_key() override
		{
			return block_.origin_key();
		}
		const Key &
		start_key() override
		{
			return block_.start_key();
		}
		INode *
		first_node() override
		{
			abort();
		}
		Collection<INode *>
		get_children() override
		{
			return {};
		}
		Collection<Key>
		get_data() override
		{
			return block_.get_data();
		}
		Collection<Key>
		get_extra() override
		{
			return block_.get_extra();
		}
		void
		del(const Key &k) override
		{
			return block_.del(k);
		}
		void
		del_checked(const Key &k) override
		{
			return block_.del_checked(k);
		}
	private:
		GeometricBlock<Key, Value, EPS, DELTA> block_;
	};
public:

	pgdm_map(): root_(new Leaf) {}

	/**
	 * If key lower than all other keys - put it in the first one.
	 */
	void
	insert(const Key &k, const Value &v)
	{
		assert(root_ != NULL);
		INode *curr = root_;
		std::stack<INode *> s;
		// std::cout << "------ Starting Insert of " << k << " ------" << std::endl;
		while (!curr->is_leaf()) {
			assert(curr != NULL);
			// std::cout << "Descending to leaf" << std::endl;
			s.push(curr);
			INode *next = NULL;
			bool found = curr->lower_bound(k, &next);
			assert(!found || next != NULL);
			if (!found) {
				// std::cout << "Lower bound didn't help, getting the first node" << std::endl;
				next = curr->first_node();
				assert(next != NULL);
			}
			assert(next != NULL);
			curr = next;
			// std::cout << "Descending iteration done" << std::endl;
			// std::cout << "New curr: " << curr << std::endl;
			// dump_node(curr);
		}
		// std::cout << "Leaf has been found" << std::endl;
		assert(curr != NULL);
		assert(curr->is_leaf());
		const void *val_ptr = static_cast<const void *>(&v);
		Collection<INode *> new_parts = curr->insert(k, val_ptr);
		// std::cout << "After insert leaf has fallen to " << new_parts.size() << " parts" << std::endl;
		if (new_parts.empty())
			return;
		/*
		 * TODO: it is not truth actually - since we introduced
		 * tombstones, number of fragments can be much bigger.
		 * We should count deleted elemets together with size of
		 * extra_ to get that guarantee on parts number.
		 */
		assert(new_parts.size() <= DELTA + 1);
		while (!s.empty() && !new_parts.empty()) {
			// std::cout << "Start inserting to ancestor " << new_parts.size() << " parts" << std::endl;
			/*
			 * Current node has fell apart, so we need to delete
			 * its old version from the parent.
			 */
			auto parent = s.top();
			parent->del_checked(curr->start_key());
			Collection<INode *> curr_new_nodes{};
			curr = s.top();
			s.pop();
			size_t i = 0;
			for (; i < new_parts.size() && curr_new_nodes.empty(); ++i) {
				curr_new_nodes =
					curr->insert(new_parts[i]->origin_key(), &new_parts[i]);
			}
			assert(curr_new_nodes.size() <= DELTA + 1);
			if (curr_new_nodes.empty()) {
				// std::cout << "Stop inserting to ancestor - no additional steps required" << std::endl;
				new_parts.clear();
				break;
			}
			if (i == new_parts.size()) {
				// std::cout << "Keep inserting - the last node fell apart" << std::endl;
				new_parts = std::move(curr_new_nodes);
				continue;
			}
			// std::cout << "Keep inserting" << std::endl;
			for (; i < new_parts.size(); ++i) {
				size_t insert_idx = 0;
				auto &curr_key = new_parts[i]->origin_key();
				while (insert_idx < curr_new_nodes.size() &&
				       curr_key <= curr_new_nodes[insert_idx]->origin_key()) {
					insert_idx++;
				}
				if (insert_idx > 0)
					insert_idx--;
				/* TODO: assert that deletion is redundant? */
				auto tmp_res = curr_new_nodes[insert_idx]->insert(curr_key, &new_parts[i]);
				assert(tmp_res.empty());
				(void)tmp_res;
			}
			new_parts = std::move(curr_new_nodes);
		}
		/*
		 * Stack is empty while new_parts is not - that means that
		 * root of our index has fallen apart. Create a new one.
		 */
		if (s.empty() && !new_parts.empty()) {
			// std::cout << "Stack is empty while new_parts is not - " << new_parts.size() << " nodes left" << std::endl;
			if (new_parts.size() == 1) {
				// std::cout << "Just re-assign root" << std::endl;
				root_ = new_parts[0];
				return;
			}
			root_ = new Node;
			for (size_t i = 0; i < new_parts.size(); ++i) {
				auto tmp_res =
					root_->insert(new_parts[i]->origin_key(), &new_parts[i]);
				assert(tmp_res.empty());
				(void)tmp_res;
			}
		}
	}

	bool
	find(const Key &k, Value *v)
	{
		INode *curr = root_;
		while (!curr->is_leaf()) {
			INode *next;
			bool found = curr->lower_bound(k, &next);
			if (!found)
				next = curr->first_node();
			curr = next;
		}
		return curr->find(k, v);
	}

	void
	dump_node(INode *curr, int depth = 0)
	{
		auto data = curr->get_data();
		auto extra = curr->get_extra();
		auto children = curr->get_children();
		std::cout << std::endl;
		std::cout << "Level: " << depth << std::endl;
		if (curr->is_leaf())
			std::cout << "-- Leaf --" << std::endl;
		std::cout << "Data:\n";
		for (auto &k : data) {
			std::cout << k << ", ";
		}
		std::cout << "\nExtra:\n";
		for (auto &k : extra) {
			std::cout << k << ", ";
		}
		std::cout << "\nChildren:\n";
		for (auto &c : children) {
			std::cout << c << ", ";
		}
		std::cout << std::endl << std::endl;
		for (auto &c : children)
			dump_node(c, depth + 1);
	}

	void
	dump()
	{
		dump_node(root_);
	}

	size_t
	count_node(INode *curr)
	{
		auto data = curr->get_data();
		auto extra = curr->get_extra();
		if (curr->is_leaf())
			return data.size() + extra.size();
		auto children = curr->get_children();
		size_t retval = 0;
		for (auto &c : children)
			retval += count_node(c);
		return retval;
	}

	size_t	
	count()
	{
		return count_node(root_);
	}

private:
	INode *root_;
};