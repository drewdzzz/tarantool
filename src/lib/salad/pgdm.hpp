#pragma once

#include <stack>
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
		virtual bool
		lower_bound(const Key &k, void *v) = 0;
		virtual bool
		is_leaf() = 0;
		virtual const Key &
		origin_key() = 0;
		virtual INode *
		first_node() = 0;
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
		INode *
		first_node() override
		{
			return block_.origin_value();
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
		INode *
		first_node() override
		{
			abort();
		}
	private:
		GeometricBlock<Key, Value, EPS, DELTA> block_;
	};
public:

	pgdm_map(): root_(new Leaf) {}

	void
	insert(const Key &k, const Value &v)
	{
		assert(root_ != NULL);
		INode *curr = root_;
		std::stack<INode *> s;
		while (!curr->is_leaf()) {
			s.push(curr);
			INode *next = NULL;
			bool found = curr->lower_bound(k, &next);
			assert(!found || next != NULL);
			if (!found) {
				next = curr->first_node();
				assert(next != NULL);
			}
			assert(next != NULL);
			curr = next;
		}
		assert(curr->is_leaf());
		const void *val_ptr = static_cast<const void *>(&v);
		Collection<INode *> new_parts = curr->insert(k, val_ptr);
		if (new_parts.empty())
			return;
		/*
		 * TODO: it is not truth actually - since we introduced
		 * tombstones, number of fragments can be much bigger.
		 * We should count deleted elemets together with size of
		 * extra_ to get that guarantee on parts number.
		 */
		assert(new_parts.size() <= DELTA + 1);
		/*
		 * TODO: review deletion of nodes that fell apart.
		 */
		while (!s.empty() && !new_parts.empty()) {
			Collection<INode *> curr_new_nodes{};
			curr = s.top();
			s.pop();
			size_t i = 0;
			for (; i < new_parts.size() && curr_new_nodes.empty(); ++i) {
				curr_new_nodes =
					curr->insert(new_parts[i]->origin_key(), new_parts[i]);
			}
			assert(curr_new_nodes.size() <= DELTA + 1);
			if (curr_new_nodes.empty())
				break;
			if (i == new_parts.size()) {
				new_parts = std::move(curr_new_nodes);
				continue;
			}
			size_t insert_idx = 0;
			assert(insert_idx > 0);
			for (; i < new_parts.size(); ++i) {
				while (insert_idx < curr_new_nodes.size() &&
				       new_parts[i]->origin_key() <= curr_new_nodes[insert_idx]->origin_key()) {
					insert_idx++;
				}
				insert_idx--;
				auto tmp_res = curr_new_nodes[insert_idx]->insert(k, new_parts[i]);
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
			if (new_parts.size() == 1) {
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
			if (!found) {
				next = curr->first_node();
			}
			curr = next;
		}	
		return curr->find(k, v);
	}

private:
	INode *root_;
};