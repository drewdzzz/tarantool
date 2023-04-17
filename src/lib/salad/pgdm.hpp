#pragma once

#include <stack>
#include <cstdlib>
#include "gblock.hpp"

template<class Key, unsigned EPS, unsigned DELTA, unsigned EXTENT_SIZE, unsigned BLOCK_SIZE>
class pgdm_map {
	static_assert(DELTA > 0U, "Delta must not be zero");
	static_assert(EPS > 0U, "Epsilon must not be zero");
	static_assert(std::is_trivially_copyable<Key>::value, "Key must be trivially copyable");
	/* TODO: check if Key is a numeric type. */

	static_assert(EXTENT_SIZE % BLOCK_SIZE == 0, "Extent size must be divisible by block size");

	template<class T>
	using Collection = std::vector<T>;

	using DataHolder = internal::DataHolder<Key, 2 * EPS>;
	using DataPayload = internal::DataPayload<Key, 2 * EPS>;
	using Node = GeometricBlock<Key, EPS, DELTA, BLOCK_SIZE>;

	Collection<Node *>
	payload_to_nodes(DataPayload *payload)
	{
		assert(payload != NULL);
		Collection<Node *> nodes{};
		for (size_t i = 0; i < payload->size; ++i)
			nodes.push_back(new Node(matras_, payload->data[i]));
		return nodes;
	}

public:

	pgdm_map(matras_alloc_func extent_alloc_func,
		 matras_free_func extent_free_func,
		 void *alloc_ctx, struct matras_stats *alloc_stats)
	{
		matras_create(&matras_,
			      EXTENT_SIZE, BLOCK_SIZE,
			      extent_alloc_func, extent_free_func, alloc_ctx,
			      alloc_stats);
		matras_head_read_view(&view_);
		root_ = new Node(matras_);
		root_->set_leaf();
	}

	/**
	 * If key lower than all other keys - put it in the first one.
	 * TODO: do not forget to truncate region.
	 */
	void
	insert(const Key &k, void *v)
	{
		assert(root_ != NULL);
		Node *curr = root_;
		std::stack<Node *> s;
		// std::cout << "------ Starting Insert of " << k << " ------" << std::endl;
		while (!curr->is_leaf()) {
			assert(curr != NULL);
			// std::cout << "Descending to leaf" << std::endl;
			s.push(curr);
			Node *next = NULL;
			Node **next_ptr = &next;
			bool found = curr->lower_bound(k, (void **)next_ptr);
			assert(!found || next != NULL);
			if (!found) {
				// std::cout << "Lower bound didn't help, getting the first node" << std::endl;
				next = static_cast<Node *>(curr->origin_value());
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
		Collection<Node *> new_nodes{};
		{
			DataPayload* new_parts = curr->insert(k, v);
			// std::cout << "After insert leaf has fallen to " << new_parts.size() << " parts" << std::endl;
			if (new_parts == NULL)
				return;
			assert(new_parts->size > 0);
			/*
			 * TODO: it is not truth actually - since we introduced
			 * tombstones, number of fragments can be much bigger.
			 * We should count deleted elemets together with size of
			 * extra_ to get that guarantee on parts number.
			 */
			assert(new_parts->size <= DELTA + 1);
			new_nodes = payload_to_nodes(new_parts);
			for (size_t i = 0; i < new_nodes.size(); ++i)
				new_nodes[i]->set_leaf();
		}
		while (!s.empty() && !new_nodes.empty()) {
			// std::cout << "Start inserting to ancestor " << new_parts.size() << " parts" << std::endl;
			/*
			 * Current node has fell apart, so we need to delete
			 * its old version from the parent.
			 */
			auto parent = s.top();
			parent->del_checked(curr->start_key());
			Collection<Node *> curr_new_nodes = {};
			size_t i = 0;
			{
				DataPayload *curr_new_parts = NULL;
				curr = s.top();
				s.pop();
				for (; i < new_nodes.size() && curr_new_parts == NULL; ++i) {
					Node *new_node = new_nodes[i];
					curr_new_parts =
						curr->insert(new_node->origin_key(), new_node);
				}
				if (curr_new_parts == NULL) {
					// std::cout << "Stop inserting to ancestor - no additional steps required" << std::endl;
					new_nodes.clear();
					break;
				}
				assert(curr_new_parts->size > 0);
				assert(curr_new_parts->size <= DELTA + 1);
				curr_new_nodes = payload_to_nodes(curr_new_parts);
			}
			if (i == new_nodes.size()) {
				// std::cout << "Keep inserting - the last node fell apart" << std::endl;
				new_nodes = std::move(curr_new_nodes);
				continue;
			}
			// std::cout << "Keep inserting" << std::endl;
			for (; i < new_nodes.size(); ++i) {
				size_t insert_idx = 0;
				Node *new_node = new_nodes[i];
				auto &curr_key = new_node->origin_key();
				while (insert_idx < curr_new_nodes.size() &&
				       curr_key <= curr_new_nodes[insert_idx]->origin_key()) {
					insert_idx++;
				}
				if (insert_idx > 0)
					insert_idx--;
				/* TODO: assert that deletion is redundant? */
				DataPayload *tmp_res = curr_new_nodes[insert_idx]->insert(curr_key, new_node);
				assert(tmp_res == NULL);
				(void)tmp_res;
			}
			new_nodes = std::move(curr_new_nodes);
		}
		/*
		 * Stack is empty while new_parts is not - that means that
		 * root of our index has fallen apart. Create a new one.
		 */
		if (s.empty() && !new_nodes.empty()) {
			// std::cout << "Stack is empty while new_parts is not - " << new_parts.size() << " nodes left" << std::endl;
			if (new_nodes.size() == 1) {
				// std::cout << "Just re-assign root" << std::endl;
				root_ = new_nodes[0];
				return;
			}
			root_ = new Node(matras_);
			for (size_t i = 0; i < new_nodes.size(); ++i) {
				DataPayload *tmp_res =
					root_->insert(new_nodes[i]->origin_key(), new_nodes[i]);
				assert(tmp_res == NULL);
				(void)tmp_res;
			}
		}
	}

	bool
	find(const Key &k, void **v)
	{
		Node *curr = root_;
		while (!curr->is_leaf()) {
			Node *next;
			Node ** next_ptr = &next;
			bool found = curr->lower_bound(k, (void **)next_ptr);
			if (!found)
				next = static_cast<Node *>(curr->origin_value());
			curr = next;
		}
		return curr->find(k, v);
	}

	// void
	// dump_node(Node *curr, int depth = 0)
	// {
	// 	auto data = curr->get_data();
	// 	auto extra = curr->get_extra();
	// 	auto children = curr->get_children();
	// 	std::cout << std::endl;
	// 	std::cout << "Level: " << depth << std::endl;
	// 	if (curr->is_leaf())
	// 		std::cout << "-- Leaf --" << std::endl;
	// 	std::cout << "Data:\n";
	// 	for (auto &k : data) {
	// 		std::cout << k << ", ";
	// 	}
	// 	std::cout << "\nExtra:\n";
	// 	for (auto &k : extra) {
	// 		std::cout << k << ", ";
	// 	}
	// 	std::cout << "\nChildren:\n";
	// 	for (auto &c : children) {
	// 		std::cout << c << ", ";
	// 	}
	// 	std::cout << std::endl << std::endl;
	// 	for (auto &c : children)
	// 		dump_node(c, depth + 1);
	// }

	// void
	// dump()
	// {
	// 	dump_node(root_);
	// }

	// size_t
	// count_node(Node *curr)
	// {
	// 	auto data = curr->get_data();
	// 	auto extra = curr->get_extra();
	// 	if (curr->is_leaf())
	// 		return data.size() + extra.size();
	// 	auto children = curr->get_children();
	// 	size_t retval = 0;
	// 	for (auto &c : children)
	// 		retval += count_node(c);
	// 	return retval;
	// }

	// size_t	
	// count()
	// {
	// 	return count_node(root_);
	// }

private:
	struct matras matras_;
	struct matras_view view_;
	Node *root_;
};