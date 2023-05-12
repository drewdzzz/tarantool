#pragma once

#include <stack>
#include <cstdlib>
#include "gblock.hpp"

namespace pgdm {

template<class Key, unsigned EPS, unsigned DELTA, unsigned EXTENT_SIZE, unsigned BLOCK_SIZE>
class pgdm_map {
	static_assert(DELTA > 0U, "Delta must not be zero");
	static_assert(EPS > 0U, "Epsilon must not be zero");
	static_assert(std::is_trivially_copyable<Key>::value, "Key must be trivially copyable");
	/* TODO: check if Key is a numeric type. */

	static_assert(2 * EPS * sizeof(internal::Cell<Key>) == BLOCK_SIZE, "EPS is consistent with BLOCK_SIZE");
	static_assert(EXTENT_SIZE % BLOCK_SIZE == 0, "Extent size must be divisible by block size");

	using ArrayTree = internal::ArrayTree<Key, internal::Cell<Key>, BLOCK_SIZE>;
	using DataPayload = internal::DataPayload<Key, BLOCK_SIZE>;
	using Node = GeometricBlock<Key, EPS, DELTA, BLOCK_SIZE>;

	Node **
	payload_to_nodes(DataPayload *payload, size_t *size)
	{
		assert(payload != NULL);
		size_t alloc_size;
		Node **nodes = (Node **)xregion_alloc_array(&fiber()->gc, Node *, payload->size, &alloc_size);
		for (size_t i = 0; i < payload->size; ++i) {
			matras_id_t result_id;
			nodes[i] = (Node *)matras_alloc(&matras_, &result_id);
			new (nodes[i]) Node(matras_, payload->data[i]);
		}
		*size = payload->size;
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
		RegionGuard(&fiber()->gc);
		assert(root_ != NULL);
		Node *curr = root_;
		std::stack<Node *> s;
		while (!curr->is_leaf()) {
			assert(curr != NULL);
			s.push(curr);
			Node *next = NULL;
			Node **next_ptr = &next;
			bool found = curr->lower_bound(k, (void **)next_ptr);
			assert(!found || next != NULL);
			if (!found) {
				next = static_cast<Node *>(curr->origin_value());
				assert(next != NULL);
			}
			assert(next != NULL);
			curr = next;
		}
		assert(curr->is_leaf());
		Node ** new_nodes{};
		size_t new_nodes_size = 0;
		{
			DataPayload* new_parts = curr->insert(k, v);
			if (new_parts == NULL)
				return;
			assert(new_parts->size > 0);
			assert(new_parts->size <= DELTA + 1);
			new_nodes = payload_to_nodes(new_parts, &new_nodes_size);
			for (size_t i = 0; i < new_nodes_size; ++i)
				new_nodes[i]->set_leaf();
		}
		while (!s.empty() && new_nodes_size != 0) {
			/*
			 * Current node has fell apart, so we need to delete
			 * its old version from the parent.
			 */
			auto parent = s.top();
			s.pop();
			Node ** curr_new_nodes = {};
			size_t curr_new_nodes_size = 0;
			size_t i = 0;
			{
				DataPayload *curr_new_parts = parent->del_checked(curr->start_key());
				curr = parent;
				for (; i < new_nodes_size && curr_new_parts == NULL; ++i) {
					Node *new_node = new_nodes[i];
					curr_new_parts =
						curr->insert(new_node->start_key(), new_node);
				}
				if (curr_new_parts == NULL) {
					new_nodes = NULL;
					new_nodes_size = 0;
					break;
				}
				assert(curr_new_parts->size > 0);
				assert(curr_new_parts->size <= DELTA + 1);
				curr_new_nodes = payload_to_nodes(curr_new_parts, &curr_new_nodes_size);
			}
			if (i == new_nodes_size) {
				new_nodes = std::move(curr_new_nodes);
				new_nodes_size = curr_new_nodes_size;
				continue;
			}
			size_t insert_idx = 0;
			for (; i < new_nodes_size; ++i) {
				Node *new_node = new_nodes[i];
				auto &curr_key = new_node->start_key();
				/*
				 * curr new parts: [[0], [5], [10]]
				 * new parts: 15
				 */
				while (insert_idx < curr_new_nodes_size &&
				       curr_key >= curr_new_nodes[insert_idx]->origin_key()) {
					insert_idx++;
				}
				if (insert_idx > 0 && curr_new_nodes[insert_idx - 1]->origin_key() <= curr_key)
					insert_idx--;
				/* TODO: assert that deletion is redundant? */
				DataPayload *tmp_res = curr_new_nodes[insert_idx]->insert(curr_key, new_node);
				assert(tmp_res == NULL);
				(void)tmp_res;
			}
			new_nodes = std::move(curr_new_nodes);
			new_nodes_size = curr_new_nodes_size;
		}
		/*
		 * Stack is empty while new_parts is not - that means that
		 * root of our index has fallen apart. Create a new one.
		 */
		if (s.empty() && new_nodes_size != 0) {
			if (new_nodes_size == 1) {
				root_ = new_nodes[0];
				return;
			}
			root_ = new Node(matras_);
			for (size_t i = 0; i < new_nodes_size; ++i) {
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

private:
	struct matras matras_;
	struct matras_view view_;
	Node *root_;
};

};