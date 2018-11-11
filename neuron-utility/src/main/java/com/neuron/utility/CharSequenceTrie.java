package com.neuron.utility;

import java.util.Arrays;

public final class CharSequenceTrie<TValue> {
	private final IndexNode m_root = new IndexNode();
	private int m_count;
	
	public CharSequenceTrie() {
	}

	public int count() {
		return m_count;
	}
	
	public void clear() {
		m_count = 0;
		m_root.clear();
	}
	
	public interface IForEach<T> {
		boolean process(CharSequence key, T object);
	}
	public void forEach(IForEach<TValue> callback) {
		forEach(m_root, callback);
	}

	@SuppressWarnings("unchecked")
	private boolean forEach(IndexNode current, IForEach<TValue> callback) {
		final ValueNode here = current.getHereNode();
		if (here != null) {
			if (!callback.process(here.m_key, (TValue)here.m_value)) {
				return false;
			}
		}
		final Node[] nodes = current.m_nodes;
		final int len = nodes.length;
		for(int i=0; i<len; i++) {
			final Node n = nodes[i];
			if (n instanceof ValueNode) {
				if (!callback.process(((ValueNode)n).m_key, (TValue)((ValueNode)n).m_value)) {
					return false;
				}
			} else if (n instanceof IndexNode) {
				IndexNode next = (IndexNode)n;
				if (!forEach(next, callback)) {
					return false;
				}
			}
		}
		return true;
	}
	
	public TValue get(CharSequence key) {
		if (key == null) {
			throw new IllegalArgumentException("key may not be null");
		}
		if (key.length() == 0) {
			throw new IllegalArgumentException("key may not be zero length");
		}
		return get(m_root, key, 0);
	}

	@SuppressWarnings("unchecked")
	private TValue get(IndexNode current, CharSequence key, int depth) {
		if (depth == key.length()) {
			final ValueNode n = current.getHereNode();
			if (n == null) {
				return null;
			}
			return (TValue)n.m_value;
		}
		final int keyIndex = getKeyByte(key,depth);
		final Node n = current.getNodeIndex(keyIndex);
		if (n instanceof ValueNode) {
			ValueNode v = (ValueNode)n;
			if (v.m_key.equals(key)) {
				return (TValue)v.m_value;
			}
		} else if (n instanceof IndexNode) {
			IndexNode next = (IndexNode)n;
			if (next != null) {
				return get(next, key, depth+1);
			}
		}
		return null;
	}

	public TValue addOrFetch(CharSequence key, TValue value) {
		if (value == null) {
			throw new IllegalArgumentException("value may not be null");
		}
		if (key == null) {
			throw new IllegalArgumentException("key may not be null");
		}
		if (key.length() == 0) {
			throw new IllegalArgumentException("key may not be zero length");
		}
		return addOrFetch(m_root, key, value, 0);
	}

	@SuppressWarnings("unchecked")
	private TValue addOrFetch(IndexNode current, CharSequence key, TValue value, int depth) {
		if (depth == key.length()) {
			// Either we have a 0 length key or we have reached the end of characters.
			// This guy needs to be added to the current IndexNode
			final ValueNode n = current.getHereNode();
			if (n == null) {
				current.setHereNode(new ValueNode(key, value));
				m_count++;
				return null;
			}
			return (TValue)n.m_value;
		}
		final int keyIndex = getKeyByte(key,depth);
		final Node n = current.getNodeIndex(keyIndex);

		if (n instanceof IndexNode) {
			final IndexNode next = (IndexNode)n;
			return addOrFetch(next, key, value, depth+1);
		}

		if (n instanceof ValueNode) {
			final ValueNode vExisting = (ValueNode)n;
			if (equals(vExisting.m_key, key, depth)) {
				return (TValue)vExisting.m_value;
			} 
			// value conflict, need to replace with index node and move existing one
			// down.  It is possible the two will conflict again, but that is ok, we will
			// catch it again on our next recurse down.
			current.clearValueNode(keyIndex);
			final IndexNode next = new IndexNode();
			current.setIndexNode(keyIndex, next);
			if (depth+1 == vExisting.m_key.length()) {
				next.setHereNode(vExisting);
			} else {
				next.setValueNode(getKeyByte(vExisting.m_key, depth+1), vExisting);
			}
			return addOrFetch(next, key, value, depth+1);
		}

		// No node here, just add value
		final ValueNode v = new ValueNode(key, value);
		current.setValueNode(keyIndex, v);
		m_count++;
		return null;
	}

	public TValue addOrReplace(CharSequence key, TValue value) {
		if (value == null) {
			throw new IllegalArgumentException("value may not be null");
		}
		if (key == null) {
			throw new IllegalArgumentException("key may not be null");
		}
		if (key.length() == 0) {
			throw new IllegalArgumentException("key may not be zero length");
		}
		return addOrReplace(m_root, key, value, 0);
	}

	@SuppressWarnings("unchecked")
	private TValue addOrReplace(IndexNode current, CharSequence key, TValue value, int depth) {
		if (depth == key.length()) {
			// Either we have a 0 length key or we have reached the end of characters.
			// This guy needs to be added to the current IndexNode
			final ValueNode n = current.getHereNode();
			if (n == null) {
				current.setHereNode(new ValueNode(key, value));
				m_count++;
				return null;
			}
			final TValue oldValue = (TValue)n.m_value;
			n.m_value = value;
			return oldValue;
		}
		final int keyIndex = getKeyByte(key,depth);
		final Node n = current.getNodeIndex(keyIndex);

		if (n instanceof IndexNode) {
			final IndexNode next = (IndexNode)n;
			return addOrReplace(next, key, value, depth+1);
		}

		if (n instanceof ValueNode) {
			final ValueNode vExisting = (ValueNode)n;
			if (equals(vExisting.m_key, key, depth)) {
				final TValue oldValue = (TValue)vExisting.m_value;
				vExisting.m_value = value;
				return oldValue;
			} 
			// value conflict, need to replace with index node and move existing one
			// down.  It is possible the two will conflict again, but that is ok, we will
			// catch it again on our next recurse down.
			current.clearValueNode(keyIndex);
			final IndexNode next = new IndexNode();
			current.setIndexNode(keyIndex, next);
			if (depth+1 == vExisting.m_key.length()) {
				next.setHereNode(vExisting);
			} else {
				next.setValueNode(getKeyByte(vExisting.m_key, depth+1), vExisting);
			}
			return addOrReplace(next, key, value, depth+1);
		}

		// No node here, just add value
		final ValueNode v = new ValueNode(key, value);
		current.setValueNode(keyIndex, v);
		m_count++;
		return null;
	}

	public TValue replace(CharSequence key, TValue value) {
		if (value == null) {
			throw new IllegalArgumentException("value may not be null");
		}
		if (key == null) {
			throw new IllegalArgumentException("key may not be null");
		}
		if (key.length() == 0) {
			throw new IllegalArgumentException("key may not be zero length");
		}
		return replace(m_root, key, value, 0);
	}

	@SuppressWarnings("unchecked")
	private TValue replace(IndexNode current, CharSequence key, TValue value, int depth) {
		if (depth == key.length()) {
			// Either we have a 0 length key or we have reached the end of characters.
			// This guy needs to be added to the current IndexNode
			final ValueNode n = current.getHereNode();
			if (n == null) {
				return null;
			}
			final TValue oldValue = (TValue)n.m_value;
			n.m_value = value;
			return oldValue;
		}
		final int keyIndex = getKeyByte(key,depth);
		final Node n = current.getNodeIndex(keyIndex);

		if (n instanceof IndexNode) {
			final IndexNode next = (IndexNode)n;
			return replace(next, key, value, depth+1);
		}
		if (n instanceof ValueNode) {
			final ValueNode vExisting = (ValueNode)n;
			if (equals(vExisting.m_key, key, depth)) {
				final TValue oldValue = (TValue)vExisting.m_value;
				vExisting.m_value = value;
				return oldValue;
			}
			return null;
		}
		return null;
	}

	public TValue remove(CharSequence key) {
		if (key == null) {
			throw new IllegalArgumentException("key may not be null");
		}
		if (key.length() == 0) {
			throw new IllegalArgumentException("key may not be zero length");
		}
		return remove(m_root, key, 0);
	}

	@SuppressWarnings("unchecked")
	private TValue remove(IndexNode current, CharSequence key, int depth) {
		if (depth == key.length()) {
			// Either we have a 0 length key or we have reached the end of characters.
			// This guy needs to be added to the current IndexNode
			final ValueNode n = current.getHereNode();
			if (n == null) {
				return null;
			}
			current.clearHereNode();
			m_count--;
			return (TValue)n.m_value;
		}
		final int keyIndex = getKeyByte(key,depth);
		final Node n = current.getNodeIndex(keyIndex);

		if (n instanceof IndexNode) {
			final IndexNode next = (IndexNode)n;
			final TValue value = remove(next, key, depth+1);
			// If we removed a value from some index node
			if (value != null) {
				// If there is at least 1 index node in next, we leave it
				if (next.m_indexNodeCount == 0) {
					// If next is an empty index node, remove the node
					if (next.m_valueNodeCount == 0) {
						current.clearIndexNode(keyIndex);
						
					// If next just has a single value node, time to pull up the value node and remove the index node
					} else if (next.m_valueNodeCount == 1) {
						current.clearIndexNode(keyIndex);
						current.setValueNode(keyIndex, (ValueNode)next.findFirstNonNull());
					}
				}
				return value;
			}

		} else if (n instanceof ValueNode) {
			final ValueNode vExisting = (ValueNode)n;
			if (equals(vExisting.m_key, key, depth)) {
				m_count--;
				current.clearValueNode(keyIndex);
				return (TValue)vExisting.m_value;
			}
		}

		return null;
	}

	private static class Node {
	}
	
	private static final class IndexNode extends Node {
		private final Node[] m_nodes = new Node[256];
		private ValueNode m_hereNode;
		private int m_indexNodeCount;
		private int m_valueNodeCount;
		
		void clear() {
			Arrays.fill(m_nodes, null);
			m_hereNode = null;
			m_indexNodeCount = 0;
			m_valueNodeCount = 0;
		}
		
		Node getNodeIndex(int index) {
			return m_nodes[index];
		}
		
		Node findFirstNonNull() {
			if (m_hereNode != null) {
				return m_hereNode;
			}
			for(Node n : m_nodes) {
				if (n != null) {
					return n;
				}
			}
			return null;
		}
		
		void setHereNode(ValueNode node) {
			m_hereNode = node;
			m_valueNodeCount++;
		}
		void clearHereNode() {
			m_hereNode = null;
			m_valueNodeCount--;
		}
		ValueNode getHereNode() {
			return m_hereNode;
		}

		private void setNode(int index, Node n) {
			m_nodes[index] = n;
		}
		
		void setIndexNode(int index, IndexNode n) {
			m_indexNodeCount++;
			setNode(index, n);
		}

		void setValueNode(int index, ValueNode n) {
			m_valueNodeCount++;
			setNode(index, n);
		}
		
		void clearIndexNode(int index) {
			m_nodes[index] = null;
			m_indexNodeCount--;
		}
		
		void clearValueNode(int index) {
			m_nodes[index] = null;
			m_valueNodeCount--;
		}
	}
	
	private boolean equals(CharSequence keyA, CharSequence keyB, int depth) {
		final int keyALen = keyA.length();
		final int keyBLen = keyB.length();
		if (keyALen != keyBLen) {
			return false;
		}
		for(int i=depth; i<keyALen; i++) {
			if (keyA.charAt(i) != keyB.charAt(i)) {
				return false;
			}
		}
		return true;
	}

	private int getKeyByte(CharSequence key, int depth) {
		final int keyIndex = key.charAt(depth);
		if (keyIndex > 255) {
			throw new IllegalArgumentException("The CharSequence contained a non-ASCII character");
		}
		return keyIndex;
	}
	
	private static final class ValueNode extends Node  {
		private final CharSequence m_key;
		private Object m_value;
		
		private ValueNode(CharSequence key, Object value) {
			m_key = key;
			m_value = value;
		}
	}
   
   // For testing
   boolean isRootNodeEmpty() {
   	for(Node n : m_root.m_nodes) {
   		if (n != null) {
   			return false;
   		}
   	}
		return true;
   }
}
