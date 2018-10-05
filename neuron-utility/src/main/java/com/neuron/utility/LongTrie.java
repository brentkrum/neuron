package com.neuron.utility;

public final class LongTrie<TValue> {
	private final IndexNode m_root = new IndexNode();
	private int m_count;
	
	public LongTrie() {
	}

	public int count() {
		return m_count;
	}
	
	public TValue get(long key) {
		return get(m_root, key, 0);
	}

	@SuppressWarnings("unchecked")
	private TValue get(IndexNode current, long key, int depth) {
		final int keyIndex = getKeyByte(key, depth);
		final Node n = current.getNodeIndex(keyIndex);
		if (n instanceof ValueNode) {
			ValueNode v = (ValueNode)n;
			if (v.m_key == key) {
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

	public TValue addOrFetch(long key, TValue value) {
		if (value == null) {
			throw new IllegalArgumentException("value may not be null");
		}
		return addOrFetch(m_root, key, value, 0);
	}

	@SuppressWarnings("unchecked")
	private TValue addOrFetch(IndexNode current, long key, TValue value, int depth) {
		final int keyIndex = getKeyByte(key, depth);
		final Node n = current.getNodeIndex(keyIndex);

		if (n instanceof IndexNode) {
			final IndexNode next = (IndexNode)n;
			return addOrFetch(next, key, value, depth+1);
		}

		if (n instanceof ValueNode) {
			final ValueNode vExisting = (ValueNode)n;
			if (vExisting.m_key == key) {
				return (TValue)vExisting.m_value;
			} 
			// value conflict, need to replace with index node and move existing one
			// down.  It is possible the two will conflict again, but that is ok, we will
			// catch it again on our next recurse down.
			current.clearValueNode(keyIndex);
			final IndexNode next = new IndexNode();
			current.setIndexNode(keyIndex, next);
			next.setValueNode(getKeyByte(vExisting.m_key, depth+1), vExisting);
			return addOrFetch(next, key, value, depth+1);
		}

		// No node here, just add value
		final ValueNode v = new ValueNode(key, value);
		current.setValueNode(keyIndex, v);
		m_count++;
		return null;
	}

	public TValue remove(long key) {
		return remove(m_root, key, 0);
	}

	@SuppressWarnings("unchecked")
	private TValue remove(IndexNode current, long key, int depth) {
		final int keyIndex = getKeyByte(key, depth);
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
			if (vExisting.m_key == key) {
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
		private int m_indexNodeCount;
		private int m_valueNodeCount;
		
		Node getNodeIndex(int index) {
			return m_nodes[index];
		}
		
		Node findFirstNonNull() {
			for(Node n : m_nodes) {
				if (n != null) {
					return n;
				}
			}
			return null;
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
	
	private static final class ValueNode extends Node  {
		private final long m_key;
		private final Object m_value;
		
		private ValueNode(long key, Object value) {
			m_key = key;
			m_value = value;
		}
	}
	
	/*
	 * We grab the indexes in reverse, since numbers tend to be most volatile in their lower
	 * order bits
	 */
   private static int getKeyByte(long l, int byteNum) {
      return ((int)(l >> byteNum*8)) & 0xFF;
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
