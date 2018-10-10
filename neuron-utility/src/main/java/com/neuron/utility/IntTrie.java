package com.neuron.utility;

import java.util.ArrayList;
import java.util.List;

public final class IntTrie<TValue> {
	private final IndexNode m_root = new IndexNode();
	private int m_count;
	
	public IntTrie() {
	}

	public int count() {
		return m_count;
	}
	
	public TValue get(int key) {
		return get(m_root, key, 0);
	}

	@SuppressWarnings("unchecked")
	private TValue get(IndexNode current, int key, int depth) {
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

	public TValue addOrFetch(int key, TValue value) {
		if (value == null) {
			throw new IllegalArgumentException("value may not be null");
		}
		return addOrFetch(m_root, key, value, 0);
	}

	@SuppressWarnings("unchecked")
	private TValue addOrFetch(IndexNode current, int key, TValue value, int depth) {
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

	public TValue addOrReplace(int key, TValue value) {
		if (value == null) {
			throw new IllegalArgumentException("value may not be null");
		}
		return addOrReplace(m_root, key, value, 0);
	}

	@SuppressWarnings("unchecked")
	private TValue addOrReplace(IndexNode current, int key, TValue value, int depth) {
		final int keyIndex = getKeyByte(key, depth);
		final Node n = current.getNodeIndex(keyIndex);

		if (n instanceof IndexNode) {
			final IndexNode next = (IndexNode)n;
			return addOrReplace(next, key, value, depth+1);
		}

		if (n instanceof ValueNode) {
			final ValueNode vExisting = (ValueNode)n;
			if (vExisting.m_key == key) {
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
			next.setValueNode(getKeyByte(vExisting.m_key, depth+1), vExisting);
			return addOrReplace(next, key, value, depth+1);
		}

		// No node here, just add value
		final ValueNode v = new ValueNode(key, value);
		current.setValueNode(keyIndex, v);
		m_count++;
		return null;
	}

	public TValue remove(int key) {
		return remove(m_root, key, 0);
	}

	@SuppressWarnings("unchecked")
	private TValue remove(IndexNode current, int key, int depth) {
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
	
	public List<TValue> values() {
		List<TValue> list = new ArrayList<>(m_count);
		values0(list, m_root);
		return list;
	}
	
	@SuppressWarnings("unchecked")
	private void values0(List<TValue> list, IndexNode current) {
		final Node[] currentNodes = current.m_nodes;
		int numIndex=0;
		int numValue=0;
		for(int i=0; i<currentNodes.length; i++) {
			final Node n = currentNodes[i];
			if (n == null) {
				continue;
			}
			if (n instanceof ValueNode) {
				ValueNode v = (ValueNode)n;
				list.add((TValue)v.m_value);
				numValue++;
			} else if (n instanceof IndexNode) {
				values0(list, (IndexNode)n);
				numIndex++;
			}
			if (numIndex == current.m_indexNodeCount && numValue == current.m_valueNodeCount) {
				return;
			}
		}
	}

	@FunctionalInterface
	public interface IForEach<T> {
		/**
		 * Walks the entire list calling the method for each item
		 * 
		 * @param object
		 * @return true to continue iteration, false to end it
		 */
		boolean process(int key, T object);
	}
	public void forEach(IForEach<TValue> callback) {
		forEach0(m_root, callback);
	}
	
	@SuppressWarnings("unchecked")
	private boolean forEach0(IndexNode current, IForEach<TValue> callback) {
		final Node[] currentNodes = current.m_nodes;
		final int maxNumIndex = current.m_indexNodeCount;
		final int maxNumValue = current.m_valueNodeCount;
		int numIndex=0;
		int numValue=0;
		for(int i=0; i<currentNodes.length; i++) {
			final Node n = currentNodes[i];
			if (n == null) {
				continue;
			}
			if (n instanceof ValueNode) {
				ValueNode v = (ValueNode)n;
				boolean keepGoing = callback.process(v.m_key, (TValue)v.m_value);
				if (!keepGoing) {
					return false;
				}
				numValue++;
			} else if (n instanceof IndexNode) {
				boolean keepGoing = forEach0((IndexNode)n, callback);
				if (!keepGoing) {
					return false;
				}
				numIndex++;
			}
			if (numIndex == maxNumIndex && numValue == maxNumValue) {
				break;
			}
		}
		return true;
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
		private final int m_key;
		private Object m_value;
		
		private ValueNode(int key, Object value) {
			m_key = key;
			m_value = value;
		}
	}
	
	/*
	 * We grab the indexes in reverse, since numbers tend to be most volatile in their lower
	 * order bits
	 */
   private static int getKeyByte(int l, int byteNum) {
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
