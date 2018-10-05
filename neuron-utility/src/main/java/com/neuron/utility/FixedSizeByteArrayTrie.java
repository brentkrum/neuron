package com.neuron.utility;

import java.nio.ByteBuffer;

public final class FixedSizeByteArrayTrie {
	private final IndexNode m_root = new IndexNode();
	private final int m_keyLen;
	
	public FixedSizeByteArrayTrie(int keyLen) {
		m_keyLen = keyLen;
	}

	public Object get(byte[] key) {
		return get(key, 0);
	}
	
	public Object get(byte[] key, int offset) {
		if (offset + m_keyLen > key.length) {
			throw new UnsupportedOperationException("Trie is for fixed size key length of " + m_keyLen + " and the array[" + offset + "-" + (offset+m_keyLen) + "] is beyond the array's length of " + key.length);
		}
		return get(m_root, key, 0, offset);
	}

	private Object get(IndexNode current, byte[] key, int depth, int offset) {
		final int keyIndex = toUnsignedInt(key[offset]);
		if (depth == m_keyLen-1) {
			ValueNode v = (ValueNode)current.m_nodes[keyIndex];
			if (v == null) {
				return null;
			}
			return v.m_value;
		}
		IndexNode next = (IndexNode)current.m_nodes[keyIndex];
		if (next == null) {
			next = new IndexNode();
			current.m_nodes[keyIndex] = next;
		}
		return get(next, key, depth+1, offset+1);
	}

	public Object addOrFetch(byte[] key, Object value) {
		return addOrFetch(key, 0, value);
	}
	
	public Object addOrFetch(byte[] key, int offset, Object value) {
		if (offset + m_keyLen > key.length) {
			throw new UnsupportedOperationException("Trie is for fixed size key length of " + m_keyLen + " and the array[" + offset + "-" + (offset+m_keyLen) + "] is beyond the array's length of " + key.length);
		}
		return addOrFetch(m_root, key, value, 0, offset);
	}

	private Object addOrFetch(IndexNode current, byte[] key, Object value, int depth, int offset) {
		final int keyIndex = toUnsignedInt(key[offset]);
		if (depth == m_keyLen-1) {
			ValueNode v = (ValueNode)current.m_nodes[keyIndex];
			if (v == null) {
				v = new ValueNode(value);
				current.m_nodes[keyIndex] = v;
				return null;
			}
			return v.m_value;
		}
		IndexNode next = (IndexNode)current.m_nodes[keyIndex];
		if (next == null) {
			next = new IndexNode();
			current.m_nodes[keyIndex] = next;
		}
		return addOrFetch(next, key, value, depth+1, offset+1);
	}

	
	public Object get(ByteBuffer key) {
		return get(key, key.position());
	}
	
	public Object get(ByteBuffer key, int startPosition) {
		if (startPosition + m_keyLen > key.limit()) {
			throw new UnsupportedOperationException("Trie is for fixed size key length of " + m_keyLen + " and the buffer[" + startPosition + "-" + (startPosition+m_keyLen) + "] is beyond the buffer's limit of " + key.limit());
		}
		return get(m_root, key, 0, startPosition);
	}

	private Object get(IndexNode current, ByteBuffer key, int depth, int offset) {
		final int keyIndex = toUnsignedInt(key.get(offset));
		if (depth == m_keyLen-1) {
			ValueNode v = (ValueNode)current.m_nodes[keyIndex];
			if (v == null) {
				return null;
			}
			return v.m_value;
		}
		IndexNode next = (IndexNode)current.m_nodes[keyIndex];
		if (next == null) {
			next = new IndexNode();
			current.m_nodes[keyIndex] = next;
		}
		return get(next, key, depth+1, offset+1);
	}
	
	public Object addOrFetch(ByteBuffer key, Object value) {
		return addOrFetch(key, key.position(), value);
	}
	
	public Object addOrFetch(ByteBuffer key, int startPosition, Object value) {
		if (startPosition + m_keyLen > key.limit()) {
			throw new UnsupportedOperationException("Trie is for fixed size key length of " + m_keyLen + " and the buffer[" + startPosition + "-" + (startPosition+m_keyLen) + "] is beyond the buffer's limit of " + key.limit());
		}
		return addOrFetch(m_root, key, value, 0, startPosition);
	}

	private Object addOrFetch(IndexNode current, ByteBuffer key, Object value, int depth, int offset) {
		final int keyIndex = toUnsignedInt(key.get(offset));
		if (depth == m_keyLen-1) {
			ValueNode v = (ValueNode)current.m_nodes[keyIndex];
			if (v == null) {
				v = new ValueNode(value);
				current.m_nodes[keyIndex] = v;
				return null;
			}
			return v.m_value;
		}
		IndexNode next = (IndexNode)current.m_nodes[keyIndex];
		if (next == null) {
			next = new IndexNode();
			current.m_nodes[keyIndex] = next;
		}
		return addOrFetch(next, key, value, depth+1, offset+1);
	}

	private static class Node {
	}
	
	private static class IndexNode extends Node {
		private final Node[] m_nodes = new Node[256];
	}
	
	private static class ValueNode extends Node  {
		private final Object m_value;
		
		private ValueNode(Object value) {
			m_value = value;
		}
	}
	
   private static int toUnsignedInt(byte x) {
      return ((int) x) & 0xff;
  }
}
