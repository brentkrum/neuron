package com.neuron.utility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;

public final class FastLinkedMap<T> {
	private static Executor m_executor;
	private volatile int m_count;
	private LLMapNode<T> m_head; // Normal: Add to head
	private LLMapNode<T> m_tail; // Normal: Remove from tail
	private List<Runnable> m_zeroEntriesCallbacks;
	private HashMap<String, LLMapNode<T>> m_map = new HashMap<>();
	
	public FastLinkedMap() {
	}
	
	static void init(Executor executor) {
		m_executor = executor;
	}
	
	public void notifyOnZeroEntries(Runnable callback) {
		final List<Runnable> callbacks;
		synchronized(this) {
			if (m_zeroEntriesCallbacks == null) {
				m_zeroEntriesCallbacks = new LinkedList<Runnable>();
			}
			m_zeroEntriesCallbacks.add(callback);
			if (m_count == 0) {
				callbacks = m_zeroEntriesCallbacks;
				m_zeroEntriesCallbacks = null;
			} else {
				callbacks = null;
			}
		}
		if (callbacks != null) {
			for(Runnable r : callbacks) {
				if (m_executor != null) {
					m_executor.execute(r);
				} else {
					r.run();
				}
			}
		}		
	}
	
	public int count() {
		return m_count;
	}
	
	public void clear() {
		synchronized(this) {
			m_count = 0;
			LLMapNode<T> node = m_head;
			while(node != null) {
				LLMapNode<T> next = node.m_next;
				node.clearListNextPrev();
				node.m_inList = false;
				node = next;
			}
			m_head = null;
			m_tail = null;
			m_map.clear();
		}		
	}

	public boolean add(LLMapNode<T> o) {
		synchronized(this) {
			synchronized(o) {
				if (o.m_inList) {
					return false;
				}
				o.addToList(m_head);
			}
			// Lookup is expensive and colliding keys is considered exceptional -- so we add the new one optimistically
			final LLMapNode<T> prevValue = m_map.put(o.getKey(), o);
			if (prevValue != null) {
				// And restore the old one in the case of a collision
				m_map.put(o.getKey(), prevValue);
				_remove(o);
            return false;
			}
			m_head = o;
			if (m_tail == null) {
				m_tail = o;
			}
			m_count++;
		}
      return true;
	}

	public final boolean addLast(LLMapNode<T> o) {
		return add(o);
	}

	public boolean addFirst(LLMapNode<T> o) {
		synchronized(this) {
			synchronized(o) {
				if (o.m_inList) {
					return false;
				}
				o.addToListFront(m_tail);
			}
			// Lookup is expensive and colliding keys is considered exceptional -- so we add the new one optimistically
			final LLMapNode<T> prevValue = m_map.put(o.getKey(), o);
			if (prevValue != null) {
				// And restore the old one in the case of a collision
				m_map.put(o.getKey(), prevValue);
				_remove(o);
            return false;
			}
			m_tail = o;
			if (m_head == null) {
				m_head = o;
			}
			m_count++;
		}
      return true;
	}
	
	public T get(String key) {
		final LLMapNode<T> node;
		synchronized(this) {
			node = m_map.get(key);
		}
		if (node == null) {
			return null;
		}
		return node.getObject();
	}
	
	public T peekFirst() {
		final LLMapNode<T> node;
		synchronized(this) {
			node = m_tail;
		}
		if (node == null) {
			return null;
		}
		return node.getObject();
	}
	
	public T peekLast() {
		final LLMapNode<T> node;
		synchronized(this) {
			node = m_head;
		}
		if (node == null) {
			return null;
		}
		return node.getObject();
	}
	
	public List<T> snapshotList() {
		final List<T> results;
		synchronized(this) {
			results = new ArrayList<T>(m_count);
			LLMapNode<T> node = m_tail;
			while(node != null) {
				results.add(node.getObject());
				node = node.m_prev;
			}
		}
		return results;
	}
	
	public T removeFirst() {
		final List<Runnable> callbacks;
		final LLMapNode<T> o;
		synchronized(this) {
			if (m_tail == null) {
				return null;
			}
			o = m_tail;
			_remove(o);
			if (m_count == 0) {
				callbacks = m_zeroEntriesCallbacks;
				m_zeroEntriesCallbacks = null;
			} else {
				callbacks = null;
			}
		}
		o.clearListNextPrev();
		if (callbacks != null) {
			for(Runnable r : callbacks) {
				if (m_executor != null) {
					m_executor.execute(r);
				} else {
					r.run();
				}
			}
		}
		return o.getObject();
	}
	
	public T removeLast() {
		final List<Runnable> callbacks;
		final LLMapNode<T> o;
		synchronized(this) {
			if (m_head == null) {
				return null;
			}
			o = m_head;
			_remove(o);
			if (m_count == 0) {
				callbacks = m_zeroEntriesCallbacks;
				m_zeroEntriesCallbacks = null;
			} else {
				callbacks = null;
			}
		}
		o.clearListNextPrev();
		if (callbacks != null) {
			for(Runnable r : callbacks) {
				if (m_executor != null) {
					m_executor.execute(r);
				} else {
					r.run();
				}
			}
		}
		return o.getObject();
	}

	public boolean remove(LLMapNode<T> o) {
		final List<Runnable> callbacks;
		synchronized(this) {
			synchronized(o) {
				if (!_remove(o)) {
					return false;
				}
			}
			if (m_count == 0) {
				callbacks = m_zeroEntriesCallbacks;
				m_zeroEntriesCallbacks = null;
			} else {
				callbacks = null;
			}
		}
		o.clearListNextPrev();
		if (callbacks != null) {
			for(Runnable r : callbacks) {
				if (m_executor != null) {
					m_executor.execute(r);
				} else {
					r.run();
				}
			}
		}
		return true;
	}

	private boolean _remove(LLMapNode<T> o) {
		if (!o.m_inList) {
			return false;
		}
		o.removeFromList();
		m_map.remove(o.getKey());
		if (o.m_prev == null) {
			m_head = o.m_next;
		}
		if (o.m_next == null) {
			m_tail = o.m_prev;
		}
		m_count--;
		return true;
	}

	public static abstract class LLMapNode<T> {
		private volatile LLMapNode<T> m_next;
		private volatile LLMapNode<T> m_prev;
		private volatile boolean m_inList;

		public LLMapNode() {
		}
		
		protected abstract String getKey();
		protected abstract T getObject();

		private final void addToList(LLMapNode<T> head) {
			if (head != null) {
				head.m_prev = this;
				m_next = head;
			}
			m_inList = true;
		}

		private final void addToListFront(LLMapNode<T> tail) {
			if (tail != null) {
				tail.m_next = this;
				m_prev = tail;
			}
			m_inList = true;
		}
		
		private final void removeFromList() {
			m_inList = false;
			if (m_next != null) {
				m_next.m_prev = m_prev;
			}
			if (m_prev != null) {
				m_prev.m_next = m_next;
			}
		}
		
		private final void clearListNextPrev() { 
			m_next = null;
			m_prev = null;
		}
	}
}
