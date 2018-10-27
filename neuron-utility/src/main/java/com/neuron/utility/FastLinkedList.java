package com.neuron.utility;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;

public final class FastLinkedList<T> {
	private static Executor m_executor;

	private final int m_maxCount; 
	private volatile int m_count;
	private LLNode<T> m_head; // Normal: Add to head (also referred to as "Last")
	private LLNode<T> m_tail; // Normal: Remove from tail (also referred to as "First")
	private List<Runnable> m_zeroEntriesCallbacks;
	
	public FastLinkedList() {
		m_maxCount = Integer.MAX_VALUE;
	}
	public FastLinkedList(int maxNumberOfItems) {
		m_maxCount = maxNumberOfItems;
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
			LLNode<T> node = m_tail;
			while(node != null) {
				LLNode<T> next = node.m_prev;
				node.clearListNextPrev();
				node.m_inList = false;
				node = next;
			}
			m_head = null;
			m_tail = null;
		}		
	}

	public boolean add(LLNode<T> o) {
		synchronized(this) {
			if (m_count >= m_maxCount) {
				return false;
			}
			synchronized(o) {
				if (o.m_inList) {
					return false;
				}
				o.addToList(m_head);
			}
			m_head = o;
			if (m_tail == null) {
				m_tail = o;
			}
			m_count++;
		}
      return true;
	}

	public final boolean addLast(LLNode<T> o) {
		return add(o);
	}
	
	public boolean addFirst(LLNode<T> o) {
		synchronized(this) {
			if (m_count >= m_maxCount) {
				return false;
			}
			synchronized(o) {
				if (o.m_inList) {
					return false;
				}
				o.addToListFront(m_tail);
			}
			m_tail = o;
			if (m_head == null) {
				m_head = o;
			}
			m_count++;
		}
      return true;
	}
	
	public T peekFirst() {
		final LLNode<T> node;
		synchronized(this) {
			node = m_tail;
		}
		if (node == null) {
			return null;
		}
		return node.getObject();
	}
	
	public T peekLast() {
		final LLNode<T> node;
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
			LLNode<T> node = m_tail;
			while(node != null) {
				results.add(node.getObject());
				node = node.m_prev;
			}
		}
		return results;
	}
	
	public interface IForEach<T> {
		/**
		 * Walks the entire list calling the method for each item
		 * 
		 * @param object
		 * @return true to continue iteration, false to end it
		 */
		boolean process(T object);
	}
	public void forEach(IForEach<T> callback) {
		synchronized(this) {
			LLNode<T> node = m_tail;
			while(node != null) {
				if (callback.process(node.getObject()) == false) {
					break;
				}
				node = node.m_prev;
			}
		}
	}
	
	public T removeFirst() {
		final List<Runnable> callbacks;
		final LLNode<T> o;
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
		final LLNode<T> o;
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

	public boolean remove(LLNode<T> o) {
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

	private boolean _remove(LLNode<T> o) {
		if (!o.m_inList) {
			return false;
		}
		o.removeFromList();
		if (o.m_prev == null) {
			m_head = o.m_next;
		}
		if (o.m_next == null) {
			m_tail = o.m_prev;
		}
		m_count--;
		return true;
	}

	public static abstract class LLNode<T> {
		private volatile LLNode<T> m_next;
		private volatile LLNode<T> m_prev;
		private volatile boolean m_inList;

		public LLNode() {
		}
		
		protected abstract T getObject();

		private final void addToList(LLNode<T> head) {
			if (head != null) {
				head.m_prev = this;
				m_next = head;
			}
			m_inList = true;
		}

		private final void addToListFront(LLNode<T> tail) {
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
