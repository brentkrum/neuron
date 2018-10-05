package com.neuron.utility;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FastLinkedList_001_Test {
	
	@Test
	public void add() {
		FastLinkedList<MyObject> ll = new FastLinkedList<>();

		populate(ll);
		Assertions.assertEquals(2, ll.count());

		Assertions.assertEquals(1, ll.peekFirst().m_data);
		Assertions.assertEquals(2, ll.peekLast().m_data);
		Assertions.assertEquals(1, ll.removeFirst().m_data);
		Assertions.assertEquals(2, ll.peekFirst().m_data);
		Assertions.assertEquals(2, ll.peekLast().m_data);
		Assertions.assertEquals(2, ll.removeFirst().m_data);
		
		populate(ll);
		Assertions.assertEquals(2, ll.removeLast().m_data);
		Assertions.assertEquals(1, ll.peekFirst().m_data);
		Assertions.assertEquals(1, ll.peekLast().m_data);
		Assertions.assertEquals(1, ll.removeLast().m_data);
		
		populate(ll);
		List<MyObject> list = ll.snapshotList();
		Assertions.assertEquals(1, list.get(0).m_data);
		Assertions.assertEquals(2, list.get(1).m_data);
				
	}
	
	private void populate(FastLinkedList<MyObject> ll) {
		ll.add(new MyObject(1));
		ll.add(new MyObject(2));
	}
	
	@Test
	public void addFirst() {
		FastLinkedList<MyObject> ll = new FastLinkedList<>();

		populateAddFirst(ll);
		Assertions.assertEquals(2, ll.count());

		Assertions.assertEquals(1, ll.peekFirst().m_data);
		Assertions.assertEquals(2, ll.peekLast().m_data);
		Assertions.assertEquals(1, ll.removeFirst().m_data);
		Assertions.assertEquals(2, ll.peekFirst().m_data);
		Assertions.assertEquals(2, ll.peekLast().m_data);
		Assertions.assertEquals(2, ll.removeFirst().m_data);
		
		populateAddFirst(ll);
		Assertions.assertEquals(2, ll.removeLast().m_data);
		Assertions.assertEquals(1, ll.peekFirst().m_data);
		Assertions.assertEquals(1, ll.peekLast().m_data);
		Assertions.assertEquals(1, ll.removeLast().m_data);
		
		populateAddFirst(ll);
		List<MyObject> list = ll.snapshotList();
		Assertions.assertEquals(1, list.get(0).m_data);
		Assertions.assertEquals(2, list.get(1).m_data);
				
	}
	
	@Test
	public void maxCount() {
		FastLinkedList<MyObject> ll = new FastLinkedList<>(1);
		
		Assertions.assertEquals(true, ll.add(new MyObject(1)));
		Assertions.assertEquals(false, ll.add(new MyObject(2)));
		ll.removeFirst();
		Assertions.assertEquals(true, ll.add(new MyObject(3)));
		Assertions.assertEquals(false, ll.add(new MyObject(4)));
	}
	
	private void populateAddFirst(FastLinkedList<MyObject> ll) {
		ll.addFirst(new MyObject(2));
		ll.addFirst(new MyObject(1));
	}
	
	private static class MyObject extends FastLinkedList.LLNode<MyObject> {
		private final int m_data;
		
		MyObject(int data) {
			m_data = data;
		}
		
		@Override
		protected MyObject getObject() {
			return this;
		}
		
	}
}
