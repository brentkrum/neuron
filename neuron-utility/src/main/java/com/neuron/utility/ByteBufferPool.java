package com.neuron.utility;

import java.nio.ByteBuffer;

public class ByteBufferPool {
	private final FastLinkedList<PooledByteBuffer> m_buffers = new FastLinkedList<>(); 
	private final int m_regionSize;
	private final int m_sliceSize;
	private volatile int m_totalRegionsAllocated; 
	
	public ByteBufferPool(int regionSize, int sliceSize) {
		m_regionSize = regionSize;
		m_sliceSize = sliceSize;
		addRegion();
	}
	
	public int getNumTotalRegionsAllocated() {
		return m_totalRegionsAllocated;
	}
	
	public int getNumOutstandingBuffers() {
		return m_totalRegionsAllocated - m_buffers.count();
	}
	
	private void addRegion() {
		int numRegions = m_regionSize / m_sliceSize;
		ByteBuffer region = ByteBuffer.allocateDirect(m_regionSize);
		int newPosition = 0;
		for(int i=0; i<numRegions; i++, newPosition+=m_sliceSize) {
			region.limit(newPosition+m_sliceSize);
			region.position(newPosition);
			m_buffers.add(new PooledByteBufferImpl(region.slice()));
		}
		m_totalRegionsAllocated += numRegions;
	}

	public PooledByteBuffer allocate() {
		while(true) {
			final PooledByteBuffer pbb = m_buffers.removeFirst();
			if (pbb == null) {
				synchronized(this) {
					final PooledByteBuffer pbb2 = m_buffers.removeFirst();
					if (pbb2 != null) {
						return pbb2;
					}
					addRegion();
				}
			} else {
				return pbb;
			}
		}
	}
	
	private class PooledByteBufferImpl extends FastLinkedList.LLNode<PooledByteBuffer> implements PooledByteBuffer {
		private final ByteBuffer m_bb;
		
		private PooledByteBufferImpl(ByteBuffer bb) {
			m_bb = bb;
		}

		@Override
		public void free() {
			m_bb.clear();
			m_buffers.add(this);
		}

		@Override
		public ByteBuffer get() {
			return m_bb;
		}

		@Override
		public void close() {
			free();
		}

		@Override
		protected PooledByteBuffer getObject() {
			return this;
		}
		
	}
}
