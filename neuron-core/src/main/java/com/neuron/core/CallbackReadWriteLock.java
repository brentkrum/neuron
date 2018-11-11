package com.neuron.core;

import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.netty.util.internal.PlatformDependent;

public final class CallbackReadWriteLock {
	private final Runnable[] m_toRun = new Runnable[NeuronApplication.getTaskPool().executorCount()];
	private AtomicInteger m_numToRun = new AtomicInteger();
	private final long m_maxWait;
	private final TimeUnit m_maxWaitTimeUnit;
	private final ReadWriteLock m_rwLock = new ReentrantReadWriteLock(true);
	private final Queue<Runnable> m_readQueue = PlatformDependent.newMpscQueue();
	private final Queue<Runnable> m_writeQueue = PlatformDependent.newMpscQueue();
	private final MyWorker m_worker = new MyWorker();
	private final AtomicLong m_queuedNextTick = new AtomicLong();
	private final AtomicLongFieldUpdater<CallbackReadWriteLock> m_tickUpdater = AtomicLongFieldUpdater.newUpdater(CallbackReadWriteLock.class, "m_queuedTick");
	private volatile long m_queuedTick;
	private AtomicInteger m_lockers = new AtomicInteger();
	
	public CallbackReadWriteLock(long maxWait, TimeUnit maxWaitTimeUnit) {
		m_maxWait = maxWait;
		m_maxWaitTimeUnit = maxWaitTimeUnit;
	}

	public void readLock(Runnable callback) {
		if (m_queuedTick==0 && m_rwLock.readLock().tryLock()) {
			// We got lock
			try {
				callback.run();
			} finally {
				m_rwLock.readLock().unlock();
			}
		} else {
			// We did not get read lock
			m_tickUpdater.compareAndSet(this, 0, m_queuedNextTick.incrementAndGet());
			m_readQueue.add(callback);
			m_worker.requestMoreWork();
		}
	}

	public void writeLock(Runnable callback) {
		m_lockers.incrementAndGet();
		if (m_queuedTick==0 && m_rwLock.writeLock().tryLock()) {
			// We got lock
			try {
				callback.run();
			} finally {
				m_rwLock.writeLock().unlock();
			}
		} else {
			// We did not get write lock immediately
			m_tickUpdater.compareAndSet(this, 0, m_queuedNextTick.incrementAndGet());
			m_writeQueue.add(callback);
		}
		if (m_lockers.decrementAndGet()==0 && m_queuedTick != 0) {
			m_worker.requestMoreWork();
		}
	}
	
	private final class MyWorker extends PerpetualWork {

		@Override
		protected void _doWork() {
			if (m_rwLock.writeLock().tryLock()) {
				// We grabbed the write lock so there are currently no readers and no writers running
			} else {
				// If we could not get the lock, then there are one or more readers or a writer currently running
			}
			
			
			
			long startTick = m_queuedTick;
			boolean exit = false;
			while(!exit) {
				if (m_readQueue.size() > 0) {
					exit = false;
					final int numToRun = m_readQueue.size();
					m_numToRun.set(numToRun);
					for(int i=0; i<numToRun; i++) {
						m_toRun[i] = m_readQueue.remove();
					}
					
				} else {
					exit = true;
				}
				if (m_writeQueue.size() > 0) {
					exit = false;
					m_rwLock.writeLock().lock();
					try {
						// call one runnable
					} finally {
						m_rwLock.writeLock().unlock();
					}
				}
			}
			m_tickUpdater.compareAndSet(CallbackReadWriteLock.this, startTick, 0);
		}
		
	}
	
	private final class Runner implements Runnable {

		@Override
		public void run() {
			if (m_rwLock.readLock().tryLock()) {
				try {
					// call all current runnables on separate threads, with one for this thread
				} finally {
					m_rwLock.readLock().unlock();
				}
			}
		}
		
	}
}
