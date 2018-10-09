package com.neuron.core;

import java.util.concurrent.atomic.AtomicLong;

import com.neuron.core.NeuronRef.INeuronStateLock;

public abstract class PerpetualWorkContextAware implements Runnable
{
	private final NeuronRef m_ref;
	private final AtomicLong m_currentWorkTick = new AtomicLong();
	private final AtomicLong m_nextRequestTick = new AtomicLong();
	
	public PerpetualWorkContextAware() {
		m_ref = NeuronSystemTLS.currentNeuron();
	}
	
	public PerpetualWorkContextAware(NeuronRef ref) {
		m_ref = ref;
	}
	
	/*
	 * 
	 * Support multiple threads calling this method
	 * 
	 */
	public final void requestMoreWork()
	{
		final long currentRequestTick = m_currentWorkTick.get();
		final long nextRequestTick = m_nextRequestTick.incrementAndGet();
		
		// The trick here is that m_currentWorkTick only needs to change, it does not
		// indicate a number of items to do or a count of things to process.  This is just
		// a housekeeping number which must change to keep the worker running
		if (m_currentWorkTick.compareAndSet(currentRequestTick, nextRequestTick))
		{
			if (currentRequestTick == 0)
			{
				// work is not scheduled to run
				NeuronApplication.getTaskPool().submit(this);
			}
			else
			{
				// Updated successfully, currently running work will pick this up
			}
		}
		else if (m_currentWorkTick.compareAndSet(0, nextRequestTick))
		{
			// work is not scheduled to run
			NeuronApplication.getTaskPool().submit(this);
		}
		else
		{
			// This conditions happens when there are multiple threads calling requestMoreWork
			// Only one can win to be considered the "owner" of submitting this work item
			// The one that loses, can silently just return it has nothing to do
		}
	}
	
	abstract protected void _lockException(Exception ex);
	abstract protected void _doWork(INeuronStateLock neuronLock);
	
	@Override
	public final void run()
	{
		final long lastRequestTick = m_currentWorkTick.get();
		try {
			try(final INeuronStateLock lock = m_ref.lockState()) {
				_doWork(lock);
			} catch(Exception ex) {
				_lockException(ex);
			}
		} finally {
			// Set tick to 0 only if it matches the starting tick
			if (m_currentWorkTick.compareAndSet(lastRequestTick, 0) == false)
			{
				// Starting tick didn't match, so we got another request for work while running
				NeuronApplication.getTaskPool().submit(this);
			}
			else
			{
				// No work requested, stopping work
			}
		}
	}

}
